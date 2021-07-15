const moment = require('moment');
const path = require('path');
const LogFactory =  require('./utils/logFactory')
const mapping = require('./utils/assetPairsMapping')
const getSocketIO = require('./socketio/socketio')
const getZeroMq = require('./zeromq/zeromq')
const Metrics = require('./prometheus/metrics')
var protobuf = require("protobufjs")

class ExchangeEventsHandler {
    constructor(exchange, settings, rabbitMq) {
        this._exchange = exchange
        this._settings = settings
        this._rabbitMq = rabbitMq
        this._socketio = getSocketIO(settings)

        this._zeroMq = getZeroMq(settings)

        this._orderBooks = new Map()
        this._lastTimePublished = new Map()
        this._log = LogFactory.create(path.basename(__filename), settings.Main.LoggingLevel)

        const suffixConfig = this._settings.Main.ExchangesNamesSuffix
        const suffix = suffixConfig ? suffixConfig : ""
        this._source = this._exchange.name.replace(this._exchange.version, "").trim()
        this._source = this._source + suffix

        this._OBProtoFile = new protobuf.Root().loadSync(__dirname + '/gRPC/orderbooks.proto', {keepCase: true});
        this._OBprotoBufRoot = this._OBProtoFile.loadSync({root:"common"});
        this._orderbookResponse = this._OBprotoBufRoot.lookupType("GetOrderBooksResponse");
        this._orderbookUpdateResponse = this._OBprotoBufRoot.lookupType("GetOrderBookUpdateResponse");


        this._TradeProtoFile = new protobuf.Root().loadSync(__dirname + '/gRPC/trades.proto', {keepCase: true});
        this._TradeprotoBufRoot = this._TradeProtoFile.loadSync({root:"common"});
        this._trade = this._TradeprotoBufRoot.lookupType("Trade");
    }

    // event handlers

    async tickerEventHandle(ticker) {
        let quote = this._mapCcxwsTickerToPublishQuote(ticker)

        let isValid = quote.ask > 0 && quote.bid > 0

        if (!isValid) {
            //TODO: sometimes ask and bid are null, has to be investigated
            //this._log.warn(`${quote.source} Quote is invalid: ${JSON.stringify(quote)}.`)
            return;
        }

        await this._publishQuote(quote)
    }

    getSnapshot(){

        const protoOrderBooks = [];

        for (let [key, value] of this._orderBooks) {
            var protoOrderBook = this._mapPublishOrderBookToProtobufOrderBook(
                    this._mapInternalOrderBookToPublishOrderBook(value),
                    value.timestamp
                );
            protoOrderBooks.push(protoOrderBook)
        } 


        var payload = this._orderbookUpdateResponse.create({orderBookUpdates: protoOrderBooks})
        const message = this._orderbookUpdateResponse.encode(payload).finish();

        this._log.debug(`Snapshot created, there are ${protoOrderBooks.length} order books.`)
        return message;
    }

    async l2snapshotEventHandle(orderBook) {
        // metrics
        Metrics.order_book_in_count.labels(orderBook.exchange, `${orderBook.base}/${orderBook.quote}`).inc()
        if (orderBook.timestampMs){
            const delayMs = moment.utc().valueOf() - orderBook.timestampMs
            if (delayMs > 200)
                this._log.warn(`Received Order book ${orderBook.exchange} ${orderBook.base}/${orderBook.quote} is older then ${delayMs} ms.`)
            Metrics.order_book_in_delay_ms.labels(orderBook.exchange, `${orderBook.base}/${orderBook.quote}`).set(delayMs)
            Metrics.order_book_in_delay.observe(delayMs)
        }

        // update cache
        const key = orderBook.marketId

        const currentOrderBook = this._orderBooks.get(key)
        var previousBestBid
        var previousBestAsk
        if (currentOrderBook) {
            previousBestBid = this._orderBooks.get(key).bids.keys().next().value
            previousBestAsk = this._orderBooks.get(key).asks.keys().next().value
        }

        const internalOrderBook = this._mapCcxwsOrderBookToInternalOrderBook(orderBook)
        this._orderBooks.set(key, internalOrderBook)

        if (currentOrderBook) {
            const currentBestBid = this._orderBooks.get(key).bids.keys().next().value
            const currentBestAsk = this._orderBooks.get(key).asks.keys().next().value
            const noPriceChange = previousBestBid == currentBestBid && previousBestAsk == currentBestAsk
            if (this._settings.Main.Events.OrderBooks.PublishOnlyIfBboPricesChanged && noPriceChange) {
                //this._log.debug(`Skipped order book ${this._exchange.name} ${key} - no price change.`)
                return
            }
        }

        // metrics
        const ob = internalOrderBook
        Metrics.order_book_out_side_price.labels(ob.source, `${ob.assetPair}`, 'bid').set(ob.bids.keys().next().value)
        Metrics.order_book_out_side_price.labels(ob.source, `${ob.assetPair}`, 'ask').set(ob.asks.keys().next().value)

        // publish
        if (this._isTimeToPublishOrderBook(key))
        {
            await this._publishOrderBook(internalOrderBook)

            this._lastTimePublished.set(key, moment.utc())
        }
    }

    async l2updateEventHandle(updateOrderBook) {
        Metrics.order_book_in_count.labels(updateOrderBook.exchange, `${updateOrderBook.base}/${updateOrderBook.quote}`).inc()
        if (updateOrderBook.timestampMs){
            const delayMs = moment.utc().valueOf() - updateOrderBook.timestampMs
            if (delayMs > 200)
                this._log.warn(`Received Order book update ${updateOrderBook.exchange} ${updateOrderBook.base}/${updateOrderBook.quote} is older then ${delayMs} ms.`)
            Metrics.order_book_in_delay_ms.labels(updateOrderBook.exchange, `${updateOrderBook.base}/${updateOrderBook.quote}`).set(delayMs)
            Metrics.order_book_in_delay.observe(delayMs)
        }

        const key = updateOrderBook.marketId

        // update cache

        const internalOrderBook = this._orderBooks.get(key)

        if (!internalOrderBook) {
            this._log.warn(`Order book ${this._exchange.name} ${key} was not found in the cache during the 'order book update' event.`)
            return
        }

        const previousBestBid = internalOrderBook.bids.keys().next().value
        const previousBestAsk = internalOrderBook.asks.keys().next().value

        updateOrderBook.asks.forEach(ask => {
            const updateAskPrice = parseFloat(ask.price)
            const updateAskSize = parseFloat(ask.size)

            internalOrderBook.asks.delete(updateAskPrice)
            
            if (updateAskSize !== 0)
                internalOrderBook.asks.set(updateAskPrice, updateAskSize)
        });

        updateOrderBook.bids.forEach(bid => {
            const updateBidPrice = parseFloat(bid.price)
            const updateBidSize = parseFloat(bid.size)

            internalOrderBook.bids.delete(updateBidPrice)

            if (updateBidSize !== 0)
                internalOrderBook.bids.set(updateBidPrice, updateBidSize)
        });

        internalOrderBook.timestampMs = updateOrderBook.timestampMs // optional, not available on most exchanges
        if (internalOrderBook.timestampMs)
            internalOrderBook.timestamp = moment(internalOrderBook.timestampMs)
        else
            internalOrderBook.timestamp = moment.utc()

        const currentBestBid = internalOrderBook.bids.keys().next().value
        const currentBestAsk = internalOrderBook.asks.keys().next().value
        const noPriceChange = previousBestBid == currentBestBid && previousBestAsk == currentBestAsk

        if (this._settings.Main.Events.OrderBooks.PublishOnlyIfBboPricesChanged && noPriceChange){
            //this._log.debug(`Skipped order book ${this._exchange.name} ${key} - no price change.`)
            return
        }

        // metrics
        const ob = internalOrderBook
        Metrics.order_book_out_side_price.labels(ob.source, `${ob.assetPair}`, 'bid').set(ob.bids.keys().next().value)
        Metrics.order_book_out_side_price.labels(ob.source, `${ob.assetPair}`, 'ask').set(ob.asks.keys().next().value)

        // publish
        const publish = this._isTimeToPublishOrderBook(key)
        if (publish)
        {
            if (this._settings.Main.Events.OrderBooks.PublishFullOrderBooks){
                await this._publishOrderBook(internalOrderBook)
            }
            else {
                var directlyMappedInternalOrderbook = this._mapCcxwsOrderBookToInternalOrderBook(updateOrderBook)
                await this._publishOrderBook(directlyMappedInternalOrderbook)
            }


            this._lastTimePublished.set(key, moment.utc().valueOf())
        }
    }

    async tradesEventHandle(trade) {
        await this._publishTrade(trade)
    }

    // publishing

    async _publishQuote(quote) {
        if (this._settings.Main.Events.Quotes.Publish)
        {
            await this._rabbitMq.send(this._settings.RabbitMq.Quotes, quote)

            this._log.debug(`Quote: ${quote.source} ${quote.asset}, bid:${quote.bid}, ask:${quote.ask}, timestamp:${quote.timestamp}.`)
        }
    }

    async _publishOrderBook(internalOrderBook) {
        if (this._settings.Main.Events.OrderBooks.Publish)
        {
            const timestamp = internalOrderBook.timestamp;
            const orderBook = this._mapInternalOrderBookToPublishOrderBook(internalOrderBook)

            if (!this._settings.RabbitMq.Disabled && this._rabbitMq != null)
                await this._rabbitMq.send(this._settings.RabbitMq.OrderBooks, orderBook)

            if (!this._settings.SocketIO.Disabled && this._socketio != null)
                this._socketio.sockets.send(orderBook);

            if (!this._settings.ZeroMq.Disabled && this._zeroMq != null) {
                if (this._settings.ZeroMq.Serializer == "protobuf") {
                    const protoOrderBook = this._mapPublishOrderBookToProtobufOrderBook(orderBook, timestamp)
                    var payload = this._orderbookUpdateResponse.create({orderBookUpdates: [protoOrderBook]})
                    const message = this._orderbookUpdateResponse.encode(payload).finish();
                    this._zeroMq.send(["orderbooks", message]);
                }
                else if (this._settings.ZeroMq.Serializer == "json") {
                    this._zeroMq.send(["orderbooks", JSON.stringify(orderBook)]);
                }
            }

            Metrics.order_book_out_count.labels(orderBook.source, `${orderBook.base}/${orderBook.quote}`).inc()
            const delayMs = moment.utc().valueOf() - orderBook.timestampMs
            Metrics.order_book_out_delay_ms.labels(orderBook.source, `${orderBook.assetPair.base}/${orderBook.assetPair.quote}`).set(delayMs)
            if (delayMs > 200){
                this._log.warn(`Published Order book ${orderBook.exchange} ${orderBook.assetPair.base}/${orderBook.assetPair.quote} is older then ${delayMs} ms.`)
            }

            this._log.debug(`Order Book: ${orderBook.source} ${orderBook.asset}, ` + 
                `levels:[${orderBook.bids.length}, ${orderBook.asks.length}], ` + 
                `volumes: [${orderBook.bidsVolume.toFixed(2)}, ${orderBook.asksVolume.toFixed(2)}], ` + 
                `timestamp: ${orderBook.timestamp}.`)
        }
    }

    async _publishTrade(trade) {
        if (this._settings.Main.Events.Trades.Publish)
        {
            if (this._settings.Main.Events.Trades.Serializer = "protobuf") {
                var protoTrade = this._mapCcxwsTradeToProtoTrade(trade);
                var payload = this._trade.create(protoTrade)
                const message = this._trade.encode(payload).finish();
                await this._rabbitMq.sendRaw(this._settings.RabbitMq.Trades, message)
            }
            else {
                await this._rabbitMq.send(this._settings.RabbitMq.Trades, trade)
            }

            this._log.debug(`Trade: ${trade.exchange}, ${trade.base}/${trade.quote}, price: ${trade.price}, amount: ${trade.amount}, side: ${trade.side}.`)
        }
    }

    // mapping

    _mapCcxwsTradeToProtoTrade(trade){
        const protoTrade = {}
        protoTrade.Venue = trade.exchange
        protoTrade.TradeId = trade.tradeId
        protoTrade.AssetPairId = trade.marketId
        protoTrade.Price = trade.price
        if (trade.side == "sell") {
            protoTrade.FilledSize = trade.amount * -1
        }
        else if (trade.side == "buy") {
            protoTrade.FilledSize = trade.amount
        }
        protoTrade.CanceledSize = 0
        protoTrade.Timestamp = this._getProtoTimestamp(trade.unix)
        return protoTrade
    }

    _mapCcxwsTickerToPublishQuote(ticker) {
        const quote = {}
        quote.source = this._source
        quote.assetPair = { 'base': ticker.base, 'quote': ticker.quote }
        quote.asset = ticker.base + ticker.quote
        quote.timestamp = moment(ticker.timestamp).toISOString()
        quote.timestampMs = ticker.timestamp
        quote.bid = parseFloat(ticker.bid)
        quote.ask = parseFloat(ticker.ask)
    
        return quote
    }
    
    _mapCcxwsOrderBookToInternalOrderBook(ccxwsOrderBook) {
        const asks = new Map();
        ccxwsOrderBook.asks.forEach(ask => {
            const askPrice = parseFloat(ask.price)
            const askSize = parseFloat(ask.size)
    
            asks.set(askPrice, askSize)
        })
    
        const bids = new Map();
        ccxwsOrderBook.bids.forEach(bid => {
            const bidPrice = parseFloat(bid.price)
            const bidSize = parseFloat(bid.size)
    
            bids.set(bidPrice, bidSize)
        })
    
        const internalOrderBook = {}
        internalOrderBook.source = ccxwsOrderBook.exchange
        internalOrderBook.assetPair = ccxwsOrderBook.marketId
        internalOrderBook.asks = asks
        internalOrderBook.bids = bids

        internalOrderBook.timestampMs = ccxwsOrderBook.timestampMs // optional, not available on most exchanges
        if (internalOrderBook.timestampMs)
            internalOrderBook.timestamp = moment(internalOrderBook.timestampMs)
        else
            internalOrderBook.timestamp = moment.utc()

        return internalOrderBook
    }

    _getProtoTimestamp(dateTime){
        if (dateTime == null || dateTime == undefined){
            return {};
        }

        const protoTimestamp = {};
        protoTimestamp.seconds = dateTime / 1000;
        protoTimestamp.nanos = (dateTime % 1000) * 1e6;
        return protoTimestamp
    }

    _mapPublishOrderBookToProtobufOrderBook(publishOrderBook, timestamp) {
        const protoOrderBook = {}
        protoOrderBook.source = publishOrderBook.source
        protoOrderBook.assetPair = publishOrderBook.assetPair
        protoOrderBook.bids = publishOrderBook.bids
        protoOrderBook.asks = publishOrderBook.asks
        protoOrderBook.timestamp = this._getProtoTimestamp(timestamp)
        return protoOrderBook
    }

    _mapInternalOrderBookToPublishOrderBook(internalOrderBook) {
        const symbol = mapping.MapAssetPairBackward(internalOrderBook.assetPair, this._settings)
    
        const base = symbol.substring(0, symbol.indexOf('/'))
        const quote = symbol.substring(symbol.indexOf("/") + 1)

        const publishingOrderBook = {}
        publishingOrderBook.source = this._source
        publishingOrderBook.asset = symbol.replace("/", "")
        publishingOrderBook.assetPair = { 'base': base, 'quote': quote }
        publishingOrderBook.timestamp = internalOrderBook.timestamp.toISOString()
        publishingOrderBook.timestampMs = internalOrderBook.timestampMs // optional, not available on most exchanges
        publishingOrderBook.bidsVolume = 0
        publishingOrderBook.asksVolume = 0

        const descOrderedBidsPrices = Array.from(internalOrderBook.bids.keys())
                                           .sort(function(a, b) { return b-a; })
        const bids = []
        for(let price of descOrderedBidsPrices) {
            if (price == 0)
                continue
            let size = internalOrderBook.bids.get(price)
            if (size == 0)
                continue
    
            publishingOrderBook.bidsVolume += size

            price = this._toFixedNumber(price)
            size = this._toFixedNumber(size)
    
            bids.push({ 'price': price, 'volume': size })

            const publishLevels = this._settings.Main.Events.OrderBooks.PublishLevels
            if (publishLevels > 0 && bids.length >= publishLevels)
                break;
        }
        publishingOrderBook.bids = bids
    
        const ascOrderedAsksPrices = Array.from(internalOrderBook.asks.keys())
                                           .sort(function(a, b) { return a-b; })
        const asks = []
        for(let price of ascOrderedAsksPrices) {
            if (price == 0)
                continue
            let size = internalOrderBook.asks.get(price)
            if (size == 0)
                continue
    
            publishingOrderBook.asksVolume += size

            price = this._toFixedNumber(price)
            size = this._toFixedNumber(size)
    
            asks.push({ 'price': price, 'volume': size })

            const publishLevels = this._settings.Main.Events.OrderBooks.PublishLevels
            if (publishLevels > 0 && asks.length >= publishLevels)
                break;
        }
        publishingOrderBook.asks = asks
    
        return publishingOrderBook
    }

    // utils

    _isTimeToPublishOrderBook(key) {
        const publishingIntervalMs = this._settings.Main.Events.OrderBooks.PublishingIntervalMs
        const lastTimePublishedMs = this._lastTimePublished.get(key)
        const delaySinceLastTimePublished = moment.utc().valueOf() - lastTimePublishedMs
        const isFirstTimePublishing = !lastTimePublishedMs
        const isTimeToPublish = delaySinceLastTimePublished > publishingIntervalMs

        return isFirstTimePublishing || isTimeToPublish
    }

    _toFixedNumber(number) {
        return number.toFixed(8).replace(/\.?0+$/,"")
    }
}

module.exports = ExchangeEventsHandler