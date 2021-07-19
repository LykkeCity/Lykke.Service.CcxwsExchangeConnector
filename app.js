const ccxt = require('ccxt');
const path = require('path');
const LogFactory =  require('./utils/logFactory')
const RabbitMq = require('./rabbitMq/rabbitMq')
const getSettings = require('./settings/settings')
const startWebServer = require('./webserver/webserver')
const assetPairsMapping = require('./utils/assetPairsMapping')
const exchangesMapping = require('./utils/exchangesMapping')
const exchangeOptions = require('./utils/exchangeOptions')
const exchangeEventsHandler = require('./exchangeEventsHandler')
const prometheus = require('prom-client')
const collectDefaultMetrics = prometheus.collectDefaultMetrics
collectDefaultMetrics()

let settings
let log
let rabbitMq

(async function main() {
    settings = (await getSettings()).CcxwsExchangeConnectorSwisschain
    log = LogFactory.create(path.basename(__filename), settings.Main.LoggingLevel)

    log.info(`Main thread started...`)

    process.on('uncaughtException',  e => log.warn(`Unhandled error: ${e}, ${e.stack}.`))
    process.on('unhandledRejection', e => log.warn(`Unhandled error: ${e}, ${e.stack}.`))

    rabbitMq = new RabbitMq(settings.RabbitMq, settings.Main.LoggingLevel)
    await rabbitMq.getChannel()

    var handler = await subscribeToExchangesData()

    await startWebServer(settings.Main.LoggingLevel, handler)
})();

async function subscribeToExchangesData() {
    const exchanges = settings.Main.Exchanges
    const symbols = settings.Main.Symbols

    return await subscribeToExchangeData(exchanges[0], symbols, settings);
}

async function subscribeToExchangeData(exchangeSettings, symbols, settings) {
    
    let exchangeName = exchangeSettings.CcxwsName
    
    let exchange = null
    
    try {
        exchange = new ccxt[exchangeName]()
    } catch(e){
        log.warn(`${exchangeName} wasn't found in ccxt: ${e}.`)
        return
    }

    const options = exchangeOptions.GetExchangeOptions(exchangeName, settings)

    const exchange_ws = exchangesMapping.MapExchangeCcxtToCcxws(exchangeName, options)
    if (!exchange_ws) {
        log.warn(`${exchangeName} wasn't mapped from ccxt to ccxws.`)
        return
    }

    exchange_ws.reconnectIntervalMs = settings.Main.ReconnectIntervalMs

    let allMarkets = []

    try {
        exchange.timeout = 30 * 1000
        allMarkets = await exchange.loadMarkets()
    } catch (e) {
        log.warn(`${exchange.id} can't load markets: ${e}.`)
        return
    }

    try {
        let availableMarkets = []
        if (symbols.includes('*')) {
            for (var key of Object.keys(allMarkets)) {
                availableMarkets.push(allMarkets[key])
            }
        } else {
            availableMarkets = getAvailableMarketsForExchange(exchange, symbols)
        }

        if (availableMarkets.length === 0) {
            log.warn(`${exchange.id} doesn't have any symbols from config.`)
            return
        }

        const handler = new exchangeEventsHandler(exchangeSettings.PublishingName, settings, rabbitMq)

        exchange_ws.on("ticker", async ticker => await handler.tickerEventHandle(ticker))
        exchange_ws.on("l2snapshot", async orderBook => await handler.l2snapshotEventHandle(orderBook))
        exchange_ws.on("l2update", async updateOrderBook => await handler.l2updateEventHandle(updateOrderBook))
        exchange_ws.on("trade", async trade => await handler.tradesEventHandle(trade))

        exchange_ws.on("error", err => log.error(err))
        exchange_ws.on("connecting", () => log.info('Connecting...'))
        exchange_ws.on("connected", () => log.info('Connected.'))
        exchange_ws.on("disconnected", () => log.warn('Disconnected.'))
        exchange_ws.on("closing", () => log.warn('Closing...'))
        exchange_ws.on("closed", () => log.warn('Closed.'))
        exchange_ws.on("reconnecting", () => log.warn('Reconnecting...'))

        log.info(`${exchange.id} - found ${availableMarkets.length} markets from config.`)

        availableMarkets.forEach(market => {
            try {
                if (settings.Main.Events.Quotes.Subscribe)
                    exchange_ws.subscribeTicker(market)

                if (settings.Main.Events.OrderBooks.Subscribe)
                {
                    if (exchange_ws.hasLevel2Snapshots)
                        exchange_ws.subscribeLevel2Snapshots(market)
                    else 
                        exchange_ws.subscribeLevel2Updates(market)
                }

                if (settings.Main.Events.Trades.Subscribe)
                    exchange_ws.subscribeTrades(market)
            } catch (e) {
                log.warn(`${exchange.id} can't subscribe : ${e}.`)
                return
            }
        });

        return handler;
    } catch (e) {
        log.warn(`Exception occured during loading markets for '${exchange.id}': ${e}.`)
        return
    }
}

function getAvailableMarketsForExchange(exchange, symbols) {
    const result = []

    for (const symbol of symbols) {
        const mappedSymbol = assetPairsMapping.TryToMapAssetPairForward(symbol, exchange, settings)
        let market = exchange.markets[mappedSymbol]
        // Inversed - first trying to map, then try to use original
        // Example:
        // if in the cofig 'symbols' contains BTC/USD and 'mapping' contains USD -> USDT
        // then it first tries to find BTC/USDT and if no such asset pair then looks for BTC/USD
        const exchangeHasMapped = typeof market === "object"
        if (exchangeHasMapped) {
            result.push(market)
        } else {
            market = exchange.markets[symbol]
            const exchangeHas = typeof market === "object"
            if (exchangeHas) {
                result.push(market)
            }
        }
    }

    return result
}