const ccxws = require("ccxws");

function MapExchangeCcxtToCcxws(ccxtExchangeName, options){
    switch(ccxtExchangeName) {
        case 'bibox':
            return new ccxws.Bibox(options)
        case 'binance':
            return new ccxws.Binance(options)
        case 'bitfinex':
            return new ccxws.bitfinex(options)
        case 'bitfinex2':
            return new ccxws.Bitfinex(options)            
        case 'bitflyer':
            return new ccxws.Bitflyer(options)
        case 'bitmex':
            return new ccxws.BitMEX(options)
        case 'bitstamp':
            return new ccxws.Bitstamp(options)
        case 'bithumb':
            return new ccxws.Bithumb(options)
        case 'bittrex':
            return new ccxws.Bittrex(options)
        case 'cex':
            return new ccxws.cex(options)
        case 'coinex':
            return new ccxws.coinex(options)
        case 'coinbasepro':
            return new ccxws.coinbasepro(options)
        case 'deribit':
            return new ccxws.Deribit(options)
        case 'ftx':
            return new ccxws.ftx(options)
        case 'gateio':
            return new ccxws.Gateio(options)
        case 'gemini':
            return new ccxws.Gemini(options)
        case 'hitbtc':
            return new ccxws.hitbtc2(options)
        case 'huobipro':
            return new ccxws.huobipro(options)
        case 'kraken':
            return new ccxws.kraken(options)
        case 'kucoin':
            return new ccxws.kucoin(options) 
        case 'liquid':
            return new ccxws.liquid(options)
        case 'okex':
            return new ccxws.OKEx(options)
        case 'poloniex':
            return new ccxws.Poloniex(options)
        case 'upbit':
            return new ccxws.Upbit(options)
        case 'zb':
            return new ccxws.zb(options)
        default:
            console.log('*******')
            console.log(ccxtExchangeName)
            throw "This point can't be reached."
    }
}

module.exports.MapExchangeCcxtToCcxws = MapExchangeCcxtToCcxws