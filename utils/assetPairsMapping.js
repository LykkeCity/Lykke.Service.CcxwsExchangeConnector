function MapAssetPairForward(assetPair, settings){
    var result = assetPair

    var base = assetPair.split("/")[0]
    var quote = assetPair.split("/")[1]

    var assetsMapping = settings.Main.AssetsMapping
    for (const element of assetsMapping) {
        if (base == element.value){
            result = element.key + "/" + quote
            break
        }
        if (quote == element.value){
            result = base + "/" + element.key
            break
        }
    }

    return result
}

function MapAssetPairBackward(assetPair, settings){
    var result = assetPair

    var base = assetPair.split("/")[0]
    var quote = assetPair.split("/")[1]

    var assetsMapping = settings.Main.AssetsMapping
    for (const element of assetsMapping) {
        if (base == element.key){
            result = element.value + "/" + quote
            break
        }
        if (quote == element.key){
            result = base + "/" + element.value
            break
        }
    }

    return result
}

function TryToMapAssetPairForward(assetPair, exchange, settings) {
    var result = assetPair

    var mappedAssetPair = MapAssetPairForward(assetPair, settings)
    var market = exchange.markets[mappedAssetPair]
    var exchangeHasMappedAssetPair = typeof market === "object"
    if (exchangeHasMappedAssetPair)
        result = mappedAssetPair

    return result
}

module.exports.MapAssetPairForward = MapAssetPairForward
module.exports.MapAssetPairBackward = MapAssetPairBackward
module.exports.TryToMapAssetPairForward = TryToMapAssetPairForward
