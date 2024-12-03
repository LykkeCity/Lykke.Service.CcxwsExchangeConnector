function GetExchangeOptions(exchangeName, settings) {

    let exchangeCredentials

    if (!settings.Main.Credentials) {
        return {}
    }

    settings.Main.Credentials.forEach(cred => {
        if (cred.Exchange == exchangeName){
            exchangeCredentials = cred
        }
    });

    let options = {}

    if (exchangeCredentials)
    {
        options.apiKey = exchangeCredentials.ApiKey
        options.apiSecret = exchangeCredentials.ApiSecret
    }

    return options
}

module.exports.GetExchangeOptions = GetExchangeOptions