const express = require('express')
const path = require('path');
const packageJson = require('../package.json')
const LogFactory =  require('../utils/logFactory')
const prometheus = require('prom-client')

let _log

async function startWebServer(loggingLevel, exchangeEventsHandler) {
    _log = LogFactory.create(path.basename(__filename), loggingLevel)
    
    const response = {
        "Author": "Swisschain",
        "Name": "Lykke.Service.CcxwsExchangeConnectorSwisschain",
        "Version": packageJson.version,
        "Env": null,
        "IsDebug": false,
        "IssueIndicators": []
      }
      
    const app = express()

    app.get('/api/isAlive', function (req, res) {
        res.header("Content-Type",'application/json')
        res.send(JSON.stringify(response, null, 4))
    })

    app.get('/metrics', async function(req, res) {
        try {
            const register = prometheus.register
            res.set('Content-Type', register.contentType);
            res.end(await register.metrics());
        } catch (ex) {
            res.status(500).end(ex);
        }
    })

    app.get('/api/snapshots', function (req, res) {
        res.header("Content-Type",'application/json')
        res.send(exchangeEventsHandler.getSnapshot())
    })

    const httpPort = process.env.HttpPort
    const server = app.listen(httpPort, function () {
        let host = server.address().address
        const port = server.address().port

        if (host === "::") { 
           host = "localhost"
        }

        _log.info(`Listening at http://${host}:${port}`)
    })
}

module.exports = startWebServer