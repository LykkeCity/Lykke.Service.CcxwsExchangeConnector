var fetch = require('node-fetch')
var fs = require('fs')

async function getSettings() {

    settingsUrl = process.env.SettingsUrl

    if (!settingsUrl)
        throw "Settings are not set."

    try
    {
        // url
        if (settingsUrl.startsWith('http')){
            const response = await fetch(settingsUrl)
            const json = await response.json()
            return json
        }
        // file
        else {
            var content = fs.readFileSync(settingsUrl, 'utf8')
            const json = JSON.parse(content)
            return json
        }
    }
    catch (error)
    {
        console.log(error)
    }
    
    throw "Something wrong with the settings."
}

module.exports = getSettings
