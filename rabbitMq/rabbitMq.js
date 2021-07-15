const amqp = require('amqplib')
const path = require('path');
const LogFactory = require('../utils/logFactory')

class RabbitMq {

    constructor(settings, loggingLevel) {
        this._settings = settings;
        this._log = LogFactory.create(path.basename(__filename), loggingLevel)
      }

    async getChannel() {
        if (!this._channel){
            this._channel = await this._createChannel()
        }

        return this._channel
    }

    async _createChannel() {
        if (this._settings.Disabled) {
            this._log.info(`Connecting and publishing to RabbitMq is disabled in the settings.`)

            return;
        }

        try
        {
            this._log.info(`Trying to connect to RabbitMq...`)

            const connection = await amqp.connect(this._settings.ConnectionString)

            this._log.info(`Finished trying to connect to RabbitMq.`)

            
            this._log.info(`Trying to create channel to RabbitMq...`)

            const channel = await connection.createChannel()

            this._log.info(`Finished trying to create a channel to RabbitMq.`)

            
            this._log.info(`Asserting that RabbitMq exchange for 'Quotes' exists...`)

            await channel.assertExchange(this._settings.Quotes, 'fanout', {durable: false})
            
            this._log.info(`Finished asserting that RabbitMq exchange for 'Quotes' exists.`)

            
            this._log.info(`Asserting that RabbitMq exchange for 'OrderBooks' exists...`)

            await channel.assertExchange(this._settings.OrderBooks, 'fanout', {durable: false})
            
            this._log.info(`Finished asserting that RabbitMq exchange for 'OrderBooks' exists.`)

            
            this._log.info(`Asserting that RabbitMq exchange for 'Trades' exists...`)

            await channel.assertExchange(this._settings.Trades, 'fanout', {durable: false})
        
            this._log.info(`Finished asserting that RabbitMq exchange for 'Trades' exists.`)

            
            return channel
        }
        catch (e)
        {
            this._log.warn(`Exception while trying to create RabbitMq channel: ${e}, ${e.stack}.`)
        }
    }

    async send(rabbitExchange, object) {
        if (this._settings.Disabled) {
            return;
        }

        const objectJson = JSON.stringify(object)
    
        try {
            const channel = await this.getChannel()
            channel.publish(rabbitExchange, '', Buffer.from(objectJson))
            this._log.debug(`Message was sent to RabbitMq: ${objectJson}`)
        }
        catch(e) {
            this._log.warn(`Error while sending a message to RabbitMq: ${e}, ${e.stack}.`)
        }
    }

    async sendRaw(rabbitExchange, object) {
        if (this._settings.Disabled) {
            return;
        }

        try {
            const channel = await this.getChannel()
            channel.publish(rabbitExchange, '', Buffer.from(object))
            this._log.debug(`Message was sent to RabbitMq: ${object}`)
        }
        catch(e) {
            this._log.warn(`Error while sending a message to RabbitMq: ${e}, ${e.stack}.`)
        }
    }

}

module.exports = RabbitMq