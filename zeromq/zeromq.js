const zmq = require('zeromq')
const LogFactory =  require('../utils/logFactory')
const path = require('path');

var zeromq = null
let _log

function getZeroMq(settings) {

  if (zeromq != null)
    return zeromq

  _log = LogFactory.create(path.basename(__filename), settings.Main.LoggingLevel)

  const isDisabled = settings.ZeroMq.Disabled
  const address = settings.ZeroMq.Address
  
  zeromq = zmq.socket("pub");
  if (!isDisabled){
    zeromq.bindSync(address);
  }

  _log.info(`ZeroMQ is publishing from '${address}'`)

  return zeromq
}

module.exports = getZeroMq