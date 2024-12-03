const socketioServer = require('socket.io')
const LogFactory =  require('../utils/logFactory')
const path = require('path');

var socketio = null
let _log

function getSocketIO(settings) {

  if (socketio != null)
    return socketio

  _log = LogFactory.create(path.basename(__filename), settings.Main.LoggingLevel)

  const isDisabled = settings.SocketIO.Disabled
  const port = settings.SocketIO.Port

  if (!isDisabled){
    socketio = socketioServer.listen(port)

    hostPort = socketio.httpServer._connectionKey
    hostPort = hostPort.replace('6:::', 'localhost')

    _log.info(`Listening Socket.IO at http://${hostPort}`)
  }
  return socketio
}

module.exports = getSocketIO