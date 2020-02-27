# -*- coding: UTF-8 -*-

import os
from qpid.messaging import *
import logging
import logger
from plugins import status

loggerSync = logging.getLogger('su.artix.loggerSync')

#
# Получение сообщений из очереди
#

messageTypeKey = 'type'
pluginPath = 'plugins'
goodMessageFile = 'lastGoodMessage.txt'

def saveLastGoodMessageId(messageId):
    try:
        with open(goodMessageFile, 'w') as outfile:
            outfile.write(messageId)
    except Exception, e:
        loggerSync.warn(e)

def getLastGoodMessageId():
    try:
        with open(goodMessageFile, 'r') as infile:
            for line in infile:
                if line and line.strip():
                    return line
    except IOError, e:
        loggerSync.warn(e)
    return None


#connection = Connection(opts.broker, reconnect=opts.reconnect, reconnect_interval=opts.reconnect_interval, reconnect_limit=opts.reconnect_limit)

def main(broker, cashCode, brokerConnectionHeartbeat):
    res = status.OK # результат. Если проверка очереди прошла нормально, то вернется 1
    mess = ''
    startPart = False
    try:
        loggerSync.info('Создаем соединение')
        connection = Connection(broker, heartbeat=brokerConnectionHeartbeat)
        connection.tcp_nodelay = True
        loggerSync.info('Открываем соединение')
        connection.open()
    except Exception, e:
        loggerSync.error(str(e))
        return status.errorProgramm, str(e)
    try:
        loggerSync.info('Создаем сессию')
        session = connection.session()
        loggerSync.info('Создаем получателя сообщений')
        destination = "artix.forward.cs" + "."+cashCode
        receiver = session.receiver(destination)
        while True:
            loggerSync.info('Проверяем наличие сообщений в очереди')
            message = receiver.fetch(timeout = 1)
            messageId = str(message.id) if message.id else None
            if messageId and messageId == getLastGoodMessageId():
                loggerSync.info('Сообщение с идентификатором %s уже было обработано. Удаляем его.' % messageId)
                session.acknowledge()
                continue
            messageType = message.properties.get(messageTypeKey)
            messageType = messageType.lower()
            loggerSync.info('Получили сообщение')
            dictMessageType = None
            if messageType == 'loaddict':
                dictMessageType = message.properties.get("dictMessageType", "FULL").upper()
                loggerSync.debug('dictMessageType = %s' % dictMessageType)
                if dictMessageType == 'FIRST_PART':
                    loggerSync.info('Справочник разбит на несколько частей. Получаем первую часть.')
                    startPart = True

            try:
                if messageType != None:
                    plugin = __import__('%s.%s' % (pluginPath, messageType), fromlist='q')
                    # выполняем скрипт, соответствующий типу сообщения
                    loggerSync.info('Вызываем плагин %s для обработки сообщения' % messageType.encode('utf8'))
                    try:
                        res, mess = plugin.main(message)
                    except Exception, e:
                        mess = str(e)
                        res = status.errorProgramm
                    if res == status.OK:
                        if messageType != 'loaddict' or dictMessageType == 'FULL' or (startPart and dictMessageType == 'LAST_PART'):
                            startPart = False
                            loggerSync.info('Удаляем из очереди принятое сообщение %s' % messageId)
                            saveLastGoodMessageId(messageId)
                            session.acknowledge()
                    else:
                        receiver.close()
                        session.close()
                        connection.close()
                        return res, mess
                else:
                    loggerSync.error("Тип сообщения не указан.")
            except ImportError, e:
                res = status.errorProgramm
                mess = "Неизвестный тип сообщения. " + str(e)
                loggerSync.error(mess)
            loggerSync.info('')
    except NotFound, e:
        res = status.errorProgramm
        mess = str(e)
        loggerSync.error(str(e))
    except Empty:
        mess = 'Проверка сообщений закончена'
        loggerSync.info(mess)
        res = status.OK
        receiver.close()
    finally:
        loggerSync.info('-----------------------')
        if session:
            session.close()
        connection.close()
    return res, mess

if __name__ == '__main__':
    import propertyReader
    main(propertyReader.remoteBroker, propertyReader.cashCode, propertyReader.brokerConnectionHeartbeat)
