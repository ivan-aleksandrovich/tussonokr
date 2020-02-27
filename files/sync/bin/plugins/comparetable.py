# -*- coding: UTF-8 -*-

import bz2
import logging
import artixinterchange
import status
log = logging.getLogger('su.artix.loggerSync')

#
# Потабличная сверка данных 
#

def main(message):
    description = message.properties.get('compareDescription', '')
    compareName = message.properties.get('compareName')
    log.info(u"Получено сообщение для сверки данных <%s>: %s" % (compareName, description))
    request = message.properties.get('requestCash')
    if request == None or len(request) < 1:
        mess = 'В полученном сообщении не указан sql запрос.'
        log.error(mess)
        return status.errorLoadDict, mess
    compareResultServerId = message.properties.get('compareResultServerId')
    if compareResultServerId == None or len(compareResultServerId) < 1:
        mess = 'В полученном сообщении не указан идентификатор результата сравнения на КС.'
        log.error(mess)
        return status.errorLoadDict, mess
    compressMethod = message.properties.get('compressMethod')
    resultServer = message.content
    try:
        if compressMethod == 'BZ2' and resultServer != None:
            resultServer = bz2.decompress(resultServer)
    except Exception, e:
        log.warning("Не удалось разархивировать результат сверки данных с КС")
    
    return artixinterchange.sendCompareTableRequest(request, compareName, compareResultServerId, resultServer)

