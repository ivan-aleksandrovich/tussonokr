# -*- coding: UTF-8 -*-

import sys, os
import logging
import status
import propertyReader
from qpid.messaging import *
from datetime import datetime
import json
import ujson
import marshal
import traceback
from qmf.console import Session
log = logging.getLogger('su.artix.loggerSync')

#
# Загрузка справочника формата ArtixInterchange
#

unloadScript = propertyReader.unloadScript

curPath = sys.path[0]
sys.path[0] = '%s' % os.path.dirname(unloadScript)
try:
    unloadModule = __import__(os.path.basename(os.path.splitext(unloadScript)[0]), fromlist='q')
except Exception, e:
    log.error('Ошибка при подключении модуля выгрузки продаж. %s' % str(e))
sys.path[0] = curPath

def unload(path, mode, dates=None, shifts=None, closedShifts=0, saleStateId=None, unloadingShopId=None, unloadingCashCode=None):
    log.info('Запускаем скрипт выгрузки продаж формата ArtixInterchange.')
    if not unloadModule:
        messEn = 'Error importing module to unload sales.'
        messRu = 'Ошибка при подключении модуля выгрузки продаж.'
        log.error(messRu)
        return status.errorUnload, messEn, messRu
    if (path == None or path == ''):
        messEn = 'No path for file upload sales'
        messRu = 'Не указан путь для файла выгрузки продаж'
        log.error(messRu)
        return status.errorUnload, messEn, messRu
    elif (dates == None or dates == '') and (shifts == None or shifts == ''):
        messEn = 'Not specified range shifts or time range unloading sales'
        messRu = 'Время выгрузки продаж или диапазон смен не указаны.'
        log.error(messRu)
        return status.errorUnload, messEn, messRu
    else:
        statusServer = '%s:%s' % (propertyReader.remoteIp, propertyReader.cashServerRestPort)
        logmess = u'Запуск выгрузки продаж'
        if mode == 'server':
            if unloadingShopId:
                logmess += u' магазин = %s' % (unloadingShopId)
            if unloadingCashCode:
                logmess += u' касса = %s' % (unloadingCashCode)
        if dates:
            logmess += u' с %s по %s в файл %s' % (dates.split('@')[0], dates.split('@')[1], path.encode('utf8'))
        else:
            logmess += u' для смен с %s по %s в файл %s' % (shifts.split('@')[0], shifts.split('@')[1], path.encode('utf8'))
        log.info(logmess)
        try:
            curPath = sys.path[0]
            sys.path[0] = '%s/../../../' % os.path.dirname(unloadScript)
            if mode == 'server':
                result = unloadModule.mainRun(unloadingShopId, unloadingCashCode, shifts, dates, converterName='aif', dataPath=path,
                                              cashUnloadStateId=saleStateId, statusHost=statusServer, closedShifts=closedShifts)
            else:
                result = unloadModule.mainRun(shifts, dates, converterName='aif', dataPath=path, cashUnloadStateId=saleStateId,
                                              statusHost=statusServer, closedShifts=closedShifts)

            sys.path[0] = curPath
            messEn = 'Unload result - %s' % result
            messRu = 'Результат выгрузки продаж - %s' % result
            log.info(messRu)
            if result:
                return status.OK, messEn, messRu
            else:
                return status.busy, messEn, messRu
        except Exception, e:
            messEn = 'Unable to unload sales. %s' % str(e)
            messRu = 'Не удалось выгрузить продажи. %s' % str(e)
            log.error(messRu)
            log.error(traceback.format_exc())
            return status.errorUnload, messEn, messRu

def filterKeys(d):
    lowerD = {}
    for k in d.keys():
        if d[k] != None and d[k] != []:
            lowerD[k.lower()] = d[k]
    return lowerD

def checkQueueParams(target, queueName, max_size, file_size, file_count, max_count):
    existQueue = False
    if os.name == 'posix':
        sess = None
        try:
            sess = Session()
            broker = sess.addBroker(target=target)
            queues = sess.getObjects(_class="queue", _package="org.apache.qpid.broker")
            for q in queues:
                if q.name == queueName:
                    existQueue = True
                    args = q.arguments
                    if args.get('qpid.max_size', None) != max_size or args.get('qpid.file_size', None) != file_size or \
                        args.get('qpid.file_count', None) != file_count or args.get('qpid.max_count', None) != max_count:
                        if q.msgDepth == 0:
                            # если настройки очереди изменились, то удаляем эту очередь, чтобы создать с новыми настройками
                            broker.getAmqpSession().queue_delete(queue=q.name, if_empty=True, if_unused=True)
                    break
        except Exception, e:
            log.warning('Can not delete queue %s. %s' % (queueName, e))
        finally:
            if sess:
                sess.close()
    return existQueue

def jsonWrapper(jsonPiece):
    return json.loads(jsonPiece, object_hook=filterKeys)

def isMessageExist(target, queueName):
    try:
        connection = Connection(target)
        connection.tcp_nodelay = True
        connection.open()
        session = connection.session()
        receiver = session.receiver(queueName)
        # Если сообщений нет, то прилетит qpid.messaging.exceptions.Empty
        message = receiver.fetch(timeout=0.5)
        session.close()
        connection.close()
        return True
    except Exception:
        return False

def uploadWithoutQueue(fileName, dictProperties):
    '''
    Напрямую вызывает конвертер aif для дальнейшей загрузки справочников
    без очереди. Указываем где лежит файл, передаем настройки
    '''
    log.info(u'Начинаем загружать справочники напрямую в базу данных.')
    uploadScript = propertyReader.uploadScript
    curPath = sys.path[0]
    sys.path[0] = os.path.dirname(uploadScript)
    try:
        uploadModule = __import__(os.path.basename(os.path.splitext(uploadScript)[0]), fromlist='q')
        result, errorList = uploadModule.insertInDB(propertyReader, fileName, dictProperties)
        if not result:
            raise Exception
    except Exception, e:
        messRu = u'Не удалось загрузить справочники. %s' % str(e)
        log.error(messRu)
        return status.errorLoadDict, messRu
    sys.path[0] = curPath

    messRu = u'Справочники загружены'
    log.info(messRu)
    return status.OK, messRu

def createQueueSender():
    queueName = "artix.loaddict.%s" % ("All" if propertyReader.mode == 'server' else 'cash')
    connection = Connection(propertyReader.localBroker)
    connection.tcp_nodelay = True
    connection.open()
    session = connection.session(transactional=True)
    existQueue = checkQueueParams(propertyReader.localBroker, queueName, propertyReader.queueMaxSize, propertyReader.queueFileSize, propertyReader.queueFileCount, propertyReader.queueMaxCount)
    sender = session.sender("%s; {create: always, node: {type: queue, durable: True, x-declare: {arguments: {'qpid.file_size':%s, 'qpid.file_count':%s, 'qpid.max_size':%s, 'qpid.max_count':%s}}} }" %
                            (queueName, propertyReader.queueFileSize, propertyReader.queueFileCount, propertyReader.queueMaxSize, propertyReader.queueMaxCount), durable=True)
    return connection, session, sender, existQueue

def upload(fileName, dictProperties):
    dictCashLoadStateId = dictProperties['dictCashLoadStateId']
    version = dictProperties['version']
    # если пришла просто команда на генерацию справочника без самого справочника, то не будем отправлять статус конвертирования
    isSendLoadDictStatus = dictProperties.get('nesGenerateCommandMode', 'none') == 'none' and not dictProperties.get('unloadElementName', None)
    if fileName and not os.path.isfile(fileName):
        mes = u'Созданный файл-справочник не найден.'
        log.error(mes)
        status.sendDictState(dictCashLoadStateId, 'Not found file-dictionary.', mes, status.STATUS_ERROR,
                             propertyReader.httpHostCS, propertyReader.httpPortCS, version)
        return status.errorProgramm, mes

    if (not propertyReader.useQueueInUpload):
        return uploadWithoutQueue(fileName, dictProperties)

    dictCSLoadStateId = None
    if propertyReader.mode == 'server' and isSendLoadDictStatus:
        dictCSLoadStateId = status.sendDictStateForCS('All', 'Start puting dict to queue', 'Начинаем отправку справочника в очередь', status.STATUS_DURING)

    objectsCount = 0
    startMessageId = ''

    beginTransProperties = {
                            'host':'localhost',
                            'port':3306,
                            'username':'netroot',
                            'password':'netroot',
                            'dictMessageId':dictProperties['messageId'],
                            'type':'control',
                            'control':'begin_tran',
                            'dictBackOfficeId': dictCSLoadStateId,
                            'dictCashLoadStateId':dictCashLoadStateId,
                            'cashCode': propertyReader.cashCode,
                            'statusServer': '%s:%s' % (propertyReader.remoteIp, propertyReader.cashServerRestPort),
                            'version': version,
                            'nesGenerateCommandMode': dictProperties['nesGenerateCommandMode'],
                            'nesGenerateCommandCashesList': dictProperties['nesGenerateCommandCashesList'],
                            'unloadElementName': dictProperties['unloadElementName'],
                            'nesGenerateCommandOwner': dictProperties['nesGenerateCommandOwner']
                            }
    datagramProperties = {'type': 'datagram', 'serialType': 'marshal'}
    endTransProperties = {'type': 'control', 'control':'end_tran'}
    try:
        # Вываливается ошибка, когда происходит попытка закрыть session без его создания
        connection, session, sender, existQueue = None, None, None, None
        connection, session, sender, existQueue = createQueueSender()
        log.info('Отправляем сообщение - начало транзакции')
        startMessageId = hex(hash(datetime.now()))
        saveProgress(objectsCount, startMessageId)
        beginTransProperties['startMessageId'] = str(startMessageId)
        sender.send(Message(content=None, id=startMessageId, properties=beginTransProperties, durable=True))

        if fileName:
            dictFile = open(fileName, 'rb')
            fileSize = os.path.getsize(fileName)
            log.info('Отправляем объекты справочника в очередь...')
            jsonPiece = ''
            listObjects = []
            lastPercent = 0
            jloads = ujson.loads if version >= '2' else jsonWrapper
            for line in dictFile.xreadlines():
                if line.strip() == "---":
                    if jsonPiece.strip() != '':
                        listObjects.append(jloads(jsonPiece))
                    jsonPiece = ''
                else:
                    jsonPiece += ' ' + line
                if len(listObjects) >= 500:
                    sender.send(Message(content=marshal.dumps(listObjects), id=hex(hash(datetime.now())), properties=datagramProperties, durable=True))
                    objectsCount += len(listObjects)
                    del listObjects[:]
                    percent = dictFile.tell() * 100.0 / fileSize
                    if (percent - lastPercent) > 10:
                        lastPercent = percent
                        log.info('Обработано %s%%' % int(percent))
            if len(listObjects) > 0:
                sender.send(Message(content=marshal.dumps(listObjects), id=hex(hash(datetime.now())), properties=datagramProperties, durable=True))
                objectsCount += len(listObjects)
                del listObjects[:]
                log.info('Обработано 100%')
            dictFile.close()


        log.info('Отправляем сообщение - конец транзакции')
        sender.send(Message(content=None, id=hex(hash(datetime.now())), properties=endTransProperties, durable=True))
        try:
            saveProgress(objectsCount, startMessageId)
            session.commit()
            if propertyReader.mode == 'server' and not existQueue:
                status.rereadQueuesInNES()
        except Exception, e:
            session.rollback()
            mes = 'Не удалось загрузить справочник. %s' % str(e)
            log.error(mes)
            if propertyReader.mode == 'server' and isSendLoadDictStatus:
                status.sendDictStateForCS('All', 'Can not load dictionary. %s' % str(e), mes, status.STATUS_ERROR, dictStateId=dictCSLoadStateId)
            status.sendDictState(dictCashLoadStateId, 'Can not load dictionary. %s' % str(e), mes, status.STATUS_ERROR,
                                 propertyReader.httpHostCS, propertyReader.httpPortCS, version)
            deleteProgress(startMessageId)
            return status.errorLoadDict, mes
        mes = 'Справочник поставлен в очередь на загрузку.'
        log.info(mes)
        if propertyReader.mode == 'server' and isSendLoadDictStatus:
            status.sendDictStateForCS('All', 'OK', 'OK', status.STATUS_READY, dictStateId=dictCSLoadStateId)
        status.sendDictState(dictCashLoadStateId, 'Send dictionary to queue for load to DB', mes, status.STATUS_DURING,
                             propertyReader.httpHostCS, propertyReader.httpPortCS, version)
        return status.OK, mes
    except ServerError, se:
        mes = 'Не удалось загрузить справочник. %s' % str(se)
        log.error(mes)
        if propertyReader.mode == 'server' and isSendLoadDictStatus:
            status.sendDictStateForCS('All', 'Can not load dictionary. %s' % str(se), mes, status.STATUS_ERROR, dictStateId=dictCSLoadStateId)
        if isMessageExist(target=propertyReader.localBroker, queueName="artix.loaddict.%s" % ("All" if propertyReader.mode == 'server' else 'cash')):
            status.sendDictState(dictCashLoadStateId, 'Can not load dictionary. %s' % str(se), 'Не удалось загрузить справочник. Очередь не пуста',
                                 status.STATUS_DURING,
                                 propertyReader.httpHostCS, propertyReader.httpPortCS, version)
        else:
            status.sendDictState(dictCashLoadStateId, 'Can not load dictionary. %s' % str(se), 'Не удалось загрузить справочник. Очередь пуста',
                                 status.STATUS_ERROR,
                                 propertyReader.httpHostCS, propertyReader.httpPortCS, version)
        deleteProgress(startMessageId)
        return status.errorLoadDict, mes
    except Exception, e:
        mes = 'Не удалось загрузить справочник. %s' % str(e)
        log.error(mes)
        if propertyReader.mode == 'server' and isSendLoadDictStatus:
            status.sendDictStateForCS('All', 'Can not load dictionary. %s' % str(e), mes, status.STATUS_ERROR, dictStateId=dictCSLoadStateId)
        status.sendDictState(dictCashLoadStateId, 'Can not load dictionary. %s' % str(e), mes, status.STATUS_ERROR,
                             propertyReader.httpHostCS, propertyReader.httpPortCS, version)
        deleteProgress(startMessageId)
        return status.errorLoadDict, mes
    finally:
        if session:
            session.close()
        if connection:
            connection.close()

def sendCompareTableRequest(requestCash, compareName, compareResultServerId, resultServer=None):
    try:
        connection, session, sender, existQueue = createQueueSender()
        messageProperties = {
                            'type':'comparetable',
                            'cashCode': propertyReader.cashCode,
                            'requestCash': requestCash,
                            'compareName': compareName,
                            'compareResultServerId': compareResultServerId,
                            'statusServer': '%s:%s' % (propertyReader.remoteIp, propertyReader.cashServerRestPort)
                            }
        log.info(u'Отправляем сообщение для сверки данных <%s>' % (compareName))
        sender.send(Message(content=resultServer, id=hex(hash(datetime.now())), properties=messageProperties, durable=True))

        try:
            session.commit()
            if propertyReader.mode == 'server' and not existQueue:
                status.rereadQueuesInNES()
        except Exception, e:
            session.rollback()
            mes = u'Не удалось отправить сообщение для сверки данных <%s>. %s' % (compareName, str(e))
            log.error(mes)
            return status.errorLoadDict, mes
        mes = u'Cообщение для сверки данных <%s> поставлено в очередь.' % (compareName)
        log.info(mes)
        return status.OK, mes
    except Exception, e:
        mes = u'Не удалось отправить сообщение для сверки данных <%s>. %s' % (compareName, str(e))
        log.error(mes)
        return status.errorLoadDict, mes
    finally:
        if session:
            session.close()
        connection.close()

def saveProgress(objectsCount, startMessageId):
    data = {"objectsCount": objectsCount}
    filePath = "/linuxcash/cash/exchangesystems/progress/counter_%s.json" % startMessageId
    if not os.path.exists(os.path.dirname(filePath)):
        os.makedirs(os.path.dirname(filePath))
    with open(filePath, 'w') as outfile:
        json.dump(data, outfile)

def deleteProgress(startMessageId):
    filePath = "/linuxcash/cash/exchangesystems/progress/counter_%s.json" % startMessageId
    if os.path.exists(filePath):
        os.remove(filePath)
