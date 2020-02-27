#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import json
import logging
import os
import platform
import re
import socket
import sys
import threading
import time
import traceback
import uuid

# импорт необходим, в нем молча происходят некоторые подготовительные работы для того, чтоб логгер начал работать
logger = __import__('logger')
from qmf.console import Session
from snakemq.message import Message, FLAG_PERSISTENT
from snakemq.queues import QueuesManager
from snakemq.storage.sqlite import SqliteQueuesStorage
from thrift.server import TServer
from thrift.transport import TSocket

import propertyReader
import unloadRun as unloadModule
from plugins import backupFile
from plugins import status, execscript
from syncThrift import CashCommunicationService
from syncThrift.ttypes import *

# в 10.04 поддержки передачи по grpc нет и не будет
grpcSupported = 'Ubuntu-10.04-lucid' not in platform.platform()

__THRIFT_LISTENING_PORT = 7795
__THRIFT_LISTENING_HOST = '0.0.0.0'

__TASK_HANDLE_SHORT_SLEEP_TIME = 3
__TASK_HANDLE_LONG_SLEEP_TIME = 60

loggerSync = logging.getLogger('su.artix.loggerSync')


class Synchronizer:

    def __init__(self, lock, type):
        self.lock = lock

    def __enter__(self):
        self.lock.acquire()
        return self.lock

    def __exit__(self, type, value, traceback):
        self.lock.release()
        pass


unloadTasksLocker = Synchronizer(threading.Lock(), "unloadTasksLocker")
unloadedSalesTasksLocker = Synchronizer(threading.Lock(), "unloadedSalesTasksLocker")

unloadTasksQueueName = 'unloadTasks'  # имя очереди задач на выгрузку продаж (в ней обрабатываются таски на выгрузку продаж)
unloadedSalesTasksQueueName = 'unloadedSalesTasks'  # имя очереди задач на выгрузку сгенерированных продаж (архив с продажами сформирован, но еще не передан), и помещены в особую директорию


def qpidTasksHandle():
    nextSleep = __TASK_HANDLE_SHORT_SLEEP_TIME

    if checkUnloadSalesOnSkipMode() == status.skipped:
        # молча ждем внешнего события, которое сбросит ошибочное состояние
        nextSleep = __TASK_HANDLE_LONG_SLEEP_TIME
    else:
        defferedSalesUnloadResult = checkUnloadedSalesQpid()
        if defferedSalesUnloadResult != status.OK:
            nextSleep = __TASK_HANDLE_LONG_SLEEP_TIME
            updateUnloadedSalesTaskState(True)
        else:
            with unloadTasksLocker:
                queueTasks = getQueueByName(unloadTasksQueueName)
                for i in range(len(queueTasks)):
                    taskStr = queueTasks.get().data
                    if taskStr != None:
                        task = json.loads(taskStr, encoding='utf8')
                        res = unloadRun(task)
                        if res == status.busy:
                            nextSleep = __TASK_HANDLE_SHORT_SLEEP_TIME
                            break
                        elif res == status.errorQpidInsert:
                            queueTasks.pop()
                            task.update({"skip": True})
                            with unloadedSalesTasksLocker:
                                unloadedSalesTasksQueue = getQueueByName(unloadedSalesTasksQueueName)
                                unloadedSalesTasksQueue.push(Message(json.dumps(task), ttl=60, flags=FLAG_PERSISTENT))
                                unloadedSalesTasksQueue.manager.close()
                            continue
                        elif res == status.OK:
                            queueTasks.pop()
                        else:
                            nextSleep = __TASK_HANDLE_LONG_SLEEP_TIME
                            break
                queueTasks.manager.close()
    return nextSleep


def grpcTasksHandle():
    nextSleep = __TASK_HANDLE_SHORT_SLEEP_TIME
    if checkUnloadSalesOnSkipMode() == status.skipped:
        # молча ждем внешнего события, которое сбросит ошибочное состояние
        nextSleep = __TASK_HANDLE_LONG_SLEEP_TIME
    else:
        with unloadTasksLocker:
            queueTasks = getQueueByName(unloadTasksQueueName)
            for taskIndex in range(len(queueTasks)):
                taskStr = queueTasks.get().data
                if taskStr is not None:
                    task = json.loads(taskStr, encoding='utf8')
                    res = unloadRun(task)
                if res == status.busy:
                    nextSleep = __TASK_HANDLE_SHORT_SLEEP_TIME
                    break
                elif res in [status.start, status.OK]:
                    queueTasks.pop()
                    task.update({"skip": False})
                    with unloadedSalesTasksLocker:
                        unloadedSalesTasksQueue = getQueueByName(unloadedSalesTasksQueueName)
                        unloadedSalesTasksQueue.push(Message(json.dumps(task), ttl=60, flags=FLAG_PERSISTENT))
                        unloadedSalesTasksQueue.manager.close()
                else:
                    nextSleep = __TASK_HANDLE_LONG_SLEEP_TIME
                    break

            queueTasks.manager.close()
        unloadStatus, successUnloadedSalesIds, messEn, messRu = unloadModule.sendGrpcUnloadedSales()
        removeUnloadedSalesByGrpc(successUnloadedSalesIds)
        if unloadStatus not in [status.OK, status.empty]:
            nextSleep = __TASK_HANDLE_LONG_SLEEP_TIME
            #  Если по какой-либо причине не удалось отправить сообщение на сервер - оно не отправится до тех пор, пока не сбросится ошибочное состояние.
            #  Сбросить его можно при перезапуске sync-а или при получении нового запроса на выгрузку продаж через thrift
            tasks = updateUnloadedSalesTaskState(True)
            try:
                httpHostCS = propertyReader.httpHostCS
                httpPortCS = propertyReader.httpPortCS
                for task in tasks:
                    if task.get('saleStateId'):
                        status.updateSaleState(task.get('saleStateId'), messEn, messRu, status.STATUS_ERROR,
                                               httpHostCS,
                                               httpPortCS)
            except Exception, e:
                loggerSync.warn("Exception while send status on server: %s", e.message)
                loggerSync.warn(traceback.format_exc())
    return nextSleep


def unloadTasksHandle(threadHandlerController):
    while threadHandlerController.get("continueTaskHandler"):
        try:
            if grpcSupported and propertyReader.protocolVersion >= 4 and propertyReader.mode == "cash":
                nextSleep = grpcTasksHandle()
            else:
                nextSleep = qpidTasksHandle()
        except Exception, e:
            nextSleep = __TASK_HANDLE_LONG_SLEEP_TIME
            loggerSync.error('Произошла ошибка во время обработки задач на выгрузку. Попробуем обработать через минуту.')
            loggerSync.error(traceback.format_exc())
        loggerSync.debug("Sleep: %s", nextSleep)
        time.sleep(nextSleep)


def unloadRun(task):
    method = 'unload'
    try:
        # reconfigure(task.get('configInfo'))
        httpHostCS = task.get('httpHostCS', propertyReader.httpHostCS)
        httpPortCS = task.get('httpPortCS', propertyReader.httpPortCS)
        status.sendStatus(loggerSync, method, status.start, 'Start unload',
                          httpHostCS, httpPortCS)
        status.updateSaleState(task.get('saleStateId'), 'Start unload', 'Начинаем выгрузку продаж', status.STATUS_DURING,
                               httpHostCS,
                               httpPortCS)
        loggerSync.info('Начинаем выгрузку продаж')
        rez, messEn, messRu = unloadModule.main(propertyReader.localBroker,
                                                propertyReader.cashCode,
                                                propertyReader.storeID,
                                                propertyReader.mode,
                                                dates='%s@%s' % (task.get('fromDate'), task.get('toDate')),
                                                saleStateId=task.get('saleStateId'),
                                                unloadingShopId=task.get('shopId'),
                                                unloadingCashCode=task.get('cashCode'),
                                                taskId=task.get('taskId'),
                                                cashCode=task.get('configInfo').get('cashCode') if task.get('configInfo') else None,
                                                shopCode=task.get('configInfo').get('shopCode') if task.get('configInfo') else None,
                                                httpHostCS=httpHostCS,
                                                httpPortCS=httpPortCS)
        loggerSync.info('Вернулись из плагина')
        status.sendStatus(loggerSync, method, rez, messRu, httpHostCS, httpPortCS)
        loggerSync.info('Отправили статус на сервер')
        saleStatus = None
        if rez == status.OK:
            saleStatus = status.STATUS_READY
        elif rez == status.start:
            saleStatus = status.STATUS_DURING
        else:
            saleStatus = status.STATUS_ERROR
        status.updateSaleState(task.get('saleStateId'), messEn, messRu, saleStatus, httpHostCS, httpPortCS)
        loggerSync.info('Обновили статус выгрузки продаж на сервере')
        return rez
    except Exception, e:
        loggerSync.error('Error 0, %s' % str(e))
        loggerSync.error(traceback.format_exc())
        status.sendStatus(loggerSync, method, status.errorProgramm, str(e), httpHostCS, httpPortCS)
        status.updateSaleState(task.get('saleStateId'), str(e), str(e), status.STATUS_ERROR, httpHostCS, httpPortCS)
    return status.errorUnload


def getQueueByName(name):
    '''
    Получение очереди задач по имени
    '''
    return QueuesManager(SqliteQueuesStorage(name + '.db')).get_queue(name)


def checkUnloadSalesOnSkipMode():
    '''
    Если очередь недооправленных продаж оставлена по ошибке, то в каждый объект записывается поле skip=True (не спрашивайте почему в каждое сообщение,
    видимо так повелось), и в данном случае попытка отправки приостанавливается до получения "внешнего" события (перезапуск sync или запроса с КС через thrift)
    Данная функция проверяет взведение этого флага у первого объекта в очереди, и если он там есть, возвращает status.skipped, иначе возвращает None
    :return: возвращает status.skipped, если он в первой записи == True, иначе возвращает None
    '''
    returnStatus = None
    with unloadedSalesTasksLocker:
        queueTasks = getQueueByName(unloadedSalesTasksQueueName)
        if len(queueTasks) > 0:
            task = json.loads(queueTasks.get().data, encoding='utf8')
            if task.get('skip'):
                returnStatus = status.skipped
        queueTasks.manager.close()
    return returnStatus


def checkUnloadedSalesQpid():
    '''
    Функция проверки очереди невыгруженных продаж.
    1. Если в очереди есть задачи и её поле 'skip' равно True, то возвращаем status.skipped
    2. Если в очереди есть задачи и её поле 'skip' равно False или отсутствует, то выполняем выгрузку невыгруженной продажи для этой задачи.
       В случае успешной выгрузки задача удаляется из очереди
    '''
    queueTasks = None
    unloadResult = status.OK
    with unloadedSalesTasksLocker:
        try:
            queueTasks = getQueueByName(unloadedSalesTasksQueueName)
            for taskIndex in range(len(queueTasks)):
                task = json.loads(queueTasks.get().data, encoding='utf8')
                unloadResult = unloadedSalesRun(task)
                if unloadResult != status.OK:
                    break
                queueTasks.pop()
        except Exception, e:
            loggerSync.error(str(e))
            loggerSync.error(traceback.format_exc())
        finally:
            if queueTasks is not None and queueTasks.manager:
                queueTasks.manager.close()
    return unloadResult


def removeUnloadedSalesByGrpc(successUnloadedSalesIds):
    '''
    Функция исключения завершенных тасков и очереди выгрузки продаж. В качестве параметра передаются идентификаторы продажи, которые были отправлены на КС.
    Если КС их успешно обработал (по его мнению конечно же), то этот список должен содержать идентификтор этого таска
    Соответсвенно, если идентификатор задачи найден в списке - она удаляется.
    :param removeAll - удалить все таски, т.к. отложенных задач не было найдено в директории
    '''
    with unloadedSalesTasksLocker:
        # можно удалить из очереди выгруженных продаж все записи, если директория с ними пуста
        removeAll = os.listdir(backupFile.getUnloadedSalesPath()) == []
        if successUnloadedSalesIds or removeAll:
            loggerSync.debug('Success task ids: %s', successUnloadedSalesIds)
            unloadedSalesTasksQueue = getQueueByName(unloadedSalesTasksQueueName)
            for taskIndex in range(len(unloadedSalesTasksQueue)):
                task = json.loads(unloadedSalesTasksQueue.get().data, encoding='utf8')
                if removeAll:
                    unloadedSalesTasksQueue.pop()
                    loggerSync.debug('Remove task with Id: %s from task queue', task.get('taskId'))
                else:
                    if task.get('taskId') in successUnloadedSalesIds:
                        loggerSync.debug('Remove task with Id: %s from task queue', task.get('taskId'))
                        unloadedSalesTasksQueue.pop()
            loggerSync.debug('Removing tasks operation success')
            unloadedSalesTasksQueue.manager.close()


def updateUnloadedSalesTaskState(skip):
    '''
    Функция устанавливает значение поля 'skip' для всех задач очереди невыгруженных продаж равным значению передаваемого параметра skip
    :return список "плохих" тасков
    '''
    tasksList = []
    try:
        with unloadedSalesTasksLocker:
            queueTasks = getQueueByName(unloadedSalesTasksQueueName)
            for taskIndex in range(len(queueTasks)):
                taskStr = queueTasks.get().data
                if taskStr != None:
                    task = json.loads(taskStr, encoding='utf8')
                    task.update({'skip': skip})
                    queueTasks.pop()
                    queueTasks.push(Message(json.dumps(task), ttl=60, flags=FLAG_PERSISTENT))
                    tasksList.append(task)
            queueTasks.manager.close()
        if skip is True:
            loggerSync.info("Выгрузка продаж остановлена из-за ошибки в передаче данных, повторная попытка произойдет после перезапуска sync-а"
                            " или запроса на выгрузку продаж с КС")
    except Exception, e:
        loggerSync.error(str(e))
        loggerSync.error(traceback.format_exc())
    return tasksList


def unloadedSalesRun(task):
    '''
    Запуск выгрузки невыгруженных продаж
    '''
    # reconfigure(task.get('configInfo')) # а нужен ли?
    return unloadModule.unloadedSales(propertyReader.localBroker, propertyReader.mode, task)


def removeRoutesByQueue(broker, queue, remoteBroker, routeDestExchange):
    sess = Session()
    _connTimeout = 10
    localBroker = sess.addBroker(broker, _connTimeout)
    localBroker._waitForStable()
    localBroker.getAmqpSession().queue_declare(queue=queue, passive=False, durable=True, arguments={'qpid.file_size': 24, 'qpid.file_count': 8})
    bridges = sess.getObjects(_class="bridge", dynamic=False)
    allLinks = []
    for bridge in bridges:
        allLinks.append(bridge._linkRef_)
    changedRemoteBroker = True
    for bridge in bridges:
        if bridge.src == queue:
            remotelink = bridge._linkRef_
            if remoteBroker.split(':')[0] == remotelink.host and remoteBroker.split(':')[1] == str(remotelink.port) and bridge.dest == routeDestExchange and bridge.sync:
                changedRemoteBroker = False
                loggerSync.info('Routing already added')
                continue

            for i in range(len(allLinks)):
                if allLinks[i].host == remotelink.host and allLinks[i].port == remotelink.port:
                    allLinks.__delitem__(i)
                    break
            loggerSync.info("Removing route (from broker=%s queue=%s to broker=%s:%s exchange=%s)" % (
                bridge.getBroker().getUrl(), bridge.src, remotelink.host, remotelink.port, bridge.dest))
            res = bridge.close()
            if res.status != 0:
                loggerSync.error("Error closing bridge: %d - %s" % (res.status, res.text))

            delLinkFlag = True
            for i in range(len(allLinks)):
                if allLinks[i].host == remotelink.host and allLinks[i].port == remotelink.port:
                    delLinkFlag = False
                    break
            if delLinkFlag:
                loggerSync.info("Closing link %s:%s" % (remotelink.host, remotelink.port))
                res = remotelink.close()
                if res.status != 0:
                    loggerSync.error("Error closing link: %d - %s" % (res.status, res.text))

    try:
        sess.delBroker(localBroker)
        sess.close()
    except Exception, e:
        loggerSync.error('Error on broker session closing. %s' % str(e))
    return changedRemoteBroker


class CashCommunicationHandler:
    lockLoad = threading.Lock()

    def load(self, loadScript, configInfo=None):
        method = 'load'
        if self.lockLoad.acquire(False):
            try:
                res = status.errorProgramm
                reconfigure(configInfo.__dict__ if configInfo else None)
                status.sendStatus(loggerSync, method, status.start, 'Start load', propertyReader.httpHostCS, propertyReader.httpPortCS)
                loggerSync.info('Начинаем загружать справочники')

                loggerSync.info("Идентификатор потока, захватившего загрузку %s, %s" % (str(threading.current_thread().ident), threading.current_thread().name))
                plugin = __import__('sync', fromlist='q')
                rez, mess = plugin.main(propertyReader.remoteBroker, propertyReader.cashCode, propertyReader.brokerConnectionHeartbeat)
                loggerSync.info('Вернулись из плагина')
                status.sendStatus(loggerSync, method, rez, mess, propertyReader.httpHostCS, propertyReader.httpPortCS)
                loggerSync.info('Отправили статус на сервер')
            except Exception, e:
                loggerSync.error('Error 0, %s' % str(e))
                status.sendStatus(loggerSync, method, status.errorProgramm, str(e), propertyReader.httpHostCS, propertyReader.httpPortCS)
                res = status.errorProgramm
            finally:
                loggerSync.info('Зашли в finally')
                loggerSync.info(
                    "Идентификатор потока, разблокировавший загрузку %s, %s" % (str(threading.current_thread().ident), threading.current_thread().name))
                self.lockLoad.release()
                loggerSync.info('Процесс загрузки разблокирован')
                return res
        else:
            loggerSync.info('Процесс загрузки справочников занят!')
            loggerSync.info("Отказано потоку с идентификатором %s, %s" % (str(threading.current_thread().ident), threading.current_thread().name))
            status.sendStatus(loggerSync, method, status.busy, 'Busy', propertyReader.httpHostCS, propertyReader.httpPortCS)
            return status.busy

    def unload(self, unloadScript, fromDate, toDate, shopId=None, cashCode=None, configInfo=None):
        method = 'unload'
        configInfoDict = configInfo.__dict__ if configInfo else None
        try:
            reconfigure(configInfo.__dict__ if configInfo else None)
            httpHostCS = propertyReader.httpHostCS
            httpPortCS = propertyReader.httpPortCS
            updateUnloadedSalesTaskState(False)
            saleStateId = None
            addToQueue = True
            # если в очереди уже есть запросы с диапазоном дат входящим в диапазон текущего запроса, то изменим начало текущего запроса
            try:
                with unloadTasksLocker:
                    queueTasks = getQueueByName(unloadTasksQueueName)
                    addToQueue, fromDate, saleStateId = self.normalizeTasks(fromDate, toDate, shopId, cashCode, saleStateId, queueTasks,
                                                                            httpHostCS, httpPortCS)
                    queueTasks.manager.close()
                if addToQueue:
                    with unloadedSalesTasksLocker:
                        unloadedSalesQueue = getQueueByName(unloadedSalesTasksQueueName)
                        addToQueue, fromDate, saleStateId = self.normalizeTasks(fromDate, toDate, shopId, cashCode, saleStateId, unloadedSalesQueue,
                                                                                httpHostCS, httpPortCS)
                        unloadedSalesQueue.manager.close()
            except Exception, e:
                loggerSync.warning(u'Не удалось проверить необработанные запросы продаж. %s' % str(e))
                loggerSync.error(traceback.format_exc())

            if addToQueue:
                saleStateId = status.addSaleState('Putting task for unload sales to queue', 'Ставим в очередь задачу на выгрузку продаж',
                                                  status.STATUS_DURING,
                                                  httpHostCS, httpPortCS, fromDate, toDate, callFromCS=True)
                task = {'fromDate': fromDate,
                        'toDate': toDate,
                        'shopId': shopId,
                        'cashCode': cashCode,
                        'configInfo': configInfoDict,  # преобразованный в словарь набор параметров, которые прислал КС. Вопрос, нужны ли они здесь.
                        'saleStateId': saleStateId,
                        'httpHostCS': httpHostCS,
                        'httpPortCS': httpPortCS,
                        'taskId': str(uuid.uuid4()) # для того, чтоб была возможность связать задачу на выгрузку с выгруженной продажей, генерирую идентификатор к ней уже здесь
                        }
                with unloadTasksLocker:
                    queueTasks = getQueueByName(unloadTasksQueueName)
                    queueTasks.push(Message(json.dumps(task), ttl=60, flags=FLAG_PERSISTENT))
                    queueTasks.manager.close()
                loggerSync.info(
                    u'Задача на выгрузку продаж поставлена в очередь (fromDate=%s, toDate=%s, shopId=%s, cashCode=%s).' % (
                        fromDate, toDate, shopId, cashCode))

            res = status.OK
        except Exception, e:
            loggerSync.error('Error %s:', e.__class__.__name__)
            loggerSync.error(str(e.message))
            loggerSync.error(traceback.format_exc())
            status.sendStatus(loggerSync, method, status.errorProgramm, str(e),
                              httpHostCS,
                              httpPortCS)
            status.updateSaleState(saleStateId, str(e), str(e), status.STATUS_ERROR, httpHostCS, httpPortCS)
            res = status.errorProgramm
        finally:
            loggerSync.info('Зашли в finally')
            loggerSync.info('Процесс выгрузки разблокирован')
            return res

    def execute(self, command):
        loggerSync.info('С КС получен скрипт для запуска')
        try:
            result = execscript.execute(command)
            return ExecuteResult(out=result.get('out'), err=result.get('err'))
        except Exception, e:
            return ExecuteResult(out='', err='%s' % str(e))

    def normalizeTasks(self, fromDate, toDate, shopId, cashCode, saleStateId, queueTasks, host, port):
        addToQueue = True
        for i in range(len(queueTasks)):
            taskStr = queueTasks.queue[i].data
            if taskStr != None:
                taskFromQueue = json.loads(taskStr, encoding='utf8')
                if taskFromQueue['shopId'] == shopId and taskFromQueue['cashCode'] == cashCode:
                    if taskFromQueue['fromDate'] == fromDate and taskFromQueue['toDate'] == toDate:
                        messRu = u'Задача за диапазон [%s - %s] уже поставлена в очередь на обработку' % (fromDate, toDate)
                        loggerSync.info(messRu)
                        addToQueue = False
                        saleStateId = status.addSaleState('Task for date [%s - %s] already in queue' % (fromDate, toDate), messRu, status.STATUS_READY,
                                                          host, port, fromDate, toDate, callFromCS=True)
                    elif taskFromQueue['fromDate'] <= fromDate and taskFromQueue['toDate'] > fromDate and taskFromQueue['toDate'] < toDate:
                        loggerSync.info(
                            u'В очереди уже есть запрос на выгрузку продаж, диапазон дат которого перекрывает часть текущего запроса. Изменяем время начала запроса продаж %s -> %s' % (
                                fromDate, taskFromQueue['toDate']))
                        fromDate = taskFromQueue['toDate']
        return addToQueue, fromDate, saleStateId


def reconfigure(configInfo):
    properies = propertyReader
    if not configInfo is None:
        for k, v in configInfo.items():
            configInfo[k] = v.encode('utf8') if isinstance(v, unicode) else v
        changedConfigProperties = False
        changedQpidProperties = False
        cashId = configInfo.get('cashId')
        if not cashId is None and properies.cashCode != cashId:
            loggerSync.info(u'Изменяем настройку cash.code: %s -> %s' % (properies.cashCode, cashId))
            properies.cashCode = cashId
            rewriteConfig('cash.code', cashId)
            changedConfigProperties = True

        shopId = configInfo.get('shopId')
        if not shopId is None and properies.storeID != shopId:
            loggerSync.info(u'Изменяем настройку storeID: %s -> %s' % (properies.storeID, shopId))
            properies.storeID = shopId
            rewriteConfig('storeID', shopId)
            changedConfigProperties = True

        qpidHostCS = configInfo.get('qpidHostCS')
        httpHostCS = configInfo.get('httpHostCS')
        if not qpidHostCS is None:
            if properies.remoteIp != qpidHostCS:
                loggerSync.info(u'Изменяем настройку dept.broker.ip: %s -> %s' % (properies.remoteIp, qpidHostCS))
                properies.remoteIp = qpidHostCS
                properies.remoteBroker = re.sub(r"@(.+):", "@%s:" % qpidHostCS, properies.remoteBroker)
                rewriteConfig('dept.broker.ip', qpidHostCS)
                changedQpidProperties = True
                changedConfigProperties = True
        elif not httpHostCS is None and properies.remoteIp != httpHostCS:
            loggerSync.info(u'Изменяем настройку dept.broker.ip: %s -> %s' % (properies.remoteIp, httpHostCS))
            properies.remoteIp = httpHostCS
            properies.remoteBroker = re.sub(r"@(.+):", "@%s:" % httpHostCS, properies.remoteBroker)
            rewriteConfig('dept.broker.ip', httpHostCS)
            changedQpidProperties = True
            changedConfigProperties = True

        if httpHostCS is not None and properies.httpHostCS != httpHostCS:
            loggerSync.info(u'Изменяем настройку httpHostCs: %s -> %s' % (properies.httpHostCS, httpHostCS))
            properies.httpHostCS = httpHostCS
            rewriteConfig('httpHostCS', httpHostCS)
            changedConfigProperties = True

        qpidPortCS = configInfo.get('qpidPortCS')
        if (not qpidPortCS is None) and (qpidPortCS != 0) and (int(properies.remotePort) != int(qpidPortCS)):
            loggerSync.info(u'Изменяем настройку dept.broker.port: %s -> %s' % (properies.remotePort, qpidPortCS))
            properies.remotePort = qpidPortCS
            properies.remoteBroker = re.sub(r":\d+", ":%s" % qpidPortCS, properies.remoteBroker)
            rewriteConfig('dept.broker.port', qpidPortCS)
            changedQpidProperties = True
            changedConfigProperties = True

        httpPortCS = configInfo.get('httpPortCS')
        if (not httpPortCS is None) and (httpPortCS != 0) and (int(properies.cashServerRestPort) != int(httpPortCS)):
            loggerSync.info(u'Изменяем настройку cashServerRestPort: %s -> %s' % (properies.cashServerRestPort, httpPortCS))
            properies.cashServerRestPort = httpPortCS
            properies.httpPortCS = httpPortCS
            rewriteConfig('cashServerRestPort', httpPortCS)
            rewriteConfig('httpPortCS', httpPortCS)
            changedConfigProperties = True

        dictUrlHostCS = configInfo.get('dictUrlHostCS')
        if (not dictUrlHostCS is None) and (properies.dictUrlHostCS != dictUrlHostCS):
            loggerSync.info(u'Изменяем настройку dictUrlHostCS: %s -> %s' % (properies.dictUrlHostCS, dictUrlHostCS))
            properies.dictUrlHostCS = dictUrlHostCS
            rewriteConfig('dictUrlHostCS', dictUrlHostCS)
            changedConfigProperties = True

        dictUrlPortCS = configInfo.get('dictUrlPortCS')
        if (not dictUrlPortCS is None) and (properies.dictUrlPortCS != dictUrlPortCS):
            loggerSync.info(u'Изменяем настройку dictUrlPortCS: %s -> %s' % (properies.dictUrlPortCS, dictUrlPortCS))
            properies.dictUrlPortCS = dictUrlPortCS
            rewriteConfig('dictUrlPortCS', dictUrlPortCS)
            changedConfigProperties = True

        clusterId = configInfo.get('clusterId')
        if (not clusterId is None) and (properies.clusterId != clusterId):
            loggerSync.info(u'Изменяем настройку clusterId: %s -> %s' % (properies.clusterId, clusterId))
            properies.clusterId = clusterId
            rewriteConfig('clusterId', clusterId)
            changedConfigProperties = True

        grpcHostCS = configInfo.get('grpcHostCS')
        if (not grpcHostCS is None) and (properies.grpcHostCS != grpcHostCS):
            loggerSync.info(u'Изменяем настройку grpcHostCS: %s -> %s' % (properies.grpcHostCS, grpcHostCS))
            properies.grpcHostCS = grpcHostCS
            rewriteConfig('grpcHostCS', grpcHostCS)
            changedConfigProperties = True

        grpcPortCS = configInfo.get('grpcPortCS')
        if (not grpcPortCS is None) and (properies.grpcPortCS != grpcPortCS):
            loggerSync.info(u'Изменяем настройку grpcPortCS: %s -> %s' % (properies.grpcPortCS, grpcPortCS))
            properies.grpcPortCS = grpcPortCS
            rewriteConfig('grpcPortCS', grpcPortCS)
            changedConfigProperties = True

        protocolVersion = configInfo.get('protocolVersion')
        if (not protocolVersion is None) and (properies.protocolVersion != protocolVersion):
            loggerSync.info(u'Изменяем настройку protocolVersion: %s -> %s' % (properies.protocolVersion, protocolVersion))
            properies.protocolVersion = protocolVersion
            rewriteConfig('protocolVersion', protocolVersion)
            changedConfigProperties = True

        unloadSalesRequestThroughCS = configInfo.get('unloadSalesRequestThroughCS')
        if (not unloadSalesRequestThroughCS is None) and (properies.unloadSalesRequestThroughCS != unloadSalesRequestThroughCS):
            loggerSync.info(u'Изменяем настройку unloadSalesRequestThroughCS: %s -> %s' % (properies.unloadSalesRequestThroughCS, unloadSalesRequestThroughCS))
            properies.unloadSalesRequestThroughCS = unloadSalesRequestThroughCS
            rewriteConfig('unloadSalesRequestThroughCS', unloadSalesRequestThroughCS)
            changedConfigProperties = True

        if changedConfigProperties:
            loggerSync.info(u'Перезаписываем конфигурационный файл.')
            with open(properies.pathProperties, 'wb') as configfile:
                properies.config.write(configfile)

        if changedQpidProperties:
            configureRouting()


def rewriteConfig(propertyName, value):
    propertyReader.config.set('Properties', propertyName, value.encode('utf8') if isinstance(value, unicode) else value)


def createAndBindQueueToExchane(broker, queue, exchange=None, routingKey='#'):
    if exchange:
        sess = Session()
        _connTimeout = 10
        localBroker = sess.addBroker(broker, _connTimeout)
        localBroker._waitForStable()
        localBroker.getAmqpSession().queue_declare(queue=queue, passive=False, durable=True, arguments={'qpid.file_size': 24, 'qpid.file_count': 8})

        localBroker.getAmqpSession().exchange_declare(exchange=exchange, passive=False, durable=True, type='topic', auto_delete=False)
        localBroker.getAmqpSession().exchange_bind(exchange=exchange, queue=queue, binding_key=routingKey)
        try:
            sess.delBroker(localBroker)
            sess.close()
        except Exception, e:
            loggerSync.error('Error on broker session closing. %s' % str(e))


def configureRouting():
    routeQueue = 'artix.backward.cs'
    bindExchange = routeDestExchange = 'artix.backward.cs'
    isConfigure = False
    times = 5  # количество попыток
    error = ''
    while not isConfigure and times:
        loggerSync.info('Пытаемся подключится к локальному серверу')
        times -= 1
        try:
            createAndBindQueueToExchane(broker=propertyReader.localBroker, queue=routeQueue, exchange=bindExchange)
            if (propertyReader.mode == 'server'):
                routeQueue = 'artix.backward.mcs'
                createAndBindQueueToExchane(broker=propertyReader.localBroker, queue=routeQueue, exchange=bindExchange)
            addNewRoute = removeRoutesByQueue(broker=propertyReader.localBroker, queue=routeQueue, remoteBroker=propertyReader.remoteBroker.split('@')[1],
                                              routeDestExchange=routeDestExchange)
            if addNewRoute:
                loggerSync.info("Adding route from broker=%s queue=%s to broker=%s exchange=%s" % (
                    propertyReader.localBroker.split('@')[1], routeQueue, propertyReader.remoteBroker.split('@')[1], routeDestExchange))
                os.system(
                    "qpid-route -d -s --ack=1 queue add %s %s %s %s" % (propertyReader.remoteBroker, propertyReader.localBroker, routeDestExchange, routeQueue))
        except socket.error, e:
            error = str(e)
            time.sleep(10)
        else:
            isConfigure = True
    if not isConfigure:
        loggerSync.error('Ошибка подключения к локальному сервису qpid. %s ' % (error))


#
#   Данный метод необходимо выполнить здесь, так как 
#   в exchangers в модуле startReader, который используется для формирования
#   выгрузки продаж, используется модуль signal, который должен быть подгружен
#   только в главном потоке. Иначе будет ошибка импортирования.
#   Такой же импорт находится в модуле artixinterchange.py
#
def importUnloadModule():
    try:
        curPath = sys.path[0]
        unloadScript = propertyReader.unloadScript
        sys.path[0] = '%s' % os.path.dirname(unloadScript)
        __import__(os.path.basename(os.path.splitext(unloadScript)[0]), fromlist='q')
        sys.path[0] = curPath
    except Exception, e:
        messRu = 'Ошибка при подключении модуля выгрузки продаж.'
        loggerSync.error('%s %s' % (messRu, str(e)))
        os._exit(1)


def serveThrift(threadHandlerController):
    transport = TSocket.TServerSocket(__THRIFT_LISTENING_HOST, __THRIFT_LISTENING_PORT)
    handler = CashCommunicationHandler()
    processor = CashCommunicationService.Processor(handler)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    loggerSync.info('Starting the server...')

    class SyncThriftServer(TServer.TThreadedServer):
        def serve(self):
            self.serverTransport.listen()
            threadNumber = 0
            while threadHandlerController.get('continueThriftHandler'):
                threadNumber += 1
                try:
                    client = self.serverTransport.accept()
                    t = threading.Thread(target=self.handle, name="thandler_%s" % threadNumber, args=(client,))
                    t.setDaemon(self.daemon)
                    t.start()
                except KeyboardInterrupt:
                    raise
                except Exception as x:
                    logging.exception(x)

    # You could do one of these for a multithreaded server
    server = SyncThriftServer(processor, transport, tfactory, pfactory, daemon=True)
    server.serve()
    loggerSync.info('Thrift server gone')


def runThriftThread(threadHandlerController):
    thriftThread = threading.Thread(target=serveThrift, name="thrift", args=(threadHandlerController,))
    thriftThread.setDaemon(True)
    thriftThread.start()
    return thriftThread


def runUnloadTaskHandler(threadHandlerController):
    unloadTasksHandler = threading.Thread(target=unloadTasksHandle, name="tasks", args=(threadHandlerController,))
    unloadTasksHandler.setDaemon(True)
    unloadTasksHandler.start()
    return unloadTasksHandler


def stackTrace():
    '''
    Если вдруг sync начнет делать непонятности в рантайме, и будет полезен stack trace его работы - создайте файл
    /tmp/sync_stack_trace (touch /tmp/sync_stack_trace), и stacktrace всех потоков появится в логе. Такой себе backdoor
    :return:
    '''
    while True:
        time.sleep(10)
        if os.path.exists("/tmp/sync_stack_trace"):
            code = []
            for threadId, stack in sys._current_frames().items():
                code.append("\n# ThreadID: %s" % threadId)
                for filename, lineno, name, line in traceback.extract_stack(stack):
                    code.append('File: "%s", line %d, in %s' % (filename,
                                                                lineno, name))
                    if line:
                        code.append("  %s" % (line.strip()))

            loggerSync.info("Stacktrace:")
            for line in code:
                print loggerSync.info(line)
            os.remove("/tmp/sync_stack_trace")


def startStackTraceKeeper():
    stackTraceThread = threading.Thread(target=stackTrace, name="stackTraceKeeper")
    stackTraceThread.setDaemon(True)
    stackTraceThread.start()


def startSync():
    importUnloadModule()
    startStackTraceKeeper()
    if not grpcSupported or propertyReader.protocolVersion < 4 or propertyReader.mode == 'server':
        configureRouting()
    else:
        loggerSync.info('Использую grpc протокол для доставки продаж на сервер. Параметры подключения: %s:%s', propertyReader.grpcHostCS,
                        propertyReader.grpcPortCS)
    updateUnloadedSalesTaskState(False)
    threadHandlerController = {"continueTaskHandler": True,
                               "continueThriftHandler": True}  # Необходимо для тестов, чтоб была возможность остановить handler, если это потребуется
    runUnloadTaskHandler(threadHandlerController)
    thriftThread = runThriftThread(threadHandlerController)
    thriftThread.join()


if __name__ == '__main__':
    try:
        startSync()
    except Exception as e:
        print("Fatal exception:\n")
        traceback.print_exc()
        print(str(e))
        loggerSync.fatal("Fatal exception: %s", str(e))
        loggerSync.fatal(traceback.format_exc())
        raise e
    loggerSync("Shutdown sync")
