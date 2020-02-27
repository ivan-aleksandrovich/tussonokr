# -*- coding: UTF-8 -*-
import logging
import optparse
import os
import platform
import re
import tempfile
import traceback
import uuid
import zipfile
import httplib2

import sys
import time
from datetime import datetime
from qpid.messaging import *

from plugins import status, backupFile

# в 10.04 поддержки передачи по grpc нет и не будет
grpcSupported = 'Ubuntu-10.04-lucid' not in platform.platform()
if grpcSupported:
    import grpc
    from syncGrpc import load_sale_pb2
    from syncGrpc import load_sale_pb2_grpc

logger = __import__('logger')
loggerSync = logging.getLogger('su.artix.loggerSync')
propertyReader = __import__("propertyReader")

#
# Запрос выгрузки продаж и помещения в очередь
#

pluginPath = 'plugins'
# Запрашиваемые параметры выгрузки
# если вызвали без параметров, то начало выгрузки = минус 2 часа от текущего времени, конец выгрузки = текущее время
CSReguestBegin = time.strftime("%Y-%m-%dT%H:%M:%S.0", time.localtime())
beginTime = int(CSReguestBegin[11:13]) - 2
if beginTime < 0:
    beginTime = beginTime + 24
    CSReguestBegin = CSReguestBegin[:11] + '%s' % beginTime + CSReguestBegin[13:]
elif beginTime < 10:
    CSReguestBegin = CSReguestBegin[:11] + '0%s' % beginTime + CSReguestBegin[13:]  # '0%s' % beginTime
else:
    CSReguestBegin = CSReguestBegin[:11] + '%s' % beginTime + CSReguestBegin[13:]
print CSReguestBegin
CSReguestEnd = time.strftime("%Y-%m-%dT%H:%M:%S.0", time.localtime())
cashIdMacro = '%(cashid)s'
defaultPathSales = os.path.normpath('%s/sales_for_cs/sales%s.json' % (tempfile.gettempdir(), cashIdMacro + '_%(timestamp)s'))
format = 'artixinterchange'


class ObjectTranslator(object):
    item = None


def prepareSaleMessages(currentTaskBag):
    '''
    Подготовить массив с сообщениями продаж, которые будут отправлены на КС. Используются возможности python и его генераторов (это из grpc), поэтому
    возвращается генератор. Как я понимаю, генератор собирается последовательно, т.е. пока не будет обработан предыдущий yield, следующий выполняться не
    начнет. Но т.к. в процессе обработки могут возникнуть ошибки, нужно знать, на каком сообщении она возникла. Для этого в функцию передается параметр, класса
    ObjectTranslator, в котором будет находится properties последнего обрабатываемого сообщения.
    :param currentTaskBag:
    :return:
    '''
    pathSales = os.path.normpath('%s/unloaded_sales_for_cs' % tempfile.gettempdir())
    if not os.path.exists(pathSales):
        os.makedirs(pathSales)
    for root, dirs, files in os.walk(backupFile.getUnloadedSalesPath()):
        if files:
            for file in files:
                archiveFileWithProps = os.path.join(root, file)
                loggerSync.info('Продажи из архива: %s.', archiveFileWithProps)
                # 1. Достать из архива проперти и получить архив без пропертей(нужно работать в темповой директории)
                archiveFile, props = backupFile.getUnloadedSale(archiveFileWithProps, pathSales)
                currentTaskBag.item = props
                saleId = props.get('salesId', 'SALES_ID_DID_NOT_SET!!!_' + str(uuid4()))
                cashUnloadStateId = props.get('cashUnloadState', '')
                # 2. Пихнуть архив без пропертей в очередь
                with open(archiveFile, 'rb') as binarySalesFile:
                    sale = load_sale_pb2.SalesPackage()
                    sale.id = saleId
                    sale.cashId = props.get('cashCode')
                    sale.shopId = props.get('shopId', '')
                    saleType = props.get('saleType')
                    if saleType == 'dates':
                        sale.dateTimeBegin = props.get('CSReguestBegin')
                        sale.dateTimeEnd = props.get('CSReguestEnd')
                        sale.typeOfSales = load_sale_pb2.SalesPackage.TypeOfSales.Value('BY_INTERVAL')
                    elif saleType == 'shifts':
                        sale.shiftNumberBegin = props.get('CSReguestBeginShift')
                        sale.shiftNumberEnd = props.get('CSReguestEndShift')
                        sale.typeOfSales = load_sale_pb2.SalesPackage.TypeOfSales.Value('BY_SHIFT')
                    else:
                        # указываю, что выгрузка почековая, если она не по смене и не по интервалу. Может, это и неправильно
                        sale.typeOfSales = load_sale_pb2.SalesPackage.TypeOfSales.Value('BY_CHECK')
                    if cashUnloadStateId:
                        sale.cashUnloadState = cashUnloadStateId
                    else:
                        loggerSync.warning('For task with taskId: %s cashUnloadStateId not set!', saleId)
                    sale.saleFile = binarySalesFile.read()
                    yield sale
                # 3. зархивировали бекапы, удали темповые файлы, отправили статус (если все хорошо, потом, в вызываемой методе,
                #  не нужно повторно отправлять статус, что продажи успешно отправлены
                backupFile.move(archiveFile, backupFile.prepareDirectory('sales'))
                backupFile.remove(archiveFileWithProps)
                status.updateSaleState(cashUnloadStateId,
                                       'Sale message send',
                                       'Сообщение с продажами отправлено',
                                       status.STATUS_READY,
                                       props.get('httpHostCS', propertyReader.httpHostCS),
                                       props.get('httpPortCS', propertyReader.httpPortCS))
                status.sendStatus(loggerSync,
                                  'unload',
                                  status.OK,
                                  'Сообщение с продажами отправлено',
                                  props.get('httpHostCS', propertyReader.httpHostCS),
                                  props.get('httpPortCS', propertyReader.httpPortCS))


def getTaskIdAndBrokenCashUnloadStateId(taskBag):
    props = taskBag.item
    taskId = None
    brokenCashUnloadStateId = None
    if props and props.get('salesId'):
        taskId = props.get('salesId')
        brokenCashUnloadStateId = props.get('cashUnloadState')
    return taskId, brokenCashUnloadStateId


def sendGrpcUnloadedSales():
    '''
    Отправить сформированные продажи по grpc-каналу. Берутся все подготовленные продажи и происходит попытка их отправки
    :return: (status, successUnloadedSalesIds, messEn, messRu)
     status - статус операции, status.OK, если все подготовленные продажи выгрузились успешно, иначе к-л другой (м.б None)
     successUnloadedSalesIds - лист идентификаторов удачно отправленных продаж (список, который возвращает сервер, поле props.salesId из продаж является этим
      идентификатором)
    '''
    rezStatus = status.OK
    rezSuccessUnloadedSalesIds = []
    messEn = "OK"
    messRu = "ОК"
    grpcHost = propertyReader.grpcHostCS
    grpcPort = propertyReader.grpcPortCS
    saleUnloadStatus = None
    currentTaskBag = ObjectTranslator()

    if os.listdir(backupFile.getUnloadedSalesPath()):
        loggerSync.info('Подготавливаем соединение к серверу для передачи сообщения по grpc')
        with grpc.insecure_channel('%s:%s' % (grpcHost, grpcPort)) as channel:
            stubSales = load_sale_pb2_grpc.SalesStub(channel)
            handshakeRequest = load_sale_pb2.Handshake()
            handshakeRequest.cashId = propertyReader.cashCode
            stubHandshakeResponse = load_sale_pb2_grpc.HandshakeRequestStub(channel)
            try:
                handshakeResponse = stubHandshakeResponse.handshake(handshakeRequest)
                if handshakeResponse.connectionAccepted:
                    if 1 in handshakeResponse.compabilityVersions:
                        saleIterator = prepareSaleMessages(currentTaskBag)
                        response = stubSales.load(saleIterator)
                        if response.requestId:
                            rezSuccessUnloadedSalesIds = response.requestId
                        if not response.success:
                            rezStatus = status.errorUnload
                            taskId, brokenCashUnloadStateId = getTaskIdAndBrokenCashUnloadStateId(currentTaskBag)
                            messEn = "Can't unload sales, exception from server message with taskId: %s - %s (%s)" % \
                                     (taskId, str(response.description), str(response.errorCode))
                            messRu = "Исключение во время выгрузки продаж через grpc на сервер, сервер возвратил ошибку: %s" % str(response.description)
                            loggerSync.warn(messEn)
                            if brokenCashUnloadStateId:
                                status.updateSaleState(brokenCashUnloadStateId, messEn, messRu, status.STATUS_ERROR, grpcHost, grpcPort)
                    else:
                        messEn = 'Unsupported in server grpc protocol version. Server support: %s, client support only version 1' \
                                 % handshakeResponse.compabilityVersions
                        messRu = 'Неподдерживаемая версия обмена для grpc. Сервер поддерживает версии: %s, однако клиент только версию 1' \
                                 % handshakeResponse.compabilityVersions
                        loggerSync.info(messEn)
                        rezStatus = status.errorUnload
                else:
                    messEn = 'Server deny connection: %s (%s)' \
                             % (str(handshakeResponse.description), str(handshakeResponse.errorCode))
                    messRu = 'Сервер запретил соединение по причине: %s (%s)' \
                             % (str(handshakeResponse.description), str(handshakeResponse.errorCode))
                    loggerSync.info(messEn)
                    rezStatus = status.errorUnload
            except grpc.RpcError as rpcError:
                taskId, brokenCashUnloadStateId = getTaskIdAndBrokenCashUnloadStateId(currentTaskBag)
                messEn = 'Exception while communicate with remote grpc service on message with taskId: %s - (%s:%s): %s: %s' % \
                         (taskId, grpcHost, grpcPort, rpcError.code(), rpcError.details())
                messRu = 'Не удалось выгрузить продажи, ошибка в момент передачи данных по grpc (%s:%s): %s: %s' % \
                         (grpcHost, grpcPort, rpcError.code(), rpcError.details())
                loggerSync.warn(messEn)
                if brokenCashUnloadStateId:
                    status.updateSaleState(brokenCashUnloadStateId, messEn, messRu, status.STATUS_ERROR, propertyReader.httpHostCS, propertyReader.httpPortCS)
                rezStatus = status.errorUnload
            if channel:
                channel.close()
        return rezStatus, rezSuccessUnloadedSalesIds, messEn, messRu
    else:
        return status.empty, None, "Directory with sales is empty", "Файлов продаж для отправки не найдено"


def main(broker, cashId, shopId, mode, dates=None, shifts=None, closedShifts=0, saleStateId=None, unloadingShopId=None, unloadingCashCode=None,
         taskId=None, cashCode=None, shopCode=None, httpHostCS=None, httpPortCS=None):
    notNoneTaskId = taskId if taskId is not None else str(uuid.uuid4())
    notNoneHttpHostCS = httpHostCS if httpHostCS else propertyReader.httpHostCS
    notNoneHttpPortCS = httpPortCS if httpPortCS else propertyReader.httpPortCS

    if grpcSupported and propertyReader.protocolVersion >= 4 and propertyReader.mode == "cash":
        # при посылке через grpc решил сделать отдельные методы. поэтому получилось немного дублирования кода.
        return prepareThroughGrpc(mode,  # где запущен этот инстанс sync-core - server или cash
                                  dates,  # диапазон дат, за которые нужно запросить продажи, в формате
                                  # 'YYYY-MM-dd'T'HH_mm_ss.SSS@YYYY-MM-dd'T'HH_mm_ss.SSS', м.б. не указан
                                  shifts,  # диапазон самен, в формате 'shift@shift', м.б. не указан
                                  closedShifts,
                                  saleStateId,  # идентификатор статуса выгрузки продаж, которые присылает КС при формировании сигнала на выгрузку продаж,
                                  # если запрос формирует сама касса, то она предварительно делает запрос на сервер, для получения этого значения
                                  unloadingShopId,
                                  unloadingCashCode,
                                  cashId,  # идентификатор кассы, с которой производится запрос продаж, значение приходит с КС
                                  shopId,  # идентификатор магазина, в котором находится касса, с которой производится запрос продаж, значение приходит с КС
                                  cashCode,  # непосредственно код кассы, должен совпадать с кодом из реестра кассы
                                  shopCode,  # непосредственно код магазина, в котором находится касса
                                  notNoneTaskId,  # идентификтор задачи на выгрузку, генерируется на стороне sync-core, должен проследовать через весь путь
                                  # выгрузки продаж
                                  notNoneHttpHostCS,
                                  notNoneHttpPortCS)
    return prepareAndSendThroughQpid(broker,
                                     cashId,  # идентификатор кассы, с которой производится запрос продаж, значение приходит с КС
                                     mode,  # режим работы, строчка "server" или "cash"
                                     dates,  # диапазон дат, за которые нужно запросить продажи, в формате
                                     # 'YYYY-MM-dd'T'HH_mm_ss.SSS@YYYY-MM-dd'T'HH_mm_ss.SSS', м.б. не указан
                                     shifts,  # диапазон смен, в формате 'shift@shift', м.б. не указан
                                     closedShifts,
                                     saleStateId,  # идентификатор статуса выгрузки продаж, которые присылает КС при формировании сигнала на выгрузку
                                     # продаж, если запрос формирует сама касса, то она предварительно делает запрос на сервер, для получения этого значения
                                     unloadingShopId,
                                     unloadingCashCode,
                                     notNoneTaskId,  # идентификтор задачи на выгрузку, генерируется на стороне sync-core, должен проследовать через
                                     # весь путь выгрузки продаж
                                     notNoneHttpHostCS,
                                     notNoneHttpPortCS)


def prepareThroughGrpc(mode, dates, shifts, closedShifts, saleStateId, unloadingShopId,
                       unloadingCashCode, cashId, shopId, cashCode, shopCode, notNoneTaskId,
                       httpHostCS, httpPortCS):
    """
    Подготовить архив с продажами, для отправки их по grpc. Будет использоваться функционал defferSale - сформированные продажи ВСЕГДА будут помещаться в эту
    директорию
    ВНИМАНИЕ: в этом методе cashId и shopId - это ИДЕНТИФИКАТОРЫ кассы и магазина (те, которые генерируются на КС), а cashCode и shopCode - это коды кассы и
    магазина (которые задаются в реестре кассы, и в вебе КС их разрешено править)
    :param mode   где запущен этот инстанс sync-core - server или cash
    :param dates   диапазон дат, за которые нужно запросить продажи, в формате
                  'YYYY-MM-dd'T'HH_mm_ss.SSS@YYYY-MM-dd'T'HH_mm_ss.SSS', м.б. не указан
    :param shifts   диапазон самен, в формате 'shift@shift', м.б. не указан
    :param closedShifts
    :param saleStateId  идентификатор статуса выгрузки продаж, которые присылает КС при формировании сигнала на выгрузку продаж,
                        если запрос формирует сама касса, то ... х.з. что здесь
    :param unloadingShopId
    :param unloadingCashCode
    :param cashId    идентификатор кассы, с которой производится запрос продаж, значение приходит с КС
    :param shopId    идентификатор магазина, в котором находится касса, с которой производится запрос продаж, значение приходит с КС
    :param cashCode    непосредственно код кассы, должен совпадать с кодом из реестра кассы
    :param shopCode   непосредственно код магазина, в котором находится касса
    :param notNoneTaskId    идентификтор задачи на выгрузку, генерируется на стороне sync-core, должен проследовать через весь путь
                            выгрузки продаж
    :param httpHostCS   хост с rest-интерфейсом КС, на который необходимо оправлять статусы
    :param httpPortCS   порт с rest-интерфейсом КС, на который необходимо оправлять статусы
    """
    try:
        # выполняем скрипт, соответствующий типу сообщения
        loggerSync.info('Вызываем плагин %s для выгрузки продаж c taksId: %s', format.encode('utf8'), notNoneTaskId.encode('utf8'))
        status.updateSaleState(saleStateId,
                               'Call plugin unloading sales',
                               'Вызываем плагин выгрузки продаж',
                               status.STATUS_DURING,
                               httpHostCS,
                               httpPortCS)
        plugin = __import__('%s.%s' % (pluginPath, format), fromlist='q')
        pathSales = os.path.normpath(defaultPathSales)
        if not os.path.exists(os.path.dirname(pathSales)):
            os.makedirs(os.path.dirname(pathSales))
        rez, messEn, messRu = plugin.unload(pathSales, mode, dates=dates, shifts=shifts, closedShifts=closedShifts,
                                            saleStateId=saleStateId, unloadingShopId=unloadingShopId, unloadingCashCode=unloadingCashCode)
        if rez == status.OK:
            for root, dirs, files in os.walk(os.path.dirname(pathSales)):
                if files:
                    backupDir = backupFile.prepareDirectory('sales')
                    for file in files:
                        saleFile = os.path.join(root, file)
                        archiveFile = backupFile.getArchiveName(saleFile, backupDir)
                        # архивируем выгруженные продажи
                        zipfile.ZipFile(archiveFile, 'w', zipfile.ZIP_DEFLATED).write(saleFile)
                        prop = {'cashCode': cashId,
                                # значение уходит в qpid, а там читается на стороне КС, и в нем ожидается именно ID кассы, поэтому такая путаница
                                # с названием кода. Хотя, конечно, причем здесь qpid, когда сообщение должно слаться через grpc? - отложенные продажи
                                # могут внезапно пойти через qpid
                                'shopId': shopId,
                                'shopCode': shopCode,
                                'realCashCode': cashCode,  # тот cashCode, который везде как cashCode, а здесь поле с таким именем уже занято.
                                'compressMethod': 'zip',
                                'cashUnloadState': saleStateId,
                                'messageType': 'sale',
                                'httpHostCS': httpHostCS,
                                'httpPortCS': httpPortCS,
                                'salesId': notNoneTaskId}
                        if dates:
                            CSReguestBegin = dates.split('@')[0]
                            CSReguestEnd = dates.split('@')[1]
                            prop.update({'saleType': 'dates', 'CSReguestBegin': CSReguestBegin, 'CSReguestEnd': CSReguestEnd})
                        elif shifts:
                            CSReguestBeginShift = shifts.split('@')[0]
                            CSReguestEndShift = shifts.split('@')[1]
                            prop.update({'saleType': 'shifts', 'CSReguestBeginShift': CSReguestBeginShift, 'CSReguestEndShift': CSReguestEndShift})
                        loggerSync.info('Помещаю продажи в директорию для последующей отправки. taskId: %s', notNoneTaskId.encode('utf8'))
                        # на самом деле тут должен быть статус STATUS_DURING (выгрузка в процессе, но такого статуса на КС внезапно нет)
                        rez = status.start
                        messRu = 'Помещаю продажи в директорию для последующей отправки.'
                        messEn = 'Send sales into deffer directory'
                        backupFile.defferSale(saleFile, propertyReader.unloadedSalesDirName, prop)
                        backupFile.remove(saleFile)
                        backupFile.remove(archiveFile)
                else:
                    messEn = 'Not found sales for the requested period.'
                    messRu = "Не найдены продажи за запрошенный период."
    except Exception as e:
        messEn = "Exception while send sales through grpc: %s." % str(e)
        messRu = "Исключение во время подготовки файла с продажами: %s" % str(e)
        rez = status.errorProgramm
        loggerSync.error(messEn)
        loggerSync.error(traceback.format_exc())
    return rez, messEn, messRu


def prepareAndSendThroughQpid(broker, cashId, mode, dates, shifts, closedShifts, saleStateId, unloadingShopId, unloadingCashCode,
                              notNoneTaskId, httpHostCS, httpPortCS):
    rez = status.errorUnload  # результат. если запись в очередь пройдет нормально, то вернется 0
    messEn = messRu = ''
    connection = None
    sender = None
    try:
        loggerSync.info('Создаем соединение.')
        connection = Connection(broker)
        loggerSync.info('Открываем соединение.')
        connection.open()
    except Exception, e:
        messRu = 'Ошибка подключения к локальному сервису qpid. %s' % str(e)
        loggerSync.error(messRu)
        return status.errorProgramm, 'Failed to connect to the local service qpid. %s' % str(e), messRu
    try:
        loggerSync.info('Создаем сессию.')
        session = connection.session()
        loggerSync.info('Создаем отправителя сообщений.')
        queue = 'artix.backward.mcs' if mode == 'server' else 'artix.backward.cs'
        sender = session.sender('%s; {create: always, node:{ type: queue, durable: True}}' % queue, durable=True)
    except Exception, e:
        rez = status.errorProgramm
        messEn, messRu = str(e)
        loggerSync.error(messRu)
        loggerSync.error(traceback.format_exc())
        return status.errorProgramm, messEn, messRu

    # Вызываем скрипт выгрузки продаж
    try:
        try:
            plugin = __import__('%s.%s' % (pluginPath, format), fromlist='q')
            # выполняем скрипт, соответствующий типу сообщения
            loggerSync.info('Вызываем плагин %s для выгрузки продаж' % format.encode('utf8'))
            status.updateSaleState(saleStateId,
                                   'Call plugin unloading sales',
                                   'Вызываем плагин выгрузки продаж',
                                   status.STATUS_DURING,
                                   httpHostCS if httpHostCS else propertyReader.httpHostCS,
                                   httpPortCS if httpPortCS else propertyReader.httpPortCS)
            pathSales = os.path.normpath(defaultPathSales)
            if not os.path.exists(os.path.dirname(pathSales)):
                os.makedirs(os.path.dirname(pathSales))
            rez, messEn, messRu = plugin.unload(pathSales, mode, dates=dates, shifts=shifts, closedShifts=closedShifts,
                                                saleStateId=saleStateId, unloadingShopId=unloadingShopId, unloadingCashCode=unloadingCashCode)
            if rez == status.OK:
                for root, dirs, files in os.walk(os.path.dirname(pathSales)):
                    if files:
                        for file in files:
                            backupDir = backupFile.prepareDirectory('sales')
                            saleFile = os.path.join(root, file)
                            archiveFile = backupFile.getArchiveName(saleFile, backupDir)
                            # архивируем выгруженые продажи
                            loggerSync.info('Архивируем выгруженые продажи.')
                            status.updateSaleState(saleStateId,
                                                   'Archiving sales',
                                                   'Архивируем выгруженые продажи',
                                                   status.STATUS_DURING,
                                                   httpHostCS if httpHostCS else propertyReader.httpHostCS,
                                                   httpPortCS if httpPortCS else propertyReader.httpPortCS)
                            zipfile.ZipFile(archiveFile, 'w', zipfile.ZIP_DEFLATED).write(saleFile)
                            f = open(archiveFile, 'rb')
                            if mode == 'server':
                                m = re.match(".*sales(.+)_.*", file)
                                if m and m.group(1) != cashIdMacro:
                                    cashId = m.group(1)
                            prop = {'cashCode': cashId, 'compressMethod': 'zip', 'cashUnloadState': saleStateId, 'messageType': 'sale',
                                    'salesId': notNoneTaskId}
                            if dates:
                                CSReguestBegin = dates.split('@')[0]
                                CSReguestEnd = dates.split('@')[1]
                                prop.update({'saleType': 'dates', 'CSReguestBegin': CSReguestBegin, 'CSReguestEnd': CSReguestEnd})
                            elif shifts:
                                CSReguestBeginShift = shifts.split('@')[0]
                                CSReguestEndShift = shifts.split('@')[1]
                                prop.update({'saleType': 'shifts', 'CSReguestBeginShift': CSReguestBeginShift, 'CSReguestEndShift': CSReguestEndShift})
                            loggerSync.info('Отправляем сообщение с продажами в очередь.')
                            status.updateSaleState(saleStateId,
                                                   'Sending sales to queue',
                                                   'Отправляем сообщение с продажами в очередь.',
                                                   status.STATUS_DURING,
                                                   httpHostCS if httpHostCS else propertyReader.httpHostCS,
                                                   httpPortCS if httpPortCS else propertyReader.httpPortCS)
                            try:
                                sender.send(Message(content=f.read(), id=hex(hash(datetime.now())), properties=prop, durable=True))
                                backupFile.move(archiveFile, backupDir)
                                messEn = 'Message with sales sent'
                                messRu = 'Сообщение с продажами отправлено'
                            except Exception, e:
                                backupFile.defferSale(saleFile, propertyReader.unloadedSalesDirName, prop)
                                backupFile.remove(archiveFile)
                                messEn = 'Couldn\'t insert file %s into Qpid queue: %s' % (archiveFile, str(e))
                                messRu = 'Не удалось добавить файл %s в очередь: %s' % (archiveFile, str(e))
                                loggerSync.error(messRu)
                                loggerSync.error(traceback.format_exc(30))
                                # если попали сюда, значит sender умер
                                try:
                                    connection.close()
                                except Exception, e:
                                    # qpidd 0.16 убивает connection, а qpidd 0.34 нет
                                    pass
                                loggerSync.info(status.errorQpidInsert)
                                loggerSync.info('-----------------------')
                                return status.errorQpidInsert, messEn, messRu
                            finally:
                                f.close()
                                #подрезание бэкапов продаж
                                backupFile.backupPruning(backupDir)
                                backupFile.remove(saleFile)
                    else:
                        messEn = 'Not found sales for the requested period.'
                        messRu = "Не найдены продажи за запрошенный период."
            else:
                sender.close()
                connection.close()
                return rez, messEn, messRu
        except Exception, e:
            messEn = 'Error while unloading sales. %s' % str(e)
            messRu = "Ошибка при выгрузке. %s" % str(e)
            loggerSync.error(messRu)
            loggerSync.error(traceback.format_exc())
    except ImportError, e:
        messEn = 'Unknown format unloading.'
        messRu = "Указан неизвестный формат выгрузки." + str(e)
        loggerSync.error(messRu)
    sender.close()
    connection.close()
    loggerSync.info(rez)
    loggerSync.info('-----------------------')
    return rez, messEn, messRu

def sendUnloadRequestToCS(host, port, cashId):
    '''
    Отправка запроса на КС для выгрузки продаж
    '''
    loggerSync.info('Отправка запроса на КС для выгрузки продаж')
    host = host if host else propertyReader.httpHostCS
    port = port if port else propertyReader.httpPortCS
    try:
        url = 'http://%s:%s/CSrest/rest/sales/unload/cashid/%s/kick' % (host, port, cashId)
        loggerSync.info('URL: %s, method: PUT' % url)
        response, content = httplib2.Http().request(url, 'PUT', headers={'Content-Type':'application/json', 'Accept': 'application/json'})
        if response.status == 200:
            loggerSync.info('Запрос успешно отправлен')
    except Exception, e:
        loggerSync.error('Ошибка при запросе выгрузки продаж через рест на КС: %s', str(e).encode('utf8'))

def unloadedSales(broker, mode, task):
    loggerSync.info('Выгрузка невыгруженных продаж.')
    connection = None
    sender = None
    try:
        loggerSync.info('Создаем соединение.')
        connection = Connection(broker)
        connection.tcp_nodelay = True
        loggerSync.info('Открываем соединение.')
        connection.open()
    except Exception, e:
        loggerSync.error('Ошибка подключения к локальному сервису qpid. %s' % str(e))
        return status.errorProgramm

    try:
        loggerSync.info('Создаем сессию.')
        session = connection.session()
        loggerSync.info('Создаем отправителя сообщений.')
        queue = 'artix.backward.mcs' if mode == 'server' else 'artix.backward.cs'
        sender = session.sender('%s; {create: always, node:{ type: queue, durable: True}}' % queue, durable=True)
    except Exception, e:
        loggerSync.error(str(e))
        return status.errorProgramm

    pathSales = os.path.normpath('%s/sales_for_cs' % tempfile.gettempdir())

    if not os.path.exists(pathSales):
        os.makedirs(pathSales)

    for root, dirs, files in os.walk(backupFile.getUnloadedSalesPath()):
        if files:
            for file in files:
                archiveFileWithProps = os.path.join(root, file)
                # 1. Достать из архива проперти и получить архив без пропертей(нужно работать в темповой директории)
                archiveFile, props = backupFile.getUnloadedSale(archiveFileWithProps, pathSales)
                if props.get('cashUnloadState') != task.get('saleStateId'):
                    backupFile.remove(archiveFile)
                    continue
                # 2. Пихнуть архив без пропертей в очередь
                loggerSync.info('Отправляем сообщение с продажами в очередь.')
                f = open(archiveFile, 'rb')
                try:
                    sender.send(Message(content=f.read(), id=hex(hash(datetime.now())), properties=props, durable=True))
                except Exception, e:
                    loggerSync.error('Выгрузка невыгруженных продаж завершилась с ошибкой: %s.' % str(e))
                    loggerSync.error(traceback.format_exc(30))
                    try:
                        connection.close()
                    except Exception, e:
                        # qpidd 0.16 убивает connection, а qpidd 0.34 нет
                        pass
                    backupFile.remove(archiveFile)
                    return status.errorQpidInsert
                finally:
                    f.close()
                backupFile.move(archiveFile, backupFile.prepareDirectory('sales'))
                backupFile.remove(archiveFileWithProps)
                status.updateSaleState(props.get('cashUnloadState'),
                                       'Unloaded sales',
                                       'Невыгруженные продажи',
                                       status.STATUS_READY,
                                       task.get('httpHostCS', propertyReader.httpHostCS),
                                       task.get('httpPortCS', propertyReader.httpPortCS))
    connection.close()
    loggerSync.info('Выгрузка невыгруженных продаж окончена.')
    return status.OK


def sendSalesRequest(options):
    if propertyReader.unloadSalesRequestThroughCS and propertyReader.mode == "cash":
        sendUnloadRequestToCS(propertyReader.httpHostCS, propertyReader.httpPortCS, propertyReader.cashCode)
        return
    messEn = 'Start unload'
    messRu = 'Начинаем выгрузку продаж'
    saleStateId = None
    try:
        stateParams = {"callFromCS": False}
        unloadParams = {"unloadingShopId": options.shopCode, "unloadingCashCode": options.cashCode}
        if options.dates:
            stateParams["fromDate"] = options.dates.split('@')[0]
            stateParams["toDate"] = options.dates.split('@')[1]
            unloadParams["dates"] = options.dates
            unloadParams["closedShifts"] = options.closedShifts
        elif options.shifts:
            stateParams["fromShiftNum"] = options.shifts.split('@')[0]
            stateParams["toShiftNum"] = options.shifts.split('@')[1]
            unloadParams["shifts"] = options.shifts
            unloadParams["closedShifts"] = options.closedShifts
        else:
            print "Время выгрузки продаж или диапазон смен не указаны. \nВызываем выгрузку продаж за последние 2 часа"
            stateParams["fromDate"] = CSReguestBegin
            stateParams["toDate"] = CSReguestEnd
            unloadParams["dates"] = '%s@%s' % (CSReguestBegin, CSReguestEnd)

        saleStateId = status.addSaleState(messEn, messRu, status.STATUS_DURING, propertyReader.httpHostCS, propertyReader.httpPortCS, **stateParams)
        unloadParams["saleStateId"] = saleStateId
        rez, messEn, messRu = main(propertyReader.localBroker, propertyReader.cashCode, propertyReader.storeID, propertyReader.mode, **unloadParams)
        saleStatus = status.STATUS_ERROR
        if rez == status.OK:
            saleStatus = status.STATUS_READY
        elif rez == status.start:
            saleStatus = status.STATUS_DURING
        status.updateSaleState(saleStateId, messEn, messRu, saleStatus, propertyReader.httpHostCS, propertyReader.httpPortCS)
    except Exception, e:
        status.updateSaleState(saleStateId, str(e), str(e), status.STATUS_ERROR, propertyReader.httpHostCS, propertyReader.httpPortCS)


if __name__ == '__main__':
    import propertyReader

    parser = optparse.OptionParser()
    parser.add_option("-s", "--shifts", dest="shifts", help="@-separated shift range")
    parser.add_option("-d", "--dates", dest="dates", help="@-separated dates range. Date format: YY-mm-ddTHH:MM:SS.0")
    parser.add_option("-c", "--closedShifts", dest="closedShifts", default=0, type='int',
                      help="Unload only closed, opened or closed and opened shifts. Use: 0-closed, 1-opened, 2-closedAndOpened  [default: %default]")
    parser.add_option("-K", "--cashCode", dest="cashCode", help="Unload for this cash (Parameter only for server mode)")
    parser.add_option("-S", "--shopCode", dest="shopCode", help="Unload for this shop (Parameter only for server mode)")
    (options, args) = parser.parse_args(sys.argv[1:])
    sendSalesRequest(options)
