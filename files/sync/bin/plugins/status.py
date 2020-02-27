# -*- coding: UTF-8 -*-

OK = 0                  # успешно обработаны сообщения
errorProgramm = 1       # ошибка программы
errorLoadDict = 2       # ошибка загрузки справочников
errorExecScript = 3     # ошибка выполнения комманды
errorLicense = 4        # ошибка применения лицензии
errorDownload = 5       # ошибка скачивания файла
errorQpidInsert = 6     # ошибка добавления данных в очередь Qpid
errorUnload = 10        # ошибка выгрузки продаж
start = 20              # выполнен запуск сервиса
busy = 30               # сервис занят в текущий момент
skipped = 40            # задача не подлежит обработке
empty = 50              # отложенных справочников не найдено (директория пуста)

import propertyReader
import urllib2
import urllib
import traceback

def sendStatus(log, method, status, message, host, port):
    proxy_support = urllib2.ProxyHandler({})
    opener = urllib2.build_opener(proxy_support)
    urllib2.install_opener(opener)

    for a in ('/', '\\'):
        message = message.replace(a, '_')
    message = urllib.quote(message, '')
    urlPart = 'statusdict'
    if method == 'unload':
        urlPart = 'statussale'
    try:
        request = 'http://%s:%s/cashstatus/rest/%s/%s/%s/%s' % (host, port, urlPart, propertyReader.cashCode, status, message)
        log.info("Request: %s", request)
        response = urllib2.urlopen(request)
    except Exception, e:
        log.warning('Ошибка отправки статуса на КС. %s' % str(e))

import httplib2
import json
http = httplib2.Http()
jsonHeader = {'Content-Type':'application/json', "Accept": "application/json"}

STATUS_ERROR = 'STATUS_ERROR'                       # Ошибка загрузки/выгрузки справочника/продаж
STATUS_READY = 'STATUS_READY'                       # Справочник/продажи успешно загружен/выгружен
STATUS_READY_WITH_ERROR = 'STATUS_READY_WITH_ERROR' # Справочник загружен с ошибками
STATUS_DURING = 'STATUS_DURING'                     # Справочник/продажи в данный момент загружается/выгружается
STATUS_ALREADY_LOAD = 'STATUS_ALREADY_LOAD'         # Продажи уже были загружены

import logging
log = logging.getLogger('su.artix.loggerSync')

def sendState(url, body, method, errMess = 'Ошибка отправки статуса.'):
    try:
        log.debug('URL: %s, method: %s' % (url, method))
        log.debug('Body: %s' % body)
        response, content = http.request(url, method, headers=jsonHeader, body=body)
        if response.status == 200:
            return json.loads(content)
    except Exception, e:
        log.warning(errMess)
        log.warning(e.message)
        log.warning(traceback.format_exc())
    return None

def sendDictStateForCS(shopId, messageEn, messageRu, status, dictStateId=None):
    body = {'messageEn': messageEn, 'messageRu': messageRu, 'status': status}
    url = 'http://localhost:8080/CSrest/rest/dicts/backOfficeState/%s' % shopId
    if dictStateId:
        log.debug('UPDATE dictState for CS')
        method = 'PUT'
        url += '/%s' % dictStateId
    else:
        method = 'POST'
        body['format'] = 'ARTIXINTERCHANGE'
        body['fileName'] = None
    body = json.dumps(body)
    return sendState(url, body, method, errMess = 'Ошибка отправки статуса загрузки справочника.')

def sendDictState(id, messageEn, messageRu, status, host, port, version=None):
    if id:
        if version >= '3':
            id += '?useDictGenerateId=true'
        url = 'http://%s:%s/CSrest/rest/dicts/cashLoad/%s/%s/%s' % (host, port, propertyReader.storeID, propertyReader.cashCode, id)
        dictCashLoadState = json.dumps({'messageEn': messageEn, 'messageRu': messageRu, 'status': status})
        return sendState(url, dictCashLoadState, 'PUT', errMess = 'Ошибка отправки статуса загрузки справочника.')
    return None

def addSaleState(messageEn, messageRu, status, host, port, fromDate=None, toDate=None, fromShiftNum=None, toShiftNum=None, callFromCS=True):
    url = 'http://%s:%s/CSrest/rest/sales/cashUnloadState/%s/%s' % (host, port, propertyReader.storeID, propertyReader.cashCode)
    saleCashUnloadState = json.dumps({'messageEn': messageEn, 'messageRu': messageRu, 'status': status, 'callFromCS': callFromCS,
                                      'fromDate': fromDate, 'toDate': toDate, 'fromShiftNum': fromShiftNum, 'toShiftNum': toShiftNum})
    return sendState(url, saleCashUnloadState, 'POST', errMess = 'Ошибка отправки статуса выгрузки продаж.')

def updateSaleState(id, messageEn, messageRu, status, host, port):
    if id:
        url = 'http://%s:%s/CSrest/rest/sales/cashUnloadState/%s' % (host, port, id)
        saleCashUnloadState = json.dumps({'messageEn': messageEn, 'messageRu': messageRu, 'status': status})
        return sendState(url, saleCashUnloadState, 'PUT', errMess='Ошибка отправки статуса выгрузки продаж.')
    return None

def rereadQueuesInNES():
    try:
        response, content = http.request('http://localhost:28081/nes/queue_reread', 'PUT', headers=jsonHeader)
        if response.status == 200:
            log.info(u'Oтправлен сигнал в NES, чтобы он перечитал очереди.')
        else:
            log.warning(u'Ошибка отправки сигнала в NES, чтобы он перечитал очереди. Он перечитает их позже.')
    except Exception, e:
        log.warning(u'Ошибка отправки сигнала в NES, чтобы он перечитал очереди. Он перечитает их позже.')
