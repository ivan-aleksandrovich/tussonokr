# -*- coding: UTF-8 -*-

import os
import logging
import status
import tempfile
import backupFile
import bz2
import traceback
import propertyReader

log = logging.getLogger('su.artix.loggerSync')
#
# Получение справочников из очереди
#

path = tempfile.gettempdir()
fileName = 'pos1'

# ключи заголовка сообщения
dictLocationKey = 'dictLocation'
formatKey = 'format'

def loadDict(Format, dictPath, dictProperties):
    dictPath = os.path.normpath(dictPath) if dictPath else None
    try:
        exec 'import ' + Format.lower() + ' as module'
    except:
        mes = 'Не найден модуль обработки справочника формата %s' % Format.encode('utf8')
        log.error(mes)
        log.error(traceback.format_exc())
        status.sendDictState(dictProperties['dictCashLoadStateId'], 'Not found module %s' % Format.encode('utf8'), mes, status.STATUS_ERROR,
                             propertyReader.httpHostCS, propertyReader.httpPortCS, dictProperties['version'])
        return status.errorProgramm, mes
    try:
        res, mes = module.upload(dictPath, dictProperties)
        if dictPath:
            backupFile.archive(dictPath, 'dicts')
        return res, mes
    except Exception, e:
        log.error(str(e))
        log.error(traceback.format_exc())
        status.sendDictState(dictProperties['dictCashLoadStateId'], str(e), 'Ошибка: %s' % str(e), status.STATUS_ERROR,
                             propertyReader.httpHostCS, propertyReader.httpPortCS, dictProperties['version'])
        return status.errorProgramm, str(e)

def str2bool(v):
    return str(v).lower() in ("yes", "true", "t", "1")

def main(message):
    fileName = 'pos.aif'
    messageId = message.id
    log.info('Определяем формат передаваемого справочника')
    format = message.properties.get(formatKey)
    version = message.properties.get('version')
    compressMethod = message.properties.get('compressMethod')
    dictMessageType = message.properties.get("dictMessageType", "FULL").upper()
    dictCashLoadStateId = message.properties.get('dictCashLoadStateId' + propertyReader.cashCode)
    if version >= '3':
        dictCashLoadStateId = message.properties.get('dictGenerateStateId')
    
        
    remoteLoadDictCommand = str2bool(message.properties.get('remoteLoadDictCommand', 'False'))
    nesGenerateCommandMode = message.properties.get('nesGenerateCommandMode', 'increment') # ['increment', 'full', 'none']
    nesGenerateCommandCashesList = message.properties.get('nesGenerateCommandCashesList', '')
    unloadElementName = message.properties.get('unloadElementName', None)
    nesGenerateCommandOwner = message.properties.get('nesGenerateCommandOwner', '')
    
    dictProperties = {'messageId': messageId, 'version': version, 'dictCashLoadStateId': dictCashLoadStateId, 'nesGenerateCommandMode': nesGenerateCommandMode, 
                      'nesGenerateCommandCashesList': nesGenerateCommandCashesList, 'unloadElementName': unloadElementName,
                      'remoteLoadDictCommand': remoteLoadDictCommand, 'nesGenerateCommandOwner': nesGenerateCommandOwner}
    
    status.sendDictState(dictCashLoadStateId, 'Get dict from queue', 'Получаем справочник из очереди', status.STATUS_DURING,
                         propertyReader.httpHostCS, propertyReader.httpPortCS, version)
    if format == None:
        mes = 'Формат справочника не указан.'
        log.warning(mes)
        return status.errorLoadDict, mes
    else:
        log.info('Формат справочника - %s.' % format.encode('utf8'))

        log.info('Определяем способ передачи справочника.')
        dictLocation = message.properties.get(dictLocationKey)
    
        if dictLocation == 'body':
            log.info('Передача справочника через тело сообщения. Получаем тело сообщения.')
            body = message.content
            if body or remoteLoadDictCommand:
                if body:
                    import writetofile
                    openModeFileWrite = 'ab' if dictMessageType in ['MIDDLE_PART', 'LAST_PART'] else 'wb'
                    writeFileName = os.path.splitext(fileName)[0]+'.bz2' if compressMethod == 'BZ2' else fileName
                    writetofile.main(body, path, writeFileName, openModeFileWrite)
                    
                    if dictMessageType in ['MIDDLE_PART', 'FIRST_PART']:
                        mes = 'Пойдем в очередь за следующей частью справочника'
                        log.info(mes)
                        return status.OK, mes
                    
                    if compressMethod == 'BZ2':
                        decompressFile(os.path.join(path, writeFileName), os.path.join(path,fileName))
                
                    # Загрузка справочника в указанном формате
                    res, mes = loadDict(format, os.path.join(path, fileName), dictProperties)
                else:
                    res, mes = loadDict(format, None, dictProperties)
                return res, mes
            else:
                mes = 'Cправочник не указан в теле сообщения.'
                log.warning(mes)
                return status.errorLoadDict, mes

        elif dictLocation == 'url':
            log.info('Передача через URL. Получаем URL справочника.')
            url = message.content
            if url or remoteLoadDictCommand:
                if url:
                    url = url.replace('{host}', str(propertyReader.dictUrlHostCS)).replace('{port}', str(propertyReader.dictUrlPortCS))
                    log.info('URL - %s' % url)
                    import downloadfile
                    fileName = url.split('/')[len(url.split('/'))-1]
                    res, mes = downloadfile.main(url, path, fileName)
                    if res and compressMethod == 'BZ2':
                        newFileName = fileName.replace('.bz2', '')
                        decompressFile(os.path.join(path, fileName), os.path.join(path, newFileName))
                        fileName = newFileName
    
                    # Загрузка справочника в указанном формате
                    if res:
                        res, mes = loadDict(format, os.path.join(path, fileName), dictProperties)
                    else:
                        res = status.errorLoadDict
                else:
                    res, mes = loadDict(format, None, dictProperties)
                return res, mes
            else:
                mes = 'Не указана ссылка на скачивание справочника.'
                log.warning(mes)
                return status.errorLoadDict, mes
        else:
            mes = 'Способ передачи справочника не определен.'
            log.warning(mes)
            return status.errorLoadDict, mes

def decompressFile(sourceFile, destFile):
    f = open(destFile, 'w')
    data = bz2.BZ2File(sourceFile, 'rb')
    chunk = data.read(1048576) # считываем 1 МБ
    while chunk:
        f.write(chunk)
        chunk = data.read(1048576) # считываем 1 Мб
    data.close()
    f.close()
    os.remove(sourceFile)

