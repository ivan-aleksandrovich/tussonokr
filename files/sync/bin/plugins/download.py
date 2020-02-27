# -*- coding: UTF-8 -*-

import os
import downloadfile, writetofile
import logging
import status
import tempfile
log = logging.getLogger('su.artix.loggerSync')

#
# Скачивание файла по ссылке полученной из сообщения
#

messageLocalPathKey = 'localPath'
tmpPath = os.path.normpath('%s/file.tmp' % tempfile.gettempdir())


def getLocalPath(message):
    # Получаем локальный путь
    path = ''
    log.info('Получаем локальный путь, по которому будет сохранен скачанный файл')
    path = str(message.properties.get(messageLocalPathKey))
    if path == '' or path == None:
        log.warning('Не указан путь, по которому необходимо сохранить скачанный файл. Файл сохранится во временную директорию %s' % path)
        path = tmpPath
    log.info('Путь для сохранения файла: %s' % path)
    path = os.path.normpath(path)
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
    vers = ''
    num = 1
    while os.path.exists(path+vers):
        vers = '_' + str(num)
        num+=1
    path = path+vers
    return path

def main(message):
    fileLocation = message.properties.get('fileLocation')

    log.info('Получаем тело сообщения')
    body = message.content

    localPath = getLocalPath(message)
    fileName = os.path.basename(localPath)
    fileDir = os.path.dirname(localPath)

    if (fileLocation == 'url'):
        url = body
        if url == None:
            mess = 'Ссылка на скачиваемый файл не указана.'
            log.warning(mess)
            return status.errorDownload, mess
        else:
            res, mess = downloadfile.main(url, fileDir, fileName)
            if res:
                res = status.OK
            else:
                res = status.errorDownload
            return res, mess
    elif (fileLocation == 'body'):
        if not body:
            mess = 'Нет файла в теле сообщения'
            log.warning(mess)
            return status.errorDownload, mess
        else:
            writetofile.main(body, fileDir, fileName)
            return status.OK, 'Файл скачан'
    else:
        mess = 'Не указано расположение файла. Свойство fileLocation должно иметь значение url или body.'
        log.error(mess)
        return status.errorDownload, mess
