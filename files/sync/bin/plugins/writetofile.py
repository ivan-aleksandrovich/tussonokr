# -*- coding: UTF-8 -*-

import os
import logging
log = logging.getLogger('su.artix.loggerSync')

# получение тела сообщения и запись в файл
def main(body, path, fileName, openMode = 'wb'):
    if os.access(path, os.F_OK):
        pass
    else:
        os.makedirs(path)
    log.info('Создаем файл %s/%s и записываем в него полученное тело сообщения' % (path.encode('utf8'), fileName.encode('utf8')))
    filePath = os.path.normpath('%s/%s' % (path, fileName))
    output = open(filePath, openMode)
    os.chmod(filePath, 0777)
    output.write(body)
    output.close()
