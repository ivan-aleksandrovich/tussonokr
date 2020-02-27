# -*- coding: UTF-8 -*-

import os, shutil
import logging
import tempfile
import subprocess
log = logging.getLogger('su.artix.loggerSync')

#
# Скачка файла по ссылке
#

tmpPath = tempfile.gettempdir()

wgetErrorMessages = {0: 'No problems occurred',\
                     1: 'Generic error code',\
                     2: 'Parse error---for instance, when parsing command-line options, the .wgetrc or .netrc...',\
                     3: 'File I/O error',\
                     4: 'Network failure',\
                     5: 'SSL verification failure',\
                     6: 'Username/password authentication failure',\
                     7: 'Protocol errors',\
                     8: 'Server issued an error response'\
                    }

def getFileName(s):
    return s.split('/')[len(s.split('/'))-1]


def main(url, path, fileName):
    if fileName == '':
        fileName = getFileName(url)
  
    # скачиваем файл, если ссылка не доступна в течение минуты переходим к следующему сообщению
    log.info('Начинаем попытку скачивания файла')

    process = subprocess.Popen(["wget", "-O%s" % os.path.join(tmpPath, fileName),\
            "-c", "-t 5", "-w 12", "-T 60", "--progress=dot:mega", url],\
            stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    (stdin, stdout) = process.communicate()

    if process.returncode != 0:
        errorMessage = wgetErrorMessages.get(process.returncode, 'Неизвестная ошибка')
        log.error("wget вернул ошибку: \nКод ошибки: \"%s\"\nСообщение: \"%s\"" % (process.returncode, errorMessage))
        return False, errorMessage
        
    if path != '' and path != None:
        if os.access(path, os.F_OK): # проверка существует ли такая директория
            pass
        else:
            log.info('Создаем директорию ' + path.encode('utf8'))
            os.makedirs(path) # создаем директорию
        # копируем файл из временной директории в заданную
        log.info('Перемещаем полученный файл из временной директории в заданную - %s' % path.encode('utf8'))
        if tmpPath != path:
            f = os.path.normpath(tmpPath + '/' + fileName)
            try:
                shutil.move(f, path)
                return True, 'Файл скачан'
            except Exception, e:
                mess = 'Не удалось переместить полученный файл. %s' % str(e)
                log.error(mess)
                return False, mess
        else:
            return True, 'Файл скачан'
    else:
        mess = 'Не указан путь сохранения файла.'
        log.error(mess)
        return False, mess
