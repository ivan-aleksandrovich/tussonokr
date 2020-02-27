# -*- coding: UTF-8 -*-

import os
import subprocess
import writetofile
import logging
import status
import tempfile
import backupFile
log = logging.getLogger('su.artix.loggerSync')

#
# Выполнение полученного из сообщения скрипта
#

tmpPath = tempfile.gettempdir()     # куда сохранить скрипт
fileName = 'script'  # как будет называться скрипт
if os.name == 'nt':
    fileName = 'script.bat'
elif os.name == 'posix':
    fileName = 'script.sh'

def main(message):
    # Получаем тело сообщения
    log.info('Получаем тело сообщения')
    script = '%s'
    if os.name == 'posix':
        script = '#!/bin/bash \n\n%s'
    script = script % message.content
    if message.content == None:
        mess = 'Тело скрипта не указано в теле сообщения.'
        log.warning(mess)
        return status.errorExecScript, mess
    else:
        writetofile.main(script, tmpPath, fileName)
        path = os.path.normpath(tmpPath+'/'+fileName)
        log.info('Запускаем полученный скрипт - %s' % path)
        result = 0
        if os.name == 'nt':
            result = os.spawnv(os.P_WAIT, path, [path])
        elif os.name == 'posix':
            result = os.spawnlp(os.P_WAIT, path, "")
        backupFile.archive(path, 'scripts')
        if result == 0:
            return status.OK, 'Команда выполнена.'
        else:
            mess = 'Не удалось выполнить скрипт'
            log.error(mess)
            return status.errorExecScript, mess

def execute(script):
    scriptName = 'script_from_cs.sh'
    writetofile.main(script, tmpPath, scriptName)
    path = os.path.normpath(tmpPath+'/'+scriptName)
    log.info('Запускаем полученный скрипт - %s' % path)
    p = subprocess.Popen([path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    backupFile.archive(path, 'scripts')
    result = {'err': err, 'out': out}
    log.info('Результат выполнения скрипта: %s' % result)
    return result
