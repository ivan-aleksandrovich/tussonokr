# -*- coding: UTF-8 -*-

import os
import writetofile
import logging
import status
import tempfile
log = logging.getLogger('su.artix.loggerSync')
#
# Получение лицензий из очереди
#

#fileDir = '/linuxcash/cash/sync/lic'
#fileName = 'license.li3c'
scriptDir = tempfile.gettempdir()
scriptName = 'licenseScript.sh'

# ключи заголовка сообщения
scriptKey = 'script'
localPathKey = 'localPath'

def main(message):
    log.info('Считываем путь для сохранения лицензии.')
    localPath = message.properties.get(localPathKey)
    if localPath == None:
        mess = 'Путь для сохранения лицензии не указан.'
        log.warning(mess)# Сохраняем в %s/%s' % (fileDir, fileName))
        return status.errorLicense, mess
    else:
        log.info('Путь для сохранения лицензии - %s.' % localPath.encode('utf8'))
        fileName = os.path.basename(localPath)
        fileDir = os.path.dirname(localPath)

        log.info('Считываем файл лицензии из тела сообщения.')
        license = message.content
        if license == None:
            mess = 'Файл лицензии не указан в теле сообщения.'
            log.warning(mess)
            return status.errorLicense, mess
        else:
            writetofile.main(license, fileDir, fileName)

            log.info('Считываем скрипт, который нужно выполнить после сохранения файла лицензии.')
            script = message.properties.get(scriptKey)
            if script == None:
                mess = 'Скрипт применения лицензии не указан.'
                log.warning(mess)
                return status.errorLicense, mess
            else:
                writetofile.main(script, scriptDir, scriptName)
                log.info('Запускаем сохраненный скрипт для применения лицензии.')
                result = os.spawnl(os.P_WAIT, os.path.normpath(scriptDir+'/'+scriptName), "")
                if result == 0:
                    os.remove(os.path.normpath(scriptDir+'/'+scriptName))
                    return status.OK, 'Файл лицензии применен.'
                else:
                    mess = 'Ошибка выполнения скрипта для применения лицензии.'
                    log.error(mess)
                    return status.errorLicense, mess
