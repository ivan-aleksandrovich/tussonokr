# -*- coding: UTF-8 -*-

import ConfigParser
import logging
import os
import sys

__import__('logger')
loggerSync = logging.getLogger('su.artix.loggerSync')
pathProperties = os.path.normpath('%s/../properties/cash.ini' % sys.path[0])


def getValue(option, defaultValue, config):
    '''
    Получить значение опции из конфигурационного файла.
    В случае ошибки вернуть значение по умолчанию
    '''
    value = None
    try:
        if type(defaultValue) is int:
            value = config.getint('Properties', option)
        elif type(defaultValue) is bool:
            value = config.getboolean('Properties', option)
        else:
            value = config.get('Properties', option)
    except ConfigParser.NoOptionError:
        loggerSync.warning(u'Не найдена настройка %s. Берем значение по умолчанию: %s' % (option, defaultValue))
        value = defaultValue

    loggerSync.info(u'%s - %s' % (option.ljust(40, ' '), str(value).ljust(20, ' ')))
    return value


def getRequiredValue(option, config):
    '''
    Получить обязательное значение из конф-го файла
    В случае, если нет секции или опции, генерируется Exception
    '''
    value = config.get('Properties', option)
    loggerSync.info(u'%s - %s' % (option.ljust(40, ' '), str(value).ljust(20, ' ')))
    return value


def rereadConfig():
    propertyReader = sys.modules[__name__]
    # Считывание настроек подключения sync-агента
    if os.access(pathProperties, os.F_OK):
        config = ConfigParser.RawConfigParser()
        setattr(propertyReader, "config", config)
        config.read(pathProperties)
        setattr(propertyReader, "localBroker", getValue('localBroker', 'guest/guest@127.0.0.1:5672', config))
        remotePort = getValue('dept.broker.port', 5672, config)
        setattr(propertyReader, "remotePort", remotePort)
        setattr(propertyReader, "maxCountBackupFiles", getValue('maxCountBackupFiles', 0, config))
        setattr(propertyReader, "actualCountDaysForBackupFiles", getValue('actualCountDaysForBackupFiles', 0, config))
        cashServerRestPort = getValue('cashServerRestPort', 8080, config)
        setattr(propertyReader, "cashServerRestPort", cashServerRestPort)
        setattr(propertyReader, "queueFileSize", getValue('queueFileSize', 128, config))
        setattr(propertyReader, "queueFileCount", getValue('queueFileCount', 16, config))
        setattr(propertyReader, "queueMaxSize", getValue('queueMaxSize', 104857600, config))
        setattr(propertyReader, "queueMaxCount", getValue('queueMaxCount', 3000, config))
        setattr(propertyReader, "mode", getValue('mode', 'cash', config))
        setattr(propertyReader, "uploadScript", getValue('uploadScript', '/linuxcash/cash/exchangesystems/exchangers/src/upload/converter/aif.py', config))
        setattr(propertyReader, "useQueueInUpload", getValue('useQueueInUpload', True, config))
        setattr(propertyReader, "brokerConnectionHeartbeat", getValue('brokerConnectionHeartbeat', 0, config))
        setattr(propertyReader, "dictUrlHostCS", getValue('dictUrlHostCS', '127.0.0.1', config))
        setattr(propertyReader, "dictUrlPortCS", getValue('dictUrlPortCS', 80, config))
        setattr(propertyReader, "clusterId", getValue('clusterId', '', config))
        setattr(propertyReader, "grpcHostCS", getValue('grpcHostCS', '127.0.0.1', config))
        setattr(propertyReader, "grpcPortCS", getValue('grpcPortCS', 10001, config))
        setattr(propertyReader, "protocolVersion", getValue('protocolVersion', 2, config))
        setattr(propertyReader, "unloadedSalesDirName", getValue('unloadedSalesDirName', 'unloadedSales', config))
        setattr(propertyReader, "unloadSalesRequestThroughCS", getValue('unloadSalesRequestThroughCS', False, config))
        try:
            # Значения, которые обязательно должны присутствовать в конф-ом файле
            remoteIp = getRequiredValue('dept.broker.ip', config)
            setattr(propertyReader, "remoteIp", remoteIp)
            setattr(propertyReader, "unloadScript", getRequiredValue('unloadScript', config))
            setattr(propertyReader, "cashCode", getRequiredValue('cash.code', config))
            setattr(propertyReader, "storeID", getRequiredValue('storeID', config))
            setattr(propertyReader, "remoteBroker", 'guest/guest@%s:%s' % (remoteIp, remotePort))
        except Exception, e:
            loggerSync.error(str(e))
            sys.exit()
        setattr(propertyReader, "httpHostCS",
                getValue('httpHostCS', remoteIp, config))  # почему-то параметр с КС передается, но на sync по обычаю игнорировался
        setattr(propertyReader, "httpPortCS",
                getValue('httpPortCS', cashServerRestPort, config))  # просто переименовал параметр, чтоб он совпадал со значением из SyncConfigInfo
    else:
        loggerSync.error(u'Нет файла с настройками')
        sys.exit()


rereadConfig()
