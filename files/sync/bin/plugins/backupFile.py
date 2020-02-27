# -*- coding: UTF-8 -*-

import os, sys, shutil, pickle
import zipfile
import glob
from datetime import timedelta, datetime
import logging
from operator import itemgetter
from uuid import uuid4
import propertyReader

log = logging.getLogger('su.artix.loggerSync')

def prepareDirectory(directory):
    '''
    Создание директории <рабочая_директория_sync>/../backup/directory
    '''
    destination = os.path.join(sys.path[0], '..', 'backup', directory)
    if not os.path.exists(destination):
        os.makedirs(destination)
    return destination

def sort_files_by_last_modified(files):
    '''
    Сортировка файлов по возрастанию даты последнего изменения
    '''
    fileData = {}
    for fname in files:
        fileData[fname] = os.stat(fname).st_mtime
    fileData = sorted(fileData.items(), key = itemgetter(1))
    return fileData

def makeArchive(sourceFile, destDir):
    '''
    Создание архива в директоии destDir с содержимым sourceFile и удалением sourceFile после архивации
    Директория destDir расположена в <рабочая_директория_sync>/../backup/
    '''
    destDir = prepareDirectory(destDir)
    archivePath = None

    if os.path.exists(sourceFile):
        log.info('Переносим в архив файл %s' % sourceFile)
        archivePath = os.path.join(destDir, getArchiveName(sourceFile, destDir))

        if zipfile.is_zipfile(sourceFile):
            log.error("Файл %s является архивом, перемещаем в папку назначения %s" % (sourceFile, destDir))
            os.rename(sourceFile, archivePath)
            raise Exception("Файл %s является архивом" % sourceFile)

        gz = zipfile.ZipFile(archivePath, 'a', 8, True)
        gz.write(sourceFile, None, 8)
        gz.close()
        remove(sourceFile)
        log.info('Создан архив %s' % archivePath)
    else:
        log.warning('Файл %s не найден.' % sourceFile)
    return archivePath

def getArchiveName(sourceFile, destDir):
    '''
    Получение имени архива для файла sourceFile в директории destDir.
    Функция учитывает существующие имена архивов и версионирует архив при необходимости.
    '''
    fileName = os.path.basename(os.path.splitext(sourceFile)[0])
    root = os.path.join(destDir, fileName)
    ext = '.zip'
    vers = ''
    num = 1
    while os.path.exists(root + vers + ext):
        vers = '_' + str(num)
        num += 1
    return fileName + vers + ext

def archive(srcFile, directory, maxCountFiles=propertyReader.maxCountBackupFiles, actualCountDays=propertyReader.actualCountDaysForBackupFiles):
    '''
    Создание архива с версионированным именем srcFile в директории directory и параметризированное удаление устаревших архивов
    '''
    dstDir = prepareDirectory(directory)
    makeArchive(srcFile, directory)
    backupPruning(dstDir, maxCountFiles, actualCountDays)

def backupPruning(directory, maxCountFiles=propertyReader.maxCountBackupFiles, actualCountDays=propertyReader.actualCountDaysForBackupFiles):
    if maxCountFiles > 0:
        files = glob.glob('%s/*.zip' % directory)
        sorted_files = sort_files_by_last_modified(files)
        deleteCount = len(sorted_files) - maxCountFiles
        for x in range(0, deleteCount):
            log.info("Удаляем устаревший архив %s" % sorted_files[x][0])
            remove(sorted_files[x][0])
    if actualCountDays > 0:
        files = glob.glob('%s/*.zip' % directory)
        for fname in files:
            actualDate = datetime.now() - timedelta(days=actualCountDays)
            if datetime.fromtimestamp(os.stat(fname).st_mtime) < actualDate:
                log.info("Удаляем устаревший архив %s" % fname)
                remove(fname)

def move(sourcePath, directory):
    '''
    Безопасное перемещение файла/директории sourcePath в директорию directory.
    Если директории directory не существует, то она будет создана
    Директория directory расположена в <рабочая_директория_sync>/../backup/
    '''
    destinationPath = prepareDirectory(directory)
    if os.path.exists(sourcePath):
        shutil.move(sourcePath, destinationPath)

def getUnloadedSalesPath():
    '''
    Функция возвращает(если не существует, то создаёт) путь к невыгруженным продажам
    '''
    return prepareDirectory(propertyReader.unloadedSalesDirName)

def defferSale(sourceFile, destDir, properties):
    '''
    Функция создания невыгруженной продажи. Создаёт архив с продажей sourceFile и её параметрами properties
    sourceFile - файл продаж,
    destDir - путь к архиву невыгруженной продажи
    properties - параметры невыгруженной продажи
    '''
    propFileName = 'sale.properties'
    try:
        log.info(u"Создание архива невыгруженных продаж из файла %s" % sourceFile)
        archiveName = makeArchive(sourceFile, destDir)
        archive = zipfile.ZipFile(archiveName, 'a', 8, True)
        with open(propFileName, 'wb') as propFile:
            pickle.dump(properties, propFile, pickle.HIGHEST_PROTOCOL)
        archive.write(propFileName, None, 8)
        archive.close()
        remove(propFileName)
    except Exception, e:
        log.error(str(e))

def getUnloadedSale(archiveName, destDir):
    '''
    Получение пути к архиву продаж и параметров продаж по имени архива невыгруженной продажи
    '''
    fileName = os.path.basename(os.path.splitext(archiveName)[0])
    archive = zipfile.ZipFile(archiveName, 'r')
    pureArchivePath = os.path.join(destDir, fileName + "_" + str(uuid4()) + '.zip')
    pureArchive = zipfile.ZipFile(pureArchivePath, 'w', 8, True)
    props = {}
    for item in archive.infolist():
        buf = archive.read(item.filename)
        if item.filename == 'sale.properties':
            with open(archive.extract(item.filename, destDir), 'rb') as propFile:
                props = pickle.load(propFile)
            remove(os.path.join(destDir, item.filename))
        else:
            pureArchive.writestr(item, buf)
    archive.close()
    pureArchive.close()
    return pureArchivePath, props

def remove(path):
    '''
    Безопасное удаление директории/файла
    '''
    if os.path.exists(path):
        os.remove(path)
