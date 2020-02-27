# -*- coding: utf-8 -*-

import logging
import logging.config
from logging.handlers import WatchedFileHandler, RotatingFileHandler
import os, sys

class WatchedFileHandlerMakeDir(WatchedFileHandler):
    def __init__(self, filename, mode='a', encoding=None, delay=0):
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        WatchedFileHandler.__init__(self, filename, mode, encoding, delay)

class RotatingFileHandlerMakeDir(RotatingFileHandler):
    def __init__(self, filename, maxBytes=1000000, backupCount=10, mode='a', encoding=None, delay=0):
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        RotatingFileHandler.__init__(self, filename, mode, maxBytes, backupCount, encoding, delay)

logging.config.fileConfig('%s/../properties/logging.conf' % sys.path[0])
