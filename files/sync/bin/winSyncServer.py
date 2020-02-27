
import win32serviceutil
import pythoncom
import win32service
import win32event
import servicemanager
import socket
from thrift.transport import TSocket
import sys

class ArtixSyncServer(win32serviceutil.ServiceFramework):
    _svc_name_ = "ArtixSyncServer"
    _svc_display_name_ = "ArtixSyncServer"
    transport = TSocket.TServerSocket('0.0.0.0', 7795)

    def __init__(self,args):
        win32serviceutil.ServiceFramework.__init__(self,args)
        self.hWaitStop = win32event.CreateEvent(None,0,0,None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.transport.close()
        win32event.SetEvent(self.hWaitStop)
        self.ReportServiceStatus(win32service.SERVICE_STOPPED)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_,''))
        self.main()

    def main(self):
        myPath = sys.path[-1]
        sys.path[-1] = sys.path[0]
        sys.path[0] = myPath
        SyncServer = __import__('SyncServer', fromlist='q')
        SyncServer.startService(self.transport)

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(ArtixSyncServer)
