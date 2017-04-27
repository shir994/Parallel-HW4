from __future__ import print_function
import sys, threading
from time import sleep
try:
    import queue
except ImportError:
    import Queue as queue
import Pyro4.core

Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')

class DispatcherQueue(object):
    def __init__(self):
        self.workqueue = queue.Queue()
        self.resultqueue = queue.Queue()
        self.worker_heartbeat = {}
        self.items_collected = {}

        self.pinger = threading.Thread(target = self.ping)
        self.pinger.daemon = True
        self.pinger.start()

    @Pyro4.expose
    def ping(self, timeout=2, sleep_time=1):
        while(True):
            for worker, beat in self.worker_heartbeat.items():
                flag = beat.wait(timeout)
                #print(flag)
                if (not flag and self.items_collected[worker]):
                    print(worker + " not responding --> resubmit task")
                    self.putWork(self.items_collected[worker])
                    self.items_collected[worker] = None
                beat.clear()
            sleep(sleep_time)

    @Pyro4.expose
    def initWorker(self, worker_name):
        self.worker_heartbeat[worker_name] = threading.Event()
        self.worker_heartbeat[worker_name].set()
        self.items_collected[worker_name] = None

    @Pyro4.expose
    def setHeartbeat(self, worker_name):
        #print("Hearbeat called")
        self.worker_heartbeat[worker_name].set()

    @Pyro4.expose
    def putWork(self, item):
        self.workqueue.put(item)

    @Pyro4.expose
    def getWork(self, worker_name, timeout=5):
        item = self.workqueue.get(timeout=timeout)
        self.items_collected[worker_name] = item
        return item

    @Pyro4.expose
    def putResult(self, item):
        self.resultqueue.put(item)
        self.items_collected[item.processedBy] = None

    @Pyro4.expose
    def getResult(self, client_name, timeout=5):
        item = self.resultqueue.get(timeout=timeout)
        if (client_name != item.assignedBy):
            self.resultqueue.put(item)
            raise queue.Empty

        return item

    @Pyro4.expose
    def workQueueSize(self):
        return self.workqueue.qsize()

def main():
    # HOST:PORT
    address = str(sys.argv[1]).split(':')
    host = address[0]
    port = int(address[1])

    daemon = Pyro4.core.Daemon(host, port)
    dispatcher = DispatcherQueue()
    uri = daemon.register(dispatcher, "dispatcher")

    print("Dispatcher is running: " + str(uri))
    daemon.requestLoop()

if __name__=="__main__":
    main()