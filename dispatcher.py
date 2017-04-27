from __future__ import print_function
import sys
import threading
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
        self.items_collected = {}
        #self.items_backup = {}
        #self.client_retry = {}
        #self.worker_registry = {}
        #self.lock = threading.Lock()

    @Pyro4.expose
    def putWork(self, item):
        #self.lock.acquire()
        #self.items_backup[item.assignedBy + "," + str(item.itemId)] = item
        #self.lock.release()
        for i in range(3):
            self.workqueue.put(item)

    @Pyro4.expose
    def getWork(self, worker_name, timeout=5):
        item = self.workqueue.get(timeout=timeout)
        if worker_name not in self.items_collected.keys():
            self.items_collected[worker_name] = [item.assignedBy + "," + str(item.itemId)]
            return item
        elif item.assignedBy + "," + str(item.itemId) not in self.items_collected[worker_name]:
            self.items_collected[worker_name].append(item.assignedBy + "," + str(item.itemId))
            return item
        else:
            self.workqueue.put(item)
            raise queue.Empty

    @Pyro4.expose
    def putResult(self, item):
        # print(self.items_backup.keys())
        self.resultqueue.put(item)

        #self.lock.acquire()
        #del self.items_backup[item.assignedBy + "," + str(item.itemId)]
        #self.lock.release()

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