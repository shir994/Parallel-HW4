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

class WrongDispatcher(Exception):
    pass

class DispatcherQueue(object):
    def __init__(self, second_adress, initial_role):
        self.workqueue = queue.Queue()
        self.resultsdict = {}
        self.done_items = {}
        #self.worker_heartbeat = {}
        self.items_collected = {}
        self.new_worker_flag = False
        self.register_worker_allowed = False
        self.cv = threading.Condition()

        # dispatcher - dispatcher parameters
        self._current_role = initial_role
        self.second_dispatcher = Pyro4.core.Proxy("PYRO:dispatcher@" + second_adress)
        self._master_iteration = False

        self.Dpinger = threading.Thread(target = self.pingDispatcher)
        self.Dpinger.daemon = True
        self.Dpinger.start()        


        # worker ping thread
        # self.pinger = threading.Thread(target = self.ping)
        # self.pinger.daemon = True
        # self.pinger.start()

    @Pyro4.expose
    @property
    def current_role(self):
        return self._current_role

    #@Pyro4.expose
    @current_role.setter
    def current_role(self, value):
        self._current_role = value

    @Pyro4.expose
    @property
    def master_iteration(self):
        return self._master_iteration

    #@Pyro4.expose
    @master_iteration.setter
    def master_iteration(self, value):
        self._master_iteration = value


    @Pyro4.expose
    def pingDispatcher(self, timeout=2, sleep_time=1):
        while(True):
            try:
                if self.second_dispatcher.current_role == 1:
                    if self.current_role == 0:
                        print ("ok, I am slave")
                    if self.current_role == 1:
                        print("conflict")
                        if self.master_iteration:
                            print("I am master now-->backup must set his state to 0 server to slave mode")
                            self.master_iteration = False
                        else:
                            print("I must be slave now")
                            self.current_role = 0
            except (Pyro4.errors.ConnectionClosedError, Pyro4.errors.CommunicationError):
                if self.current_role == 0:
                    self.current_role = 1
                    self.master_iteration = True
                    print("master is dead, I am master now", self.master_iteration)
            sleep(sleep_time)

    # def ping(self, timeout=2, sleep_time=1):
    #     while(True):
    #         for worker, beat in self.worker_heartbeat.items():
    #             flag = beat.wait(timeout)
    #             #print(flag)
    #             if (not flag and self.items_collected[worker]):
    #                 print(worker + " not responding --> resubmit task")
    #                 self.putWork(self.items_collected[worker])
    #                 self.items_collected[worker] = None
    #             beat.clear()
    #         sleep(sleep_time)

    #         self.cv.acquire()
    #         self.register_worker_allowed = True
    #         self.cv.notify()
    #         while self.new_worker_flag:
    #             self.cv.wait()
    #         self.register_worker_allowed = False
    #         self.cv.release()

    def CheckDispatcher(self):
        if self.current_role == 0:
            raise WrongDispatcher

    @Pyro4.expose
    def initWorker(self, worker_name):
        print("Worker " + worker_name + " registered")
        self.items_collected[worker_name] = None
        self.CheckDispatcher()

    @Pyro4.expose
    def initClient(self, client_name):
        print ("Client " + client_name + " registered")
        self.resultsdict[client_name] = queue.Queue()
        self.done_items[client_name] = {}

    # @Pyro4.expose
    # def setHeartbeat(self, worker_name):
    #     self.worker_heartbeat[worker_name].set()

    @Pyro4.expose
    def copyWork(self, item):
        self.workqueue.put(item)

    @Pyro4.expose
    def copyResult(self, item):
        self.resultsdict[item.assignedBy].put(item)
        self.done_items[item.assignedBy][item.data] = item

    @Pyro4.expose
    def putWork(self, item):
        self.CheckDispatcher()
        try:
            self.second_dispatcher.copyWork(item)
        except:
            pass
        self.workqueue.put(item)

    @Pyro4.expose
    def getWork(self, worker_name, timeout=5):
        self.CheckDispatcher()

        item = self.workqueue.get(timeout=timeout)
        while(item.data in self.done_items[item.assignedBy].keys()):
            item = self.workqueue.get(timeout=timeout)
        
        ### Dummy for now here

        #self.items_collected[worker_name] = item
        return item

    @Pyro4.expose
    def putResult(self, item):
        self.CheckDispatcher()
        try:
            self.second_dispatcher.copyResult(item)
        except:
            pass

        self.resultsdict[item.assignedBy].put(item)
        self.done_items[item.assignedBy][item.data] = item

    @Pyro4.expose
    def getResult(self, client_name, timeout=5):
        self.CheckDispatcher()
        return self.resultsdict[client_name].get(timeout=timeout)

    @Pyro4.expose
    def workQueueSize(self):
        return self.workqueue.qsize()

def main():
    # HOST:PORT
    address = str(sys.argv[1]).split(':')
    host = address[0]
    port = int(address[1])

    second_adress = str(sys.argv[2])
    initial_role = int(sys.argv[3])

    daemon = Pyro4.core.Daemon(host, port)
    dispatcher = DispatcherQueue(second_adress, initial_role)
    uri = daemon.register(dispatcher, "dispatcher")

    print("Dispatcher is running: " + str(uri))
    daemon.requestLoop()

if __name__=="__main__":
    main()