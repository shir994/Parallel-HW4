from __future__ import print_function
import os,socket,sys, threading
from time import sleep
from multiprocessing import Process
from multiprocessing import Queue as multip_queue
from math import floor, sqrt
try:
    import queue
except ImportError:
    import Queue as queue
import Pyro4.core
from workitem import Workitem

Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')

Pyro4.config.COMMTIMEOUT = 5



class WrongDispatcher(Exception):
    pass

def factorize(n):
    step = lambda x: 1 + (x<<2) - ((x>>1)<<1)
    maxq = long(floor(sqrt(n)))
    d = 1
    q = n % 2 == 0 and 2 or 3
    while q <= maxq and n % q != 0:
        q = step(d)
        d += 1
    return q <= maxq and [q] + factorize(n//q) or [n]

class Worker(object):

    def __init__(self, dispatcher_one, dispatcher_two):
        self.dispatcher_one = dispatcher_one
        self.dispatcher_two = dispatcher_two
        self.current_dispatcher = self.dispatcher_one
        self.item = None
        self.WORKERNAME = "Worker_%d@%s" % (os.getpid(), socket.gethostname())
        print("This is worker %s" % self.WORKERNAME)

    def factorize_mimic(self, n, q):
        q.put(factorize(n))

    def process(self, item):
        print("factorizing %s -->" % item.data)
        sys.stdout.flush()

        q = multip_queue()
        p = Process(target=self.factorize_mimic, args=(int(item.data), q))
        p.start()
        item.result = q.get()
        p.join()

        print(item.result)
        item.processedBy = self.WORKERNAME
        self.item = item

    def ProcessWork(self, sleep_time=3):
        while True:
            try:
                print("1")
                try:
                    item = self.current_dispatcher.getWork(self.WORKERNAME)
                except queue.Empty:
                    print("no work available yet")
                else:
                    self.process(item)
                    break
            except (Pyro4.errors.ConnectionClosedError,
                    Pyro4.errors.CommunicationError, WrongDispatcher):
                sleep(sleep_time)
                print("changing dispatcher")
                if self.current_dispatcher == self.dispatcher_one:
                    self.current_dispatcher = self.dispatcher_two
                else:
                    self.current_dispatcher = self.dispatcher_one

    def PutResult(self, sleep_time=3):
        while True:
            try:
                print("2")
                self.current_dispatcher.putResult(self.item)
            except (Pyro4.errors.ConnectionClosedError,
                    Pyro4.errors.CommunicationError, WrongDispatcher):
                sleep(sleep_time)
                print("changing dispatcher")
                if self.current_dispatcher == self.dispatcher_one:
                    self.current_dispatcher = self.dispatcher_two
                    print("4")
                else:
                    self.current_dispatcher = self.dispatcher_one
                    print("5")
            else:
                break

    def StartLoop(self):
        while True:
            self.ProcessWork()
            self.PutResult()


def main():
    disp_address = str(sys.argv[1])
    dispatcher_one = Pyro4.core.Proxy("PYRO:dispatcher@" + disp_address)
    disp_address = str(sys.argv[2])
    dispatcher_two = Pyro4.core.Proxy("PYRO:dispatcher@" + disp_address)

    worker = Worker(dispatcher_one, dispatcher_two)
    worker.StartLoop()
      
if __name__=="__main__":
    main()