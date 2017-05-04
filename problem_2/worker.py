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

WORKERNAME = "Worker_%d@%s" % (os.getpid(), socket.gethostname())

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


def factorize_mimic(n, q):
    q.put(factorize(n))

def process(item):
    print("factorizing %s -->" % item.data)
    sys.stdout.flush()

    q = multip_queue()
    p = Process(target=factorize_mimic, args=(int(item.data), q))
    p.start()
    item.result = q.get()
    p.join()

    print(item.result)
    item.processedBy = WORKERNAME

def beat_setter(dispatcher, worker_name, sleep_time):
    while(True):
        dispatcher.setHeartbeat(worker_name)
        sleep(sleep_time)

def main():
    SLEEPTIME = 3
    print("This is worker %s" % WORKERNAME)
    disp_address = str(sys.argv[1])
    dispatcher_one = Pyro4.core.Proxy("PYRO:dispatcher@" + disp_address)
    try:
        dispatcher_one.initWorker(WORKERNAME)
    except (Pyro4.errors.ConnectionClosedError, Pyro4.errors.CommunicationError, WrongDispatcher):
        pass

    disp_address = str(sys.argv[2])
    dispatcher_two = Pyro4.core.Proxy("PYRO:dispatcher@" + disp_address)
    try:
        dispatcher_two.initWorker(WORKERNAME)
    except (Pyro4.errors.ConnectionClosedError, Pyro4.errors.CommunicationError, WrongDispatcher):
        pass

    # SLEEPTIME = 1
    # heartbit_setter = threading.Thread(target = beat_setter, args=([dispatcher, WORKERNAME, SLEEPTIME]))
    # heartbit_setter.daemon = True
    # heartbit_setter.start()

    current_dispatcher = dispatcher_one
    while True:
        while True:
            try:
                print("1")
                try:
                    item = current_dispatcher.getWork(WORKERNAME)
                except queue.Empty:
                    print("no work available yet")
                else:
                    process(item)
                    break
            except (Pyro4.errors.ConnectionClosedError, Pyro4.errors.CommunicationError, WrongDispatcher):
                sleep(SLEEPTIME)
                print("changing dispatcher")
                if current_dispatcher == dispatcher_one:
                    current_dispatcher = dispatcher_two
                else:
                    current_dispatcher = dispatcher_one
        while(True):
            try:
                print("2")
                current_dispatcher.putResult(item)
            except (Pyro4.errors.ConnectionClosedError, Pyro4.errors.CommunicationError, WrongDispatcher):
                sleep(SLEEPTIME)
                print("changing dispatcher")
                if current_dispatcher == dispatcher_one:
                    current_dispatcher = dispatcher_two
                    print("4")
                else:
                    current_dispatcher = dispatcher_one
                    print("5")
            else:
                break            
                                    


if __name__=="__main__":
    main()