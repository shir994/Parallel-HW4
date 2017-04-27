from __future__ import print_function
import os,socket,sys, threading
from time import sleep
from math import floor, sqrt
try:
    import queue
except ImportError:
    import Queue as queue
import Pyro4.core
from workitem import Workitem

Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')

WORKERNAME = "Worker_%d@%s" % (os.getpid(), socket.gethostname())

def factorize(n):
    step = lambda x: 1 + (x<<2) - ((x>>1)<<1)
    maxq = long(floor(sqrt(n)))
    d = 1
    q = n % 2 == 0 and 2 or 3
    while q <= maxq and n % q != 0:
        q = step(d)
        d += 1
    return q <= maxq and [q] + factorize(n//q) or [n]

def process(item):
    print("factorizing %s -->" % item.data)
    sys.stdout.flush()
    item.result = factorize(int(item.data))
    print(item.result)
    item.processedBy = WORKERNAME

def beat_setter(dispatcher, worker_name, sleep_time):
    while(True):
        dispatcher.setHeartbeat(worker_name)
        sleep(sleep_time)

def main():
    disp_address = str(sys.argv[1])
    dispatcher = Pyro4.core.Proxy("PYRO:dispatcher@" + disp_address)
    print("This is worker %s" % WORKERNAME)
    dispatcher.initWorker(WORKERNAME)

    SLEEPTIME = 1
    heartbit_setter = threading.Thread(target = beat_setter, args=([dispatcher, WORKERNAME, SLEEPTIME]))
    heartbit_setter.daemon = True
    heartbit_setter.start()

    while True:
        try:
            item = dispatcher.getWork(WORKERNAME)
        except queue.Empty:
            print("no work available yet")
        else:
            process(item)
            dispatcher.putResult(item)

if __name__=="__main__":
    main()