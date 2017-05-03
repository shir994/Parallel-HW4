from __future__ import with_statement
import os,socket,sys
try:
    import queue
except ImportError:
    import Queue as queue
import random
import Pyro4.core
from workitem import Workitem
from time import sleep

Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')



class WrongDispatcher(Exception):
    pass


def readNumbers(path):
    print("\nReading numbers")
    with open(path) as f:
        lines = f.read().splitlines()
    numbers = [int(e) for e in lines]
    return numbers

class Client(object):

    def __init__(self):
        self.CLIENTNAME = "Client_%d@%s" % (os.getpid(), socket.gethostname())
        self.current_work_index = 0
        self.results = {}

    def placeWork(self, dispatcher, numbers):
        print("\nPlacing work items into dispatcher queue")
        for i in range(self.current_work_index, len(numbers)):
            item = Workitem(i+1, numbers[i])
            item.assignedBy = self.CLIENTNAME
            dispatcher.putWork(item)
            self.current_work_index = i + 1

    def collectResult(self, dispatcher, item_count):
        try:
            item = dispatcher.getResult(self.CLIENTNAME)
            print("Got result: %s (from %s)" % (item, item.processedBy))
            self.results[item.data] = item.result
        except queue.Empty:
            print("Not all results available yet (got %d out of %d). Work queue size: %d" %  \
                    (len(self.results), item_count, dispatcher.workQueueSize()))

def writeResults(results, path):
    print("\nWriting results")
    with open(path, 'w') as f:
        for (number, factorials) in results.items():
            f.write(str(number) + ': ' + ', '.join(map(str,factorials)) + '\n')

def main():
    SLEEPTIME = 1
    client = Client()
    disp_address = str(sys.argv[1])
    dispatcher_one = Pyro4.core.Proxy("PYRO:dispatcher@" + disp_address)
    try:
        dispatcher_one.initClient(client.CLIENTNAME)
    except (Pyro4.errors.ConnectionClosedError, Pyro4.errors.CommunicationError, WrongDispatcher):
        pass

    disp_address = str(sys.argv[2])
    dispatcher_two = Pyro4.core.Proxy("PYRO:dispatcher@" + disp_address)
    try:
        dispatcher_two.initClient(client.CLIENTNAME)
    except (Pyro4.errors.ConnectionClosedError, Pyro4.errors.CommunicationError, WrongDispatcher):
        pass

    numbers_path = str(sys.argv[3])
    results_path = str(sys.argv[4])

    numbers = readNumbers(numbers_path)

    current_dispatcher = dispatcher_one
    while(client.current_work_index < len(numbers)):
        try:
            client.placeWork(current_dispatcher, numbers)
        except (Pyro4.errors.ConnectionClosedError, Pyro4.errors.CommunicationError, WrongDispatcher):
            print("changing dispatcher")
            sleep(SLEEPTIME)
            if current_dispatcher == dispatcher_one:
                current_dispatcher = dispatcher_two
            else:
                current_dispatcher = dispatcher_one        

    while(len(client.results) < len(numbers)):
        try:
            client.collectResult(current_dispatcher, len(numbers))
        except (Pyro4.errors.ConnectionClosedError, Pyro4.errors.CommunicationError, WrongDispatcher):
            print("changing dispatcher")
            sleep(SLEEPTIME)
            if current_dispatcher == dispatcher_one:
                current_dispatcher = dispatcher_two
            else:
                current_dispatcher = dispatcher_one 

    writeResults(client.results, results_path)

if __name__=="__main__":
    main()