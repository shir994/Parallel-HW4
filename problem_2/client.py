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

Pyro4.config.COMMTIMEOUT = 5

class WrongDispatcher(Exception):
    pass

class Client(object):

    def __init__(self, dispatcher_one, dispatcher_two, numbers):
        self.dispatcher_one = dispatcher_one
        self.dispatcher_two = dispatcher_two
        self.current_dispatcher = self.dispatcher_one
        self.numbers = numbers
        self.CLIENTNAME = "Client_%d@%s" % (os.getpid(), socket.gethostname())
        self.current_work_index = 0
        self.results = {}

        self.RegisterClient()

    def placeWork(self):
        print("\nPlacing work items into dispatcher queue")
        for i in range(self.current_work_index, len(self.numbers)):
            item = Workitem(i+1, self.numbers[i])
            item.assignedBy = self.CLIENTNAME
            self.current_dispatcher.putWork(item)
            self.current_work_index = i + 1

    def collectResult(self):
        try:
            item = self.current_dispatcher.getResult(self.CLIENTNAME)
            print("Got result: %s (from %s)" % (item, item.processedBy))
            self.results[item.data] = item.result
        except queue.Empty:
            print("Not all results available yet (got %d out of %d). Work queue size: %d" %  \
                    (len(self.results), len(self.numbers), self.current_dispatcher.workQueueSize()))

    def RegisterClient(self):
        try:
            self.dispatcher_one.initClient(self.CLIENTNAME)
        except (Pyro4.errors.ConnectionClosedError, Pyro4.errors.CommunicationError, WrongDispatcher):
            pass

        try:
            self.dispatcher_two.initClient(self.CLIENTNAME)
        except (Pyro4.errors.ConnectionClosedError, Pyro4.errors.CommunicationError, WrongDispatcher):
            pass

    def SendWork(self, sleep_time=3):
        while(self.current_work_index < len(self.numbers)):
            try:
                self.placeWork()
            except (Pyro4.errors.ConnectionClosedError,
                    Pyro4.errors.CommunicationError, WrongDispatcher):
                print("changing dispatcher")
                sleep(sleep_time)
                if self.current_dispatcher == self.dispatcher_one:
                    self.current_dispatcher = self.dispatcher_two
                else:
                    self.current_dispatcher = self.dispatcher_one

    def GetResults(self, sleep_time=3):
        while(len(self.results) < len(self.numbers)):
            try:
                self.collectResult()
            except (Pyro4.errors.ConnectionClosedError,
                    Pyro4.errors.CommunicationError, WrongDispatcher):
                print("changing dispatcher")
                sleep(sleep_time)
                if self.current_dispatcher == self.dispatcher_one:
                    self.current_dispatcher = self.dispatcher_two
                else:
                    self.current_dispatcher = self.dispatcher_one
        return self.results

def readNumbers(path):
    print("\nReading numbers")
    with open(path) as f:
        lines = f.read().splitlines()
    numbers = [int(e) for e in lines]
    return numbers

def writeResults(results, path):
    print("\nWriting results")
    with open(path, 'w') as f:
        for (number, factorials) in results.items():
            f.write(str(number) + ': ' + ', '.join(map(str,factorials)) + '\n')

def main():
    disp_address = str(sys.argv[1])
    dispatcher_one = Pyro4.core.Proxy("PYRO:dispatcher@" + disp_address)
    disp_address = str(sys.argv[2])
    dispatcher_two = Pyro4.core.Proxy("PYRO:dispatcher@" + disp_address)

    numbers_path = str(sys.argv[3])
    results_path = str(sys.argv[4])


    numbers = readNumbers(numbers_path)

    client = Client(dispatcher_one, dispatcher_two, numbers)

    client.SendWork()
    results = client.GetResults()

    writeResults(results, results_path)

if __name__=="__main__":
    main()
