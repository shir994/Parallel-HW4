from __future__ import with_statement
import os,socket,sys
try:
    import queue
except ImportError:
    import Queue as queue
import random
import Pyro4.core
from workitem import Workitem

Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')

CLIENTNAME = "Client_%d@%s" % (os.getpid(), socket.gethostname())

def readNumbers(path):
    print("\nReading numbers")
    with open(path) as f:
        lines = f.read().splitlines()
    numbers = [int(e) for e in lines]
    return numbers

def placeWork(dispatcher, numbers):
    print("\nPlacing work items into dispatcher queue")
    for i in range(len(numbers)):
        item = Workitem(i+1, numbers[i])
        item.assignedBy = CLIENTNAME
        dispatcher.putWork(item)

def collectResults(dispatcher, item_count):
    print("\nGetting results from dispatcher queue")
    results = {}
    while len(results) < item_count:
        try:
            item = dispatcher.getResult(CLIENTNAME)
            print("Got result: %s (from %s)" % (item, item.processedBy))
            results[item.data] = item.result
        except queue.Empty:
            print("Not all results available yet (got %d out of %d). Work queue size: %d" %  \
                    (len(results), item_count, dispatcher.workQueueSize()))
    return results

def writeResults(results, path):
    print("\nWriting results")
    with open(path, 'w') as f:
        for (number, factorials) in results.items():
            f.write(str(number) + ': ' + ', '.join(map(str,factorials)) + '\n')

def main():
    disp_address = str(sys.argv[1])
    numbers_path = str(sys.argv[2])
    results_path = str(sys.argv[3])

    numbers = readNumbers(numbers_path)

    with Pyro4.core.Proxy("PYRO:dispatcher@" + disp_address) as dispatcher:
        placeWork(dispatcher, numbers)
        results = collectResults(dispatcher, len(numbers))

    writeResults(results, results_path)

if __name__=="__main__":
    main()