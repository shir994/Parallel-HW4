class Workitem(object):
    def __init__(self, itemId, data):
        print("Created workitem %s" % itemId)
        self.itemId=itemId
        self.data=data
        self.result=None
        self.processedBy=None
        self.assignedBy=None

    def __str__(self):
        return "<Workitem id=%s>" % str(self.itemId)

    def __eq__(self, other):
        if isinstance(other, self.__class__) and\
                self.itemId == other.itemId and\
                self.assignedBy == other.assignedBy:
            return True
        else:
            return False
