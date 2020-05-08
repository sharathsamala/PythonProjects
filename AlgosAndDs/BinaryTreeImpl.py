

class BinaryTree(object):

    def __init__(self, val_obj):
        self.key = val_obj
        self.right_child = None
        self.left_child = None

    def insert_left(self, val_obj):

        if not self.left_child:
            self.left_child = BinaryTree(val_obj)

        else:
            t = self.left_child
            t.left_child = BinaryTree(val_obj)
            self.left_child = t

    def insert_right(self, val_obj):

        if not self.right_child:
            self.right_child = BinaryTree(val_obj)

        else:
            t = self.right_child
            t.right_child = BinaryTree(val_obj)
            self.right_child = t

    def getRightChild(self):
        return self.right_child

    def getLeftChild(self):
        return self.left_child

    def setRootVal(self, obj):
        self.key = obj

    def getRootVal(self):
        return self.key



b = BinaryTree('p')
b.insert_right('r')
b.insert_left('l')

print(b.getRightChild().getRootVal())

