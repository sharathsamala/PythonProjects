

class Stack(object):

    def __init__(self):
        self.stack_val = []
        self.length = 0

    def push(self, k):
        self.stack_val.append(k)
        self.length += 1

    def pop(self):
        if not self.isEmpty():
            self.length -= 1
            return self.stack_val.pop()
        else:
            return Exception('Stack is empty, unable to pop elements')

    def peek(self):
        return self.stack_val[self.length - 1]

    def size(self):
        return self.length

    def isEmpty(self):
        return self.length == 0


newStack = Stack()

newStack.push(34)
newStack.push('hey this is')
newStack.push(True)
print(newStack.size())
print(newStack.pop())
print(newStack.peek())
print(newStack.pop())
print(newStack.size())
print(newStack.isEmpty())
print(newStack.pop())
print(newStack.pop())
print(newStack.pop())