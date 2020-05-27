

class QueueImpl(object):
    def __init__(self):
        self.queue = []
        self.size = 0

    def enqueue(self, val):
        self.queue.append(val)
        self.size += 1

    def dequeue(self):

        if self.size > 0:
            val = self.queue[0]
            self.queue.remove(val)
            self.size -= 1
            return val
        else:
            raise Exception("No elements")

    def getSize(self):
        return self.size


def chair_and_song_without_queue(n, k):
    arry = [i for i in range(1, n+1)]
    while len(arry) > 1:

        for j in range(k):
            if j < k-1:
                t = arry[0]
                arry.append(t)
                arry.remove(t)
            else:
                arry.remove(arry[0])
    return arry[0]


def chair_and_song_recc(n, k):
    if n == 1:
        return 1
    else:
        return (chair_and_song_recc(n - 1, k) + k - 1) % n + 1


def chair_and_song(n, k):

    if n < 1 or k < 1:
        return "Not valid"

    elif n == 1:
        return n

    else:

        qu = QueueImpl()
        for n in range(1, n+1):
            qu.enqueue(n)

        while qu.size > 1:
            for i in range(k-1):
                if qu.size > 1:
                    qu.enqueue(qu.dequeue())
            # print(qu.dequeue()) for debugging
            qu.dequeue()

        return qu.dequeue()


print(chair_and_song(5, 2))
print(chair_and_song_without_queue(5, 2))
print(chair_and_song_recc(5, 2))




