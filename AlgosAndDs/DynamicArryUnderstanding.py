import sys

arr = []

for n in range(50):
    array_len = len(arr)
    array_size = sys.getsizeof(arr)
    print("Array length = {0} and Array size = {1}".format(array_len, array_size))
    arr.append(n)

# Sample output
# ---------------
# Array length = 0 and Array size = 64
# Array length = 1 and Array size = 96
# Array length = 2 and Array size = 96
# Array length = 3 and Array size = 96
# Array length = 4 and Array size = 96
