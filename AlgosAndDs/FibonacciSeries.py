

def fibonacci_1(n):

    if n == 0:
        return None
    if n == 1:
        return [0]
    if n == 2:
        return [1, 1]

    output = list()

    output.append(1)
    output.append(1)

    for i in range(2, n):
        output.append(output[i-2]+output[i-1])

    return output[n-1]

print(fibonacci_1(10))