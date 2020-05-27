

def myfun(n):

    m=0
    while n > 0:
        m = 10 * m + n % 10
        n = n/10

    return m

print(myfun(9870))