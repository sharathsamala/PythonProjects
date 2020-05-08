

def cumulative_sum(n):

    if n == 0:
        return 0
    else:
        return n + cumulative_sum(n-1)


print(cumulative_sum(5))


def digits_sum(n):

    if n < 10:
        return n

    else:
        return n % 10 + digits_sum(int(n/10))


print(digits_sum(12345))


def word_split(word, word_list):
    pass

