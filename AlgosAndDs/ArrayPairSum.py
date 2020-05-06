from nose.tools import assert_equal
import time


def array_pair_sum(input_array, k):

    pair_arry = []
    array_len = len(input_array)

    for i in range(array_len):

        for j in range(i+1, array_len):

            if input_array[i] + input_array[j] == k:
                a, b = (input_array[i], input_array[j]) if (input_array[i] > input_array[j]) else (input_array[j],
                                                                                                   input_array[i])
                pair_arry.append("({0},{1})".format(a, b))

    # print(list(set(pair_arry)))
    return len(list(set(pair_arry)))


def array_pair_sum1(input_array, k):

    # edge case
    if(len(input_array)<2):
        return 0

    seen = set()
    pairs = set()

    for val in input_array:

        target = k - val
        if target not in seen:
            seen.add(val)

        else:
            pairs.add((min(val, target), max(val, target)))

    return len(pairs)


class ArrayPairSumTest(object):

    def test(self, sol):
        start = time.time()
        assert_equal(sol([1, 9, 2, 8, 3, 7, 4, 6, 5, 5, 13, 14, 11, 13, -1], 10), 6)
        assert_equal(sol([1, 2, 3, 1], 3), 1)
        assert_equal(sol([1, 3, 2, 2], 4), 2)
        end = time.time()
        print("All tests are passed in {0} sec".format(end - start))


t = ArrayPairSumTest()
t.test(array_pair_sum)
t.test(array_pair_sum1)

# print(array_pair_sum([1, 2, 3, 1], 3))

