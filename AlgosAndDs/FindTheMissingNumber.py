from nose.tools import assert_equal
import time




def find_missing_number(array_1, array_2):

    array_1_sum = 0
    array_2_sum = 0
    for val in array_1:
        array_1_sum += val

    for val in array_2:
        array_2_sum += val

    return array_1_sum - array_2_sum



class MissingNumberTest(object):

    def test(self, sol):
        start = time.time()
        assert_equal(sol([5, 5, 7, 7], [5, 7, 7]), 5)
        assert_equal(sol([1, 2, 3, 4, 5, 6, 7], [3, 7, 2, 1, 4, 6]), 5)
        assert_equal(sol([9, 8, 7, 6, 5, 4, 3, 2, 1], [9, 8, 7, 5, 4, 3, 2, 1]), 6)
        end = time.time()
        print("All tests are passed in {0} sec".format(end - start))


t = MissingNumberTest()
t.test(find_missing_number)
