from nose.tools import assert_equal
import time


# Approach one - general
def anagram_check(s1, s2):

    s1_arr = []
    for c in s1.lower().replace(' ', ''):
        s1_arr.append(c)

    for c in s2.lower().replace(' ', ''):
        if c not in s1_arr:
            return False
        s1_arr.remove(c)
    return True if len(s1_arr) == 0 else False


# Approach two - quick and optimized
def anagram_check1(s1, s2):

    s1 = s1.replace(' ', '').lower()
    s2 = s2.replace(' ', '').lower()

    return (len(s1) == len(s2)) and (sorted(s1) == sorted(s2))


class AnagramCheckTest(object):

    def test(self, sol):
        start = time.time()
        assert_equal(sol('go go go', 'gggooo'), True)
        assert_equal(sol('abc', 'cba'), True)
        assert_equal(sol('hi man', 'man      hi'), True)
        assert_equal(sol('aabbcc', 'aabbc'), False)
        assert_equal(sol('123', '1 3'), False)
        end = time.time()
        print("All tests are passed in {0} sec".format(end - start))

# print(anagram_check('hi man', 'man      hi'))
a = AnagramCheckTest()
a.test(anagram_check)
a.test(anagram_check1)
