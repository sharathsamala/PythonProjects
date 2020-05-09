

"""
- loop1 = from big to small
    set maxPosition = 0
    loop to find max position val
- set big value to maxposition val
"""
def selection_sort_impl(arr):

    arr_len = len(arr)

    for i in range(arr_len-1, 0, -1):
        max_position = 0

        for j in range(1, i+1):
            if arr[j] > arr[max_position]:
                max_position = j

        t = arr[i]
        arr[i] = arr[max_position]
        arr[max_position] = t

    return arr

sample_arr = [1,4,645,35,7,3,734,7,83,73]
print(selection_sort_impl(sample_arr))