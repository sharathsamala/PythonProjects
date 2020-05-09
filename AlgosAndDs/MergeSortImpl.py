

def merge_sort(arr):

    if len(arr) > 1:

        mid_size = len(arr) // 2
        left_arr = arr[:mid_size]
        right_arr = arr[mid_size:]

        merge_sort(left_arr)
        merge_sort(right_arr)

        i, j, k = 0, 0, 0

        while i < len(left_arr) and j < len(right_arr):

            if left_arr[i] < right_arr[j]:
                arr[k] = left_arr[i]
                i += 1
            else:
                arr[k] = right_arr[j]
                j += 1
            k += 1

        while i < len(left_arr):
            arr[k] = left_arr[i]
            i += 1
            k += 1

        while j < len(right_arr):
            arr[k] = right_arr[j]
            j += 1
            k += 1
    return arr

sample_arr = [1,4,645,35,7,3,734,7,83,73]
print(merge_sort(sample_arr))

