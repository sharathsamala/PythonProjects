

def bubble_sort_impl(arr):

    arr_len = len(arr)

    # second loop is checking less number
    for i in range(arr_len-1,0,-1):

        for j in range(i):

            if arr[j] > arr[j+1]:
                t = arr[j+1]
                arr[j+1] = arr[j]
                arr[j] = t

    return arr


sample_arr = [1,4,645,35,7,3,734,7,83,73]
print(bubble_sort_impl(sample_arr))