

def quick_sort(arr, start, end):

    if start < end:
        pIndex = start
        pivotVal = arr[end]
        for i in range(start, end-1):
            if arr[i] <= pivotVal:
                t = arr[i]
                arr[i] = arr[pIndex]
                arr[pIndex] = t
                pIndex += 1

        quick_sort(arr, start, pIndex-1)
        quick_sort(arr, pIndex + 1, end)

    return arr


sample_arr = [1,4,645,35,7,3,734,7,83,73]
print(quick_sort(sample_arr, 1, 9))

