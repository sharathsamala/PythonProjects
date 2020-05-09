

def insertion_sort_imps(arr):

    final_arry = [arr[0]]

    for i in range(1,len(arr)):

        last_index = len(final_arry) -1
        while(final_arry[last_index] > arr[i]):
            last_index -= 1

        final_arry.insert(last_index+1, arr[i])

    return final_arry


sample_arr = [1,4,645,35,7,3,734,7,83,73]
print(insertion_sort_imps(sample_arr))

