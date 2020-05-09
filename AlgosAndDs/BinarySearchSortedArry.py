

def binary_search_sorted_arry(arr, ele):

    if len(arr) == 0:
        return False

    middle_ele = len(arr) // 2

    if arr[middle_ele] == ele:
        return True
    else:

        if arr[middle_ele] > ele:
            return binary_search_sorted_arry(arr[:middle_ele], ele)

        else:
           return binary_search_sorted_arry(arr[middle_ele+1:], ele)




arr = [1,3,5,9,13,17,21]

print(binary_search_sorted_arry(arr, 77))