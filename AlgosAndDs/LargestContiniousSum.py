

def largest_cont_sum(arr):

    if len(arr) == 0:
        return 0

    current_sum = max_sum = arr[0]

    for i in arr[1:]:

        current_sum = max(current_sum+i, i)
        max_sum = max(current_sum, max_sum)

    return max_sum


print(largest_cont_sum([1, 2, -1, 3, 4, 10, 10, -10, -1])) # ans 29

print(largest_cont_sum([-1, 2, -55, 3, 4, 10, 10, -10, -1, 66, -55]))