


def coin_change_loop_rn(sum, coin_arr, cache):

    min_coins = sum
    if sum in coin_arr:
        return 1

    elif cache[sum] > 0:
        return cache[sum]

    else:

        for i in [c for c in coin_arr if c <= sum]:

            num_coins = 1 + coin_change_loop_rn(sum-i, coin_arr, cache)

            if num_coins < min_coins:
                min_coins = num_coins
                cache[sum] = min_coins

    return min_coins


def coin_change_loop(sum, coin_arr):
    cache = [0] * (sum + 1)
    return coin_change_loop_rn(sum, coin_arr, cache)




print(coin_change_loop(345, [1, 3, 5, 7]))
