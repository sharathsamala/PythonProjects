

# dynamic programming with cache
def dp_min_coins(target, coins, cache):

    print("getting min val for ", target)
    # base case
    min_coins = target
    if target in coins:
        cache[target] = 1
        return 1

    elif cache[target] > 0:
        return cache[target]

    else:

        for i in [c for c in coins if c <= target]:

            num_coins = 1 + dp_min_coins(target-i, coins, cache)

            if num_coins < min_coins:
                min_coins = num_coins
                cache[target] = min_coins
    return min_coins


target = 75
coins = (1,5,10,25)
cache = [0]*(target+1)

print(dp_min_coins(target, coins, cache))

