

def string_rev(s):

    if len(s) == 1:
        return s

    else:
        return string_rev(s[1:]) + s[0]

print(string_rev("sharath"))


