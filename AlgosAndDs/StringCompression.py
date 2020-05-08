

def string_compression(s):

    final_string = ""
    previous_char = ""
    counter = 0

    if s == "":
        return ""
    for val in s:
        if previous_char == val:
            counter += 1
        elif previous_char == "":
            counter += 1
            previous_char = val
        else:
            final_string += previous_char+str(counter)
            counter = 1
            previous_char = val
    final_string += previous_char+str(counter)
    print(final_string)

string_compression("adddddaaaaeAAAhhGGGG") #a1d5a4e1A3h2G4


