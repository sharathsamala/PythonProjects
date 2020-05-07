

def sentence_reversal(input_str):

    word_list = []

    word = ""
    prev = ""
    for c in input_str:

        if c == " " and prev == "":
            if word != "":
                word_list.append(word)
                word = ""
        else:
            word += c
            prev = ""

    return " ".join(reversed(word_list))

print(sentence_reversal("     this is       a bird    "))