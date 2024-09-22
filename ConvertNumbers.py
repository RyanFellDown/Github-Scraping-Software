#This function converts numbers with K's to their respective whole number (aka 100K -> 100,000).

def convertNumbers(peopleTotal):
    finalTotal = []
    for numbers in peopleTotal:
        if 'k' in numbers:
            newString = numbers.replace('k', '')
            newString = float(newString)
            newString = newString * 1000
            newString = int(newString)
            finalTotal.append(newString)
        else:
            finalTotal.append(numbers)
    return finalTotal