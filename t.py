import random

while True:
    mom = random.randint(0, 4)
    dad = random.randint(0, 4)
    bro = random.randint(0, 4)
    variables = [mom, dad, bro]
    print(variables)
    if len(set(variables)) == len(variables): break