from random import randint
from sqlite3 import enable_shared_cache
from this import d

bot = ["Y","O","H"]

# user = input(" which one Y,O,H : ")
user = ''
botAction = bot[randint(0,2)]


print( "user -> ", user, ": bot ->", botAction)


if user == botAction:
    print(" even ")
elif user == 'Y':
    if(botAction == 'O'):
        print(" you lose")
    else:
        print(" you win")
elif user == 'O':
    if(botAction == 'Y'):
        print(" you win")
    else:
        print(" you lose")
elif user == 'H':
    if(botAction == 'Y'):
        print(" you lose")
    else:
        print("you win")
else:
    print(" incorect input ")


    