
from configparser import NoOptionError
from lib2to3.pgen2.pgen import DFAState
from typing import Dict

bookInfo = {
    'name': 'Python Basic',
    'price' : 300,
    'years': 1990,
    'athors': 'Thomas'
}

c = [ ('name','Somsri'), ('tel', '02-222-1222') ]

print(type(c))
print(type(bookInfo))

customer = dict(c)
print(customer, type(customer))

print(customer)

print(bookInfo['name'])
print(bookInfo.get('name'))

customer['DOB'] = '01-01-1990'
print(customer)

bookInfo['price'] = None

print(bookInfo)