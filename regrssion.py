import requests

createProfile = 'https://10.89.241.3:443/link/data/input/CCBData'
prepaidProfile = {'msisdn': '66600040007', 'accountType': 'PREPAID', 'accountType': 'NEW_REGISTRATION'}

res = requests.post(createProfile , json = prepaidProfile)

print(res.text)