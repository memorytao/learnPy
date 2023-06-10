import requests
import json

endpoint = "https://restcountries.com/v3.1/all"


balanceCheck = ""
topUp = ""

response = requests.get(endpoint)
print(response.status_code)
