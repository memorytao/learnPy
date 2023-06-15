import urllib3
import requests
import time

createProfile = 'https://10.89.241.3:443/link/data/input/CCBData'
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 66330001000 - 66330001999
init = 66330001000

for i in range(1000):
    prepaidProfile = {
    "msisdn": str(init),
    "customerNumber": str(init),
    "accountType": "POSTPAID",
    "companyCode": "dtn",
    "recordTimestamp": "2022-10-26 17:20:10",
    "agentCode" : "LLPRASIT",
    "recordType": "NEW_REGISTRATION",
    "transactionId": "CCBO2015051918231725861020",
    "messageId": "CCBO2015051918231725861020_1"
    }
    init+=1
    print(prepaidProfile)
    res = requests.post(createProfile, json = prepaidProfile, verify = False)
    print(res.json())


