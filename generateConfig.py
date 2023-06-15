import time
import os
import pandas as pd

file = ''
brand = ''
fileNamesPrP = ['SPDListForNBAPrP.map', 'NBAstates_priorityPrP.map', 'SPDListForNBAPrP.map SIT', 'NBAstates_priorityPrP.map SIT']
fileNamesPoP = ['SPDListForNBAPoP.map', 'NBAstates_priorityPoP.map', 'SPDListForNBAPoP.map SIT', 'NBAstates_priorityPoP.map SIT']

while True:
    for i,item in enumerate(["Prepaid", "Postpaid"]):
        print(f'{i+1}) {item}')   

    brand = input('Select customers... |')
    if(brand.isdigit() and (int(brand) == 1 or int(brand) == 2 )):    
        break

profile = fileNamesPrP if int(brand) == 1 else fileNamesPoP

while True:
    for idx, name in enumerate(profile):
        print(f'{idx+1}) {name}')
    selected = input('\nPlease select file... |')
    if(selected.isdigit() and int(selected) >= 1 and int(selected) <= 4):
        file = profile[int(selected)-1]
        break
    time.sleep(0.7)
    print('\n\n...Please select number from list!... \n')

print('Selectd ->', file)

fileName = ''
df = pd.read_excel(os.getcwd()+'/Postpaid_NBA_State_configure.xlsx',sheet_name=file)

try:
    f = open(file,"w")
    for data in df['Export configure']:
        print(data)
        f.write(data+'\n')
    f.close()
    print('... Done! ...')

except:
    print(' Something wrong.. gplease check columns name or values in file ')
    print(' e.g. -> "Export configures" > "Export configure" ')

