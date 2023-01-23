import pandas as pd
import matplotlib.pyplot as plt

# Read the CSV file into a DataFrame
df = pd.read_csv("Train_WD_2.csv")

# Extract the data for the x and y values
hrs = df['Hour']
cdr = df['CDR']
values = df['value']

listOfCDR = cdr.unique().tolist()
listOfDatas = list(zip(hrs, cdr, values))
maxOfEachHrs = dict.fromkeys(hrs, 0)

for hour in maxOfEachHrs.keys():
    
    maxOfEachCDR = dict.fromkeys(listOfCDR, 0)
    for cdr in maxOfEachCDR.keys():
        findMax = 0
        for colHrs,colCDR,colVal in listOfDatas:
            if (findMax == 0 and cdr == colCDR and hour == colHrs):
                findMax = colVal
                maxOfEachCDR[cdr] = findMax
                continue
            if (cdr == colCDR and hour == colHrs and colVal > findMax ):
                findMax = colVal
                maxOfEachCDR[cdr] = findMax
                
    # Create the line plot
    print(maxOfEachCDR)
    # plt.bar(maxOfEachCDR.keys(), maxOfEachCDR.values())
    # plt.xlabel('CDR')
    # plt.ylabel('value')
    # plt.title('Source By '+ str(hour))
    # plt.figure(hour)
    # plt.show()