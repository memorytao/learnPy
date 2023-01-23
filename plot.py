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
sumOfEachHrs = {}

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

    sumOfEachHrs[hour] = maxOfEachCDR    
# print(sumOfEachHrs,end="\n")        
    # Create the line plot
    plt.subplots(figsize=(12, 6))
    plt.bar(maxOfEachCDR.keys(), maxOfEachCDR.values())
    plt.xlabel('CDR')
    plt.ylabel('value')
    plt.title('Source By Hour at '+ str(hour))
    plt.subplots_adjust(left=0.25, right=0.75, bottom=0.25, top=0.75)
    plt.show()