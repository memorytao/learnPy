import pandas as pd
import matplotlib.pyplot as plt

# Read the CSV file into a DataFrame
df = pd.read_csv("Train_WD_2.csv")

# Extract the data for the x and y values
hrs = df['Hour']
cdr = df['CDR']
values = df['value']

listOfCDR = cdr.unique().tolist()
listOfDatas = list(zip(cdr, values))
maxOfEachCDR = dict.fromkeys(listOfCDR,0)

for cdr in maxOfEachCDR.keys():
    findMax = 0
    for key,value in listOfDatas:
        if(findMax == 0 and cdr == key):
            findMax = value
            maxOfEachCDR[cdr] = findMax
            continue
        if(cdr == key and value > findMax):
            findMax = value
            maxOfEachCDR[cdr] = findMax


print(maxOfEachCDR)

# Create the line plot
plt.bar(maxOfEachCDR.keys(), maxOfEachCDR.values())
plt.xlabel('CDR')
plt.ylabel('value')
plt.title('Source By Time')
plt.show()

