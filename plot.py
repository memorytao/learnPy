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


column1_name = 'CDR'
column2_name = 'Hour'
condition = (df[column1_name] == 'MON') & (df[column2_name] == 10)
filtered_df = df[condition]
print(filtered_df)

# graph each hours




# print(sumOfEachHrs,end="\n")        
    # Create the line plot
    # plt.figure(figsize=(12, 6))
    
    # plt.plot(maxOfEachCDR.keys(), maxOfEachCDR.values())
    # plt.scatter(maxOfEachCDR.keys(), maxOfEachCDR.values())
    # plt.xlabel('Type of CDR')
    # plt.ylabel('Number of CDR')
    # plt.ylim(-10000,999999) 
    # plt.title('Source By Hour at '+ str(hour))
    # plt.subplots_adjust(left=0.25, right=0.75, bottom=0.25, top=0.75)
# plt.show()
#