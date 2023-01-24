import pandas as pd
import matplotlib.pyplot as plt

# Read the CSV file into a DataFrame
df = pd.read_csv("Train_WD_2_copy.csv")

# Extract the data for the x and y values
hrs = df['Hour']
cdr = df['CDR']
values = df['value']

listOfCDR = cdr.unique().tolist()
listOfDatas = list(zip(hrs, cdr, values))


# column1_name = 'CDR'
# column2_name = 'Hour'
# condition = (df[column1_name] == 'MON') & (df[column2_name] == 10)
# filtered_df = df[condition]
# print(filtered_df)

# getVal = (df['CDR'] == 'MGR') & (df['Hour'] == 0)
# graph each hours
# for cdr in listOfCDR:


# for CDR in listOfCDR:
#     for i in range(24):
#         condition = (CDR == cdr) & (hrs == i)
#         print(df[condition].sum())

df_hour_0 = df[(df['Hour'] == 0) & (df['CDR'] == 'MGR')]
getNew = df_hour_0.groupby(['CDR','Hour'])['value'].sum().reset_index()

df_grouped = df.groupby(['CDR', 'Hour'])['value'].sum().reset_index()
print(df_grouped)
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

plt.scatter(df_hour_0['Hour'], df_hour_0['value'])
plt.subplots_adjust(left=0.25, right=0.75, bottom=0.25, top=0.75)
plt.show()
