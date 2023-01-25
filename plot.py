import pandas as pd
import matplotlib.pyplot as plt
# /content/Train_WD_2.csv
df = pd.read_csv('D:\code\py\Train_WD_2.csv')

hrs = df['Hour']
cdr = df['CDR']
values = df['value']
listOfCDR = cdr.unique().tolist()
df['ratio'] = df['value']/df.groupby(['CDR', 'Hour'])['value'].transform('max')

for i in range(24):
    fig, ax = plt.subplots()
    for CDR in listOfCDR:
        condition = (hrs == i) & (cdr == CDR)
        df[condition].plot(x='index', y='ratio', label=CDR, ax=ax)
    plt.legend()
    plt.title("Ratio values for Hour " + str(i))
    plt.show()
