import pandas as pd
import matplotlib.pyplot as plt
# /content/Train_WD_2.csv
<<<<<<< HEAD
df = pd.read_csv('Train_WD_2.csv')
=======
df = pd.read_csv('Train_WD_23.csv')3
>>>>>>> master

hrs = df['Hour']
cdr = df['CDR']
values = df['value']
listOfCDR = cdr.unique().tolist()
df['ratio'] = df['value']/df.groupby(['CDR', 'Hour'])['value'].transform('max')

for i in range(24):
<<<<<<< HEAD
    fig, ax = plt.subplots()
    for CDR in listOfCDR:
        condition = (hrs == i) & (cdr == CDR)
        df[condition].plot(x='index', y='ratio', label=CDR, ax=ax)
=======
    fig, ax = plt.subplots(figsize=(14,8))
    for CDR in listOfCDR:
        condition = (hrs == i) & (cdr == CDR)
        df[condition].plot(x='index', y='ratio', label=CDR, ax=ax)
    plt.ylim(0,1)
>>>>>>> master
    plt.legend()
    plt.title("Ratio values for Hour " + str(i))
    plt.show()