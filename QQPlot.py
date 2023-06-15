import numpy as np
import statsmodels.api as sm
import scipy.stats as stats
from matplotlib import pyplot as plt
import pandas as pd

filename = "./Train_WD_2.csv"
df = pd.read_csv(filename)
data = np.genfromtxt(filename, delimiter=",",
                     skip_header=1, usecols=(0, 1, 2, 3))

cdr = np.unique(data[:, 2])
hours = np.unique(data[:, 1])

CDR = df['CDR']

for c in CDR.unique():
    cdr_data = []
    for hour in hours:
        cdr_hour_data = data[(df['CDR'] == c) & (data[:, 1] == hour), 3]
        cdr_data.append(cdr_hour_data)

    fig = sm.qqplot(np.array(cdr_data), stats.norm, fit=True, line="45")
    plt.title('Data via CDR : '+c)
<<<<<<< HEAD
    plt.show()
=======
    plt.show()
>>>>>>> master
