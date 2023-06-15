import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
import pandas as pd
filename = "./Train_WD_2.csv"
df = pd.read_csv(filename)
data = np.genfromtxt(filename, delimiter=",", skip_header=1, usecols=(0,1,2,3))
cdr = np.unique(data[:,2])
hours = np.unique(data[:,1])
CDR = df['CDR']


for c in CDR.unique():
    for hour in hours:
        fig = plt.figure()
        ax = fig.add_subplot(111)
        cdr_hour_data = data[(df['CDR'] == c) & (data[:, 1] == hour), 3]
        if cdr_hour_data.size == 0:
            continue
        stats.probplot(cdr_hour_data, dist="norm", plot=ax)
        plt.title(c + ' at Hour ' + str(int(hour)))
        plt.show()