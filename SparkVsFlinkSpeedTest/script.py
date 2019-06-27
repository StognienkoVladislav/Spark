import numpy as np
import time
import pandas as pd


df = pd.read_csv('data_sets/vgsales.csv')

start_time = time.time()
check = df[(df['Year'] > 2004) & (df['Year'] < 2016) &
           (df['NA_Sales']/df['Global_Sales'] > 0.5) & (df['Genre'] == 'Action')]
print("--- %s seconds ---" % (time.time() - start_time))

print(check.head())
