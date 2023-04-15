#!/usr/bin/env python
# coding: utf-8

# ## Notebook 4
# 
# 
# 

# In[33]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# In[34]:


get_ipython().run_cell_magic('pyspark', '', "df = spark.read.load('abfss://test@debdatalakegen21.dfs.core.windows.net/synapse/workspaces/debadmin/warehouse/nyctaxi.db/nyc_taxi_holiday_weather/part-00000-0169cef3-e225-4f61-bca2-e8496c24c874-c000.snappy.parquet', format='parquet')\r\ndisplay(df.limit(10))\n")


# In[35]:


#Data statistics overview
df.summary().show()


# In[36]:


#Conversion to pandas data frame and do data plotting
pd_df = df.toPandas()


# In[37]:


#	Data Exploratory analysis
val1 = pd_df['tipAmount'].plot(kind='hist', bins=25, facecolor='red')
val1.set_title('Tip dollar distribution')
val1.set_xlabel('Tip in us dollar')
val1.set_ylabel('Number of tips')
plt.suptitle('')
plt.show()


# In[39]:


# Find the correlation between Passenger count and Trip Amount
df.stat.corr("tripDistance","tipAmount")


# In[40]:


# Visualize the relationship between Distance and tip amounts

var2 = pd_df.plot(kind='scatter', x= 'tripDistance', y = 'tipAmount', c='red', alpha = 0.20, s=2.5*(pd_df['passengerCount']))
var2.set_title('Tip amount by Distance')
var2.set_xlabel('Distance Travelled')
var2.set_ylabel('Tip Amount in dollar')
plt.axis([-2, 90, -2, 30])
plt.suptitle('')
plt.show()


