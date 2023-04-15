#!/usr/bin/env python
# coding: utf-8

# ## Notebook 2
# 
# 
# 

# In[29]:


##Follow book chapter 5 on how to leverage azure open dataset, change abfss endpoint(replace yyyy and xxx with your storage endpoint,also path need to be replaced)
get_ipython().run_line_magic('%pyspark', '')
df = spark.read.load('abfss://test@debdatalakegen21.dfs.core.windows.net/synapse/workspaces/debadmin/warehouse/nyctaxi.db/nyc_taxi_holiday_weather/part-00000-0169cef3-e225-4f61-bca2-e8496c24c874-c000.snappy.parquet', format='parquet')
display(df.limit(10))



# In[ ]:


df.groupBy("vendorID").max().show()


# In[30]:


df.agg({'tipAmount':'sum'}).show()



# In[31]:


columns_to_remove = ["holidayName"]
df_new = df.select([column for column in df.columns if column not in columns_to_remove])


# In[32]:


display(df_new.limit(10))


# In[33]:


pip install seaborn


# In[34]:


pip install numpy


# In[35]:


pip install yellowbrick


# In[36]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# In[37]:


df.summary().show()


# In[38]:


pd_df =  df.toPandas()


# In[39]:


val1 = pd_df['tipAmount'].plot(kind='hist', bins=25, facecolor='red')
val1.set_title('Tip dollar distribution')
val1.set_xlabel('Tip in us dollar')
val1.set_ylabel('Number of tips')
plt.suptitle('')
plt.show()


# In[40]:


df.stat.corr("tripDistance","tipAmount")

