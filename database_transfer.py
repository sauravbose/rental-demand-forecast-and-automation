# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 14:59:47 2017

@author: bosesaur
"""

import pyodbc
import numpy as np
import pandas as pd

#Establish connection with Oracle database
conn_oracle = pyodbc.connect(DSN = "Database", uid = "", pwd = "")
cursor = conn_oracle.cursor()

#Retrieve data from Oracle using SQL
sql = "select Division.Division, YieldPool.YieldPool from Division, YieldPool"
cursor.execute(sql)
res = cursor.fetchall()

conn_oracle.close()

#Wrangle data into an Access acceptable form using numpy and pandas
res = [list(elem) for elem in res]
res = np.array(res)

column_names = []
for i in range(len(cursor.description)):
    column_names.append(cursor.description[i][0])
    
res = pd.DataFrame(res,columns=column_names)

'''The shortcut to the entire data wrangling process is :
   res = pd.read_sql_query(sql,conn_oracle)''' 

#write the dataframe to MS Excel
writer = pd.ExcelWriter("H:\oracle_data.xlsx", engine = "xlsxwriter")
res.to_excel(writer, sheet_name = "Oracle", index = False)
writer.save()

#write the dataframe to MS Access

