# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 11:27:35 2017

@author: bosesaur
"""

import pyodbc
import sqlite3
import pandas as pd

c_orc = pyodbc.connect(DSN = "AvisDW",usd = "pricing", pwd = "pricing")

sql = "select LOC.POOL AS Pool, LOC.MARKET_GROUP AS Mrkt_Grp, LOC.BRAND AS Brand, \
RES.CO_DATE AS Co_Date, RES.CO_TIME AS Co_Time, RES.RES_COUNT AS Res_Count, \
RES.PRICE_QUOTE_AMT_USD AS Price_Quote, RES.TOUR_VOUCH_PREPMT_AMT_USD AS Tour_Vouch \
from RESUSER.RES_FACT RES INNER JOIN PRICING.DD_CO_LOC LOC ON \
RES.DW_CO_LOC_ID = LOC.DW_STATION WHERE LOC.MARKET_GROUP IN ('JFK_T1','EWR_T1','MCO_T1','PHL_T1')\
AND RES.RES_STATUS = 'AR' AND \
RES.CO_DATE BETWEEN to_date('27-06-2016','dd-mm-YYYY') AND to_date('27-06-2017','dd-mm-YYYY')"

data = pd.read_sql_query(sql,c_orc)


c_orc.close()

c_sq = sqlite3.connect(r"H:\data_warehouse.db")

data.to_sql("Raw_Capture",c_sq, if_exists = 'replace', index = False)

datatemp = data.copy()
datatemp.loc[datatemp['PRICE_QUOTE'] != 0, 'REVENUE'] = datatemp['PRICE_QUOTE']
datatemp.loc[datatemp['PRICE_QUOTE'] == 0, 'REVENUE'] = datatemp['TOUR_VOUCH']

del datatemp["PRICE_QUOTE"]
del datatemp["TOUR_VOUCH"]

datatemp.to_sql("Refined_Data",c_sq,if_exists = 'replace', index = False)

d = pd.read_sql_query("select * from Refined_Data Order By REVENUE DESC",c_sq)
d["Cumulative_sum"] = d.REVENUE.cumsum()
d["Cumulative_perc"] = 100*d.Cumulative_sum/d.REVENUE.sum()
d["Cumulative_Res_Count"] = d.RES_COUNT.cumsum()

col = d.columns
col = list(col)
ind = col.index('RES_COUNT')
ind2 = col.index('Cumulative_Res_Count')
col.insert(ind+1,col.pop(ind2))
d = d[col]

d.to_sql("Cumulative_Data",c_sq,if_exists='replace', index = False)

writer = pd.ExcelWriter("perc.xlsx")
d.to_excel(writer)
writer.save()

c_sq.close()