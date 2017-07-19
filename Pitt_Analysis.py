# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 10:52:02 2017

@author: bosesaur
"""

import pyodbc
import sqlite3
import pandas as pd
import datetime

conn_orc = pyodbc.connect(DSN="",uid = "", pwd = "")

sql_cy_hold = "select LOC.MARKET_GROUP, LOC.BRAND_TYPE, RES.CO_DATE, RES.TRXN_DATE, \
BUS.DFP_SUPER_SEGMENT, RES.PRODUCT_CATEGORY, SUM(RES.ACTIVE_RES_FLAG) AS Active_Res,\
SUM(RES.CANCEL_FLAG) AS Cancel_Flg,SUM(RES.NOSHOW_FLAG) AS Noshow_Flg \
from PPSS.PA_CHECKOUT_LOCATION LOC INNER JOIN PPSS.PA_RESERVATION RES \
ON RES.DW_STATION = LOC.DW_STATION INNER JOIN PPSS.PA_BUSINESS_SEGMENT BUS ON \
BUS.DFP_PRICING_SEGMENT = RES.DFP_PRICING_SEGMENT WHERE LOC.MARKET_GROUP = 'PIT_T1' \
GROUP BY LOC.MARKET_GROUP, LOC.BRAND_TYPE, RES.CO_DATE, RES.TRXN_DATE, BUS.DFP_SUPER_SEGMENT,\
RES.PRODUCT_CATEGORY"  

data = pd.read_sql(sql_cy_hold,conn_orc)

sql_walkup = "select LOC.MARKET_GROUP, LOC.BRAND_TYPE, REN.CO_DATE, \
SUM(REN.WALKUP_FLAG) AS WALKUP FROM PPSS.PA_CHECKOUT_LOCATION LOC INNER JOIN PPSS.PA_RENTAL REN \
ON REN.DW_STATION = LOC.DW_STATION WHERE LOC.MARKET_GROUP = 'PIT_T1' GROUP BY LOC.MARKET_GROUP, \
LOC.BRAND_TYPE, REN.CO_DATE"

data_walk = pd.read_sql(sql_walkup,conn_orc)

conn_orc.close()

data["RES_COUNT"] = data["ACTIVE_RES"] - data["CANCEL_FLG"] - data["NOSHOW_FLG"] 
data["CO_DATE"] = data["CO_DATE"].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d"))

data["TRXN_DATE"] = data["TRXN_DATE"].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d"))
data["TRXN_DATE"] = data["TRXN_DATE"].apply(lambda x: x.date())

data.drop(["ACTIVE_RES","CANCEL_FLG","NOSHOW_FLG"], axis = 1, inplace = True)

start_cy = datetime.datetime.strptime("20170601","%Y%m%d")
end_cy = datetime.datetime.strptime("20170630","%Y%m%d")

data_cy = data[(data["CO_DATE"] >= start_cy) & (data["CO_DATE"]<=end_cy)]
data_cy = data_cy.reset_index()
data_cy = data_cy.drop("index",1)
data_cy["CO_DATE"] = data_cy["CO_DATE"].apply(lambda x: x.date())

data_walk = data_walk[(data_walk["CO_DATE"] >= start_cy) & (data_walk["CO_DATE"]<=end_cy)]
data_walk = data_walk.reset_index()
data_walk = data_walk.drop("index",1)
data_walk["CO_DATE"] = data_walk["CO_DATE"].apply(lambda x: x.date())


conn_sq = sqlite3.connect("Pittsburgh.db")

data_cy.to_sql("Current_Yr_Holdings",conn_sq,if_exists='replace')


data_cy = pd.read_sql("select * from Current_Yr_Holdings", conn_sq)

writer = pd.ExcelWriter("pitt.xlsx")
data_cy.to_excel(writer,index=False)
writer.save()

header = data_cy.columns
newdata = pd.DataFrame(columns = header)
dates_cy = data_cy["CO_DATE"].unique()
brand = data_cy["BRAND_TYPE"].unique()
dp = [1,3,7,14]

index = 0
count = 0

for i in brand:
    for j in dates_cy:
        for k in dp:
            dat = datetime.datetime.strptime(j,"%Y-%m-%d")
            dat = dat.date() 
            trxndat = dat - datetime.timedelta(k)
            trxndat = datetime.datetime.strftime(trxndat,"%Y-%m-%d")
            d = data_cy[(data_cy["CO_DATE"]==j) & (data_cy["BRAND_TYPE"]==i) & (data_cy["TRXN_DATE"]<trxndat)]
            d = d.reset_index()
            count = d["RES_COUNT"].sum()
            newdata = newdata.append(d.loc[0,:], ignore_index = True)
            newdata.loc[index,"RES_COUNT"] = count
            newdata.loc[index,"Days_Prior"] = k
            index+=1 


del newdata["index"]  
del newdata["level_0"]
newdata.to_sql("Curr_yr_Res_Holdings",conn_sq, if_exists = 'replace')
          


start_py = start_cy - datetime.timedelta(364)
end_py = end_cy - datetime.timedelta(364)


data_py = data[(data["CO_DATE"] >= start_py) & (data["CO_DATE"]<=end_py)]
data_py = data_py.reset_index()
data_py = data_py.drop("index",1)


data_py["CO_DATE"] = data_py["CO_DATE"].apply(lambda x: x.date())


data_py.to_sql("Prior_Yr_Holdings",conn_sq,if_exists='replace')

data_py = pd.read_sql("select * from Prior_Yr_Holdings", conn_sq)

newdata_py = pd.DataFrame(columns = header)

dates_py = data_py["CO_DATE"].unique()

index = 0
count = 0

for i in brand:
    for j in dates_py:
        for k in dp:
            dat = datetime.datetime.strptime(j,"%Y-%m-%d")
            dat = dat.date() 
            trxndat = dat - datetime.timedelta(k)
            trxndat = datetime.datetime.strftime(trxndat,"%Y-%m-%d")
            d = data_py[(data_py["CO_DATE"]==j) & (data_py["BRAND_TYPE"]==i) & (data_py["TRXN_DATE"]<trxndat)]
            d = d.reset_index()
            count = d["RES_COUNT"].sum()
            newdata_py = newdata_py.append(d.loc[0,:], ignore_index = True)
            newdata_py.loc[index,"RES_COUNT"] = count
            newdata_py.loc[index,"Days_Prior"] = k
            index+=1
            

del newdata_py["index"]  
del newdata_py["level_0"]

newdata_py["CO_DATE"] = newdata_py["CO_DATE"].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d"))
newdata_py["CO_DATE"] = newdata_py["CO_DATE"].apply(lambda x: x.date())
newdata_py["CO_DATE"] = newdata_py["CO_DATE"] + datetime.timedelta(364)
newdata_py.to_sql("Prior_yr_Res_Holdings",conn_sq, if_exists = 'replace')

sql_py = "select CO_DATE, BRAND_TYPE, MARKET_GROUP, SUM(RES_COUNT) AS PY_RES FROM Prior_Yr_Holdings GROUP BY CO_DATE, BRAND_TYPE, MARKET_GROUP"

p = pd.read_sql(sql_py,conn_sq)
p["CO_DATE"] = p["CO_DATE"].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d"))
p["CO_DATE"] = p["CO_DATE"].apply(lambda x: x.date())
p["CO_DATE"] = p["CO_DATE"] + datetime.timedelta(364)

p.to_sql("Prior_Res_Total",conn_sq, if_exists='replace')

sql_cy = "select CO_DATE, BRAND_TYPE, MARKET_GROUP, SUM(RES_COUNT) AS CY_RES FROM Current_Yr_Holdings GROUP BY CO_DATE, BRAND_TYPE, MARKET_GROUP"

c = pd.read_sql(sql_cy,conn_sq)
c.to_sql("Current_Res_Total",conn_sq, if_exists='replace')



sql_join1 = "select CY.CO_DATE, CY.BRAND_TYPE, CY.MARKET_GROUP, CY.Days_Prior, PY.RES_COUNT AS PY_RES_HOLD, CY.RES_COUNT AS CY_RES_HOLD, PR.PY_RES, CR.CY_RES, WK.WALKUP FROM Prior_yr_Res_Holdings PY, Curr_yr_Res_Holdings CY, Prior_Res_Total PR , Current_Res_Total CR, Walkups WK WHERE  CY.CO_DATE=PY.CO_DATE AND CY.CO_DATE=PR.CO_DATE AND   CY.CO_DATE=CR.CO_DATE AND CY.CO_DATE=WK.CO_DATE AND CY.BRAND_TYPE = PY.BRAND_TYPE AND CY.BRAND_TYPE = PR.BRAND_TYPE AND CY.BRAND_TYPE = CR.BRAND_TYPE AND CY.BRAND_TYPE = WK.BRAND_TYPE AND CY.MARKET_GROUP=PY.MARKET_GROUP AND CY.MARKET_GROUP=PR.MARKET_GROUP AND CY.MARKET_GROUP=CR.MARKET_GROUP AND CY.MARKET_GROUP=WK.MARKET_GROUP AND CY.Days_Prior=PY.Days_Prior"
joined_data = pd.read_sql(sql_join1,conn_sq)
joined_data[r"YOY_Res_Growth_%"] = (joined_data["CY_RES_HOLD"]/joined_data["PY_RES_HOLD"]) - 1 

joined_data["Adj_PY_Acutals(PROS_Fcst_Auto_Infl)"] = (1+joined_data["YOY_Res_Growth_%"])*joined_data["PY_RES"] + joined_data["WALKUP"]

joined_data["CY_Actual"] = joined_data["CY_RES"]+joined_data["WALKUP"]

joined_data[r"%_Error_of_Adj_PY_Acutals"] = (joined_data["CY_Actual"]/joined_data["Adj_PY_Acutals(PROS_Fcst_Auto_Infl)"]) - 1


joined_data.to_sql("Joined_Data",conn_sq,if_exists='replace')

writer = pd.ExcelWriter(r"H:\newpitts.xlsx")
joined_data.to_excel(writer,index=False)
writer.save()


conn_sq.close()
