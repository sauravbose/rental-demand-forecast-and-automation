# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 10:52:02 2017

@author: bosesaur
"""

import pyodbc
import sqlite3
import pandas as pd
import datetime
import numpy as np
import re


def capture_data(conn_sq):
    conn_orc = pyodbc.connect(DSN="ProsProd",uid = "bosesaur", pwd = "wel2tiger")
    
    sql_cy_hold = "select LOC.MARKET_GROUP, LOC.BRAND_TYPE, RES.CO_DATE, RES.TRXN_DATE, \
    BUS.DFP_SUPER_SEGMENT, RES.PRODUCT_CATEGORY, SUM(RES.ACTIVE_RES_FLAG) AS Active_Res,\
    SUM(RES.CANCEL_FLAG) AS Cancel_Flg,SUM(RES.NOSHOW_FLAG) AS Noshow_Flg \
    from PPSS.PA_CHECKOUT_LOCATION LOC INNER JOIN PPSS.PA_RESERVATION RES \
    ON RES.DW_STATION = LOC.DW_STATION INNER JOIN PPSS.PA_BUSINESS_SEGMENT BUS ON \
    BUS.DFP_PRICING_SEGMENT = RES.DFP_PRICING_SEGMENT WHERE LOC.MARKET_GROUP = 'PIT_T1' \
    AND BUS.DFP_SUPER_SEGMENT != 'ONE-WAY' GROUP BY LOC.MARKET_GROUP, LOC.BRAND_TYPE, RES.CO_DATE, RES.TRXN_DATE, BUS.DFP_SUPER_SEGMENT,\
    RES.PRODUCT_CATEGORY"  

    data = pd.read_sql(sql_cy_hold,conn_orc)

    sql_walkup = "select LOC.MARKET_GROUP, LOC.BRAND_TYPE, REN.CO_DATE, \
    SUM(REN.WALKUP_FLAG) AS WALKUP FROM PPSS.PA_CHECKOUT_LOCATION LOC INNER JOIN PPSS.PA_RENTAL REN \
    ON REN.DW_STATION = LOC.DW_STATION INNER JOIN PPSS.PA_BUSINESS_SEGMENT BUS ON \
    BUS.DFP_PRICING_SEGMENT = REN.DFP_PRICING_SEGMENT WHERE LOC.MARKET_GROUP = 'PIT_T1' \
    AND BUS.DFP_SUPER_SEGMENT != 'ONE-WAY' GROUP BY LOC.MARKET_GROUP, LOC.BRAND_TYPE, REN.CO_DATE"

    data_walk_all = pd.read_sql(sql_walkup,conn_orc)

    conn_orc.close()

    data["RES_COUNT"] = data["ACTIVE_RES"] - data["CANCEL_FLG"] - data["NOSHOW_FLG"] 
    data["CO_DATE"] = data["CO_DATE"].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d"))
    data["TRXN_DATE"] = data["TRXN_DATE"].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d"))
    data["TRXN_DATE"] = data["TRXN_DATE"].apply(lambda x: x.date())
    data.drop(["ACTIVE_RES","CANCEL_FLG","NOSHOW_FLG"], axis = 1, inplace = True)

    data.to_sql("All_Data",conn_sq,if_exists='replace')

    data_walk_all["CO_DATE"] = data_walk_all["CO_DATE"].apply(lambda x: datetime.datetime.strptime(x,"%Y%m%d"))

    
    return [data,data_walk_all]



def current_yr_data(data,data_walk_all):
    start_cy = datetime.datetime.strptime("20170601","%Y%m%d")
    end_cy = datetime.datetime.strptime("20170630","%Y%m%d")
    
    data_cy = data[(data["CO_DATE"] >= start_cy) & (data["CO_DATE"]<=end_cy)]
    data_cy = data_cy.reset_index()
    data_cy = data_cy.drop("index",1)
    data_cy["CO_DATE"] = data_cy["CO_DATE"].apply(lambda x: x.date())
    
    data_cy.to_sql("Current_Yr_Holdings",conn_sq,if_exists='replace')
    
    data_walk = data_walk_all[(data_walk_all["CO_DATE"] >= start_cy) & (data_walk_all["CO_DATE"]<=end_cy)]
    data_walk = data_walk.reset_index()
    data_walk = data_walk.drop("index",1)
    data_walk["CO_DATE"] = data_walk["CO_DATE"].apply(lambda x: x.date())
    
    data_walk.to_sql("Walkups",conn_sq,if_exists='replace')
    
    
    data_cy = pd.read_sql("select * from Current_Yr_Holdings", conn_sq)
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
    
    return [data_cy,data_walk,start_cy,end_cy,dates_cy,brand,dp]
              

def prior_yr_data(data,start_cy,end_cy):
    start_py = start_cy - datetime.timedelta(364)
    end_py = end_cy - datetime.timedelta(364)
    
    
    data_py = data[(data["CO_DATE"] >= start_py) & (data["CO_DATE"]<=end_py)]
    data_py = data_py.reset_index()
    data_py = data_py.drop("index",1)
    data_py["CO_DATE"] = data_py["CO_DATE"].apply(lambda x: x.date())
        
    data_py.to_sql("Prior_Yr_Holdings",conn_sq,if_exists='replace')
    
    data_py = pd.read_sql("select * from Prior_Yr_Holdings", conn_sq)
    
    header = data_py.columns
    newdata_py = pd.DataFrame(columns = header)
    dates_py = data_py["CO_DATE"].unique()
    brand = data_py["BRAND_TYPE"].unique()
    dp = [1,3,7,14]
    
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
    
    
def res_hold():
        
    sql_py = "select CO_DATE, BRAND_TYPE, MARKET_GROUP, SUM(RES_COUNT) AS PY_RES FROM \
    Prior_Yr_Holdings GROUP BY CO_DATE, BRAND_TYPE, MARKET_GROUP"
    
    p = pd.read_sql(sql_py,conn_sq)
    
    p["CO_DATE"] = p["CO_DATE"].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d"))
    p["CO_DATE"] = p["CO_DATE"].apply(lambda x: x.date())
    p["CO_DATE"] = p["CO_DATE"] + datetime.timedelta(364)
    
    p.to_sql("Prior_Res_Total",conn_sq, if_exists='replace')
       
    sql_cy = "select CO_DATE, BRAND_TYPE, MARKET_GROUP, SUM(RES_COUNT) AS CY_RES FROM \
    Current_Yr_Holdings GROUP BY CO_DATE, BRAND_TYPE, MARKET_GROUP"
    
    c = pd.read_sql(sql_cy,conn_sq)
    c.to_sql("Current_Res_Total",conn_sq, if_exists='replace')
    
def pros_forecast():
    forecast = pd.read_excel(r"H:\fcst_classification_data.xlsx")
    
    col = forecast.columns
    col = list(col)
    for i in range(len(col)):
        col[i]=col[i].replace(" ", "_")
        
    forecast.columns = col
    
    forecast["Co_Date"] = forecast["Co_Date"].apply(lambda x: x.date())
    forecast.to_sql("Forecast",conn_sq, if_exists='replace')
    
    fore_start = start_cy.date()
    fore_end = end_cy.date()
    
    f_cy = forecast[forecast["Mrkt_Grp"] == "PIT_T1"]
    f_cy = f_cy[(f_cy["Co_Date"]>=fore_start) & (f_cy["Co_Date"]<=fore_end)]
    f_cy = f_cy.reset_index()
    del f_cy["index"]
    
    f_cy.to_sql("CY_Fore",conn_sq,if_exists='replace')
    
    sql_fore = "select Co_Date, Brand, Mrkt_Grp, Run_Dp, SUM(Pros_Adj) As FQT_Forecast \
    from CY_Fore Group By Co_Date, Brand, Mrkt_Grp, Run_Dp"
    
    f_cy = pd.read_sql(sql_fore,conn_sq)
    
    f_cy.to_sql("CY_Fore",conn_sq,if_exists='replace')


def yoy_calc():
    sql_join1 = "select CY.CO_DATE, CY.BRAND_TYPE, CY.MARKET_GROUP, CY.Days_Prior, \
    PY.RES_COUNT AS PY_RES_HOLD, CY.RES_COUNT AS CY_RES_HOLD, PR.PY_RES, \
    CR.CY_RES, WK.WALKUP FROM Prior_yr_Res_Holdings PY, Curr_yr_Res_Holdings CY, \
    Prior_Res_Total PR , Current_Res_Total CR, Walkups WK WHERE  CY.CO_DATE=PY.CO_DATE AND \
    CY.CO_DATE=PR.CO_DATE AND   CY.CO_DATE=CR.CO_DATE AND CY.CO_DATE=WK.CO_DATE \
    AND CY.BRAND_TYPE = PY.BRAND_TYPE AND CY.BRAND_TYPE = PR.BRAND_TYPE AND \
    CY.BRAND_TYPE = CR.BRAND_TYPE AND CY.BRAND_TYPE = WK.BRAND_TYPE AND \
    CY.MARKET_GROUP=PY.MARKET_GROUP AND CY.MARKET_GROUP=PR.MARKET_GROUP AND \
    CY.MARKET_GROUP=CR.MARKET_GROUP AND CY.MARKET_GROUP=WK.MARKET_GROUP AND \
    CY.Days_Prior=PY.Days_Prior"
    joined_data = pd.read_sql(sql_join1,conn_sq)
    
    joined_data[r"YOY_Res_Growth_Perc"] = (joined_data["CY_RES_HOLD"]/joined_data["PY_RES_HOLD"]) - 1 
    joined_data["Adj_PY_Acutals(PROS_Fcst_Auto_Infl)"] = (1+joined_data["YOY_Res_Growth_Perc"])*joined_data["PY_RES"] + joined_data["WALKUP"]
    joined_data["CY_Actual"] = joined_data["CY_RES"]+joined_data["WALKUP"]
    joined_data[r"Perc_Error_of_Adj_PY_Acutals"] = (joined_data["CY_Actual"]-joined_data["Adj_PY_Acutals(PROS_Fcst_Auto_Infl)"]) /joined_data["CY_Actual"]
        
    joined_data.to_sql("Joined_Data",conn_sq,if_exists='replace')
    
    sql_join2 = "select J.*, C.FQT_Forecast From Joined_Data J Left Join CY_Fore C ON \
    J.CO_DATE = C.Co_Date AND J.BRAND_TYPE = C.Brand AND J.MARKET_GROUP = C.Mrkt_Grp AND \
    J.Days_Prior = C.Run_Dp"
    joined2 = pd.read_sql(sql_join2,conn_sq)
    
    del joined2["index"]
    joined2[r"Perc_Error_of_FQT_Forecast"] = (joined2["CY_Actual"]-joined2["FQT_Forecast"])/joined2["CY_Actual"]
    
    joined2.to_sql("Final_Forecast_Detailed",conn_sq,if_exists='replace')
    
    sql_yoy= "select CO_DATE AS Co_Date, BRAND_TYPE AS Brand, MARKET_GROUP AS Mrkt_Grp, \
    Days_Prior, YOY_Res_Growth_Perc, PY_RES_HOLD AS PY_Res_Hold, PY_RES AS PY_Res_Total, CY_RES_HOLD AS CY_Res_Hold, \
    WALKUP AS Walkups, [Adj_PY_Acutals(PROS_Fcst_Auto_Infl)], FQT_Forecast,  CY_Actual, Perc_Error_of_Adj_PY_Acutals, \
    Perc_Error_of_FQT_Forecast FROM Final_Forecast_Detailed ORDER BY Co_Date"
    
    yoy = pd.read_sql(sql_yoy,conn_sq)
    
    yoy.to_sql("YOY_Data",conn_sq,if_exists='replace')
    return yoy
   

def timeseries_detailed(data,dates_cy,brand,dp):
    data_timeseries = data.copy()
    data_timeseries["TRXN_DATE"] = data_timeseries["TRXN_DATE"].apply(lambda x: datetime.datetime.combine(x,datetime.time()))
    data_timeseries["CO_DATE"] = data_timeseries["CO_DATE"].apply(lambda x: datetime.datetime.combine(x,datetime.time()))
    
    timeseries = {}
    timeseries = dict.fromkeys(dates_cy)
    for i in timeseries:
        timeseries[i] = dict.fromkeys(brand)
        
    for i in timeseries:
        for j in timeseries[i]:
            timeseries[i][j] = dict.fromkeys(dp)
            
    for i in timeseries:
        for j in timeseries[i]:
            for k in timeseries[i][j]:
                timeseries[i][j][k] = []
                
                
    for i in dates_cy:
        for j in brand:
            for k in dp:
                dates = datetime.datetime.strptime(i,"%Y-%m-%d")
                end_final = dates 
                start_co = dates - datetime.timedelta(364)
                end_co = start_co + datetime.timedelta(56)
                start_trxn = start_co - datetime.timedelta(k)
                end_trxn = end_co - datetime.timedelta(k)
                while (end_co<=end_final):
                    d1 = data_timeseries[(data_timeseries["CO_DATE"]==start_co) & (data_timeseries["BRAND_TYPE"]==j) & (data_timeseries["TRXN_DATE"]<start_trxn)]
                    d1 = d1.reset_index()
                    count1 = d1["RES_COUNT"].sum()
                    d2 = data_timeseries[(data_timeseries["CO_DATE"]==end_co) & (data_timeseries["BRAND_TYPE"]==j) & (data_timeseries["TRXN_DATE"]<end_trxn)]
                    d2 = d2.reset_index()
                    count2 = d2["RES_COUNT"].sum()
                    growth = (count2/count1) - 1
                    timeseries[i][j][k].append(growth)
                    start_co += datetime.timedelta(28)
                    end_co += datetime.timedelta(28)
                    start_trxn += datetime.timedelta(28)
                    end_trxn += datetime.timedelta(28)
    for i in dates_cy:
       for j in brand:
           for k in dp:
                timeseries[i][j][k] = np.array(timeseries[i][j][k]).mean()
    
    timeseries_database = pd.DataFrame(columns = ["Co_Date","Brand","Days_Prior","Timeseries_Growth_Perc"])
    index = 0
    for i in timeseries:
        for j in timeseries[i]:
            for k in timeseries[i][j]:
                timeseries_database.loc[index,"Co_Date"]=i
                timeseries_database.loc[index,"Brand"] = j
                timeseries_database.loc[index,"Days_Prior"] = k
                timeseries_database.loc[index,"Timeseries_Growth_Perc"] = timeseries[i][j][k]
                index+=1
    
    timeseries_database.to_sql("Timeseries",conn_sq,if_exists='replace')
    
    sql = "select Trend_Adj.*, Timeseries.Timeseries_Growth_Perc from Trend_Adj, Timeseries WHERE Timeseries.Co_Date = Trend_Adj.Co_Date AND Timeseries.Brand = Trend_Adj.Brand AND Timeseries.Days_Prior = Trend_Adj.Days_Prior"
    timeseries_data = pd.read_sql(sql,conn_sq)
    timeseries_data["Timeseries_Forecast"] = (1+timeseries_data["Timeseries_Growth_Perc"])*timeseries_data["PY_Res_Total"] + timeseries_data["Walkups"]
   
    del timeseries_data["index"]
    timeseries_data[r"Perc_Error_of_Timeseries_Forecast"] = (timeseries_data["CY_Actual"]-timeseries_data["Timeseries_Forecast"])/timeseries_data["CY_Actual"]

    timeseries_data.to_sql("Timeseries",conn_sq,if_exists='replace')
            
    return timeseries_data

def trend_adj(data,dates_cy,brand,dp):
    data_trend_adj = data.copy()
    data_trend_adj["TRXN_DATE"] = data_trend_adj["TRXN_DATE"].apply(lambda x: datetime.datetime.combine(x,datetime.time()))
    data_trend_adj["CO_DATE"] = data_trend_adj["CO_DATE"].apply(lambda x: datetime.datetime.combine(x,datetime.time()))
    
    trend = {}
    trend = dict.fromkeys(dates_cy)
    for i in trend:
        trend[i] = dict.fromkeys(brand)
        
    for i in trend:
        for j in trend[i]:
            trend[i][j] = dict.fromkeys(dp)
            
    for i in trend:
        for j in trend[i]:
            for k in trend[i][j]:
                trend[i][j][k] = []
                
                
    for i in dates_cy:
        for j in brand:
            for k in dp:
                cy_date = datetime.datetime.strptime(i,"%Y-%m-%d")
                py_date = cy_date - datetime.timedelta(364)
                cy_strt_trxn = cy_date - datetime.timedelta(56)
                py_strt_trxn = py_date - datetime.timedelta(56)
                d1 = data_trend_adj[(data_trend_adj["CO_DATE"]==cy_date) & (data_trend_adj["BRAND_TYPE"]==j) & (data_trend_adj["TRXN_DATE"]>=cy_strt_trxn) & (data_trend_adj["TRXN_DATE"]<=cy_date)]
                d1 = d1.reset_index()
                count1 = d1["RES_COUNT"].sum()
                d2 = data_trend_adj[(data_trend_adj["CO_DATE"]==py_date) & (data_trend_adj["BRAND_TYPE"]==j) & (data_trend_adj["TRXN_DATE"]>=py_strt_trxn) & (data_trend_adj["TRXN_DATE"]<=py_date)]
                d2 = d2.reset_index()
                count2 = d2["RES_COUNT"].sum()
                growth = (count1/count2) - 1
                trend[i][j][k].append(growth)
                
    trend_database = pd.DataFrame(columns = ["Co_Date","Brand","Days_Prior","Trend_Adj_Growth_Perc"])
    index = 0
    for i in trend:
        for j in trend[i]:
            for k in trend[i][j]:
                trend_database.loc[index,"Co_Date"]=i
                trend_database.loc[index,"Brand"] = j
                trend_database.loc[index,"Days_Prior"] = k
                trend_database.loc[index,"Trend_Adj_Growth_Perc"] = trend[i][j][k]
                index+=1
    
    trend_database["Trend_Adj_Growth_Perc"] =  trend_database["Trend_Adj_Growth_Perc"].apply(lambda x: re.sub(r"[\[\]]","",str(x)))   
    trend_database["Trend_Adj_Growth_Perc"] = trend_database["Trend_Adj_Growth_Perc"].apply(lambda x: float(x))

    trend_database.to_sql("Trend_Adj",conn_sq,if_exists='replace')
    
    sql = "select YOY.*, TREND.Trend_Adj_Growth_Perc from YOY_Data YOY, Trend_Adj TREND WHERE TREND.Co_Date = YOY.Co_Date AND TREND.Brand = YOY.Brand AND TREND.Days_Prior = YOY.Days_Prior"
    trend_data = pd.read_sql(sql,conn_sq)
    trend_data["Trend_Adj_Forecast"] = (1+trend_data["Trend_Adj_Growth_Perc"])*trend_data["PY_Res_Total"] + trend_data["Walkups"]
   
    del trend_data["index"]
    trend_data[r"Perc_Error_of_Trend_Adj_Forecast"] = (trend_data["CY_Actual"]-trend_data["Trend_Adj_Forecast"])/trend_data["CY_Actual"]

    trend_data.to_sql("Trend_Adj",conn_sq,if_exists='replace')

    return trend_data                
    

def write_data():
    sql = "select Co_Date, Brand, Mrkt_Grp, Days_Prior, PY_Res_Hold, CY_Res_Hold , \
    CY_Actual, YOY_Res_Growth_Perc, [Adj_PY_Acutals(PROS_Fcst_Auto_Infl)], \
    Perc_Error_of_Adj_PY_Acutals, FQT_Forecast, Perc_Error_of_FQT_Forecast,  \
    Trend_Adj_Growth_Perc, Trend_Adj_Forecast, Perc_Error_of_Trend_Adj_Forecast, \
    Timeseries_Growth_Perc, Timeseries_Forecast, Perc_Error_of_Timeseries_Forecast \
    FROM Timeseries"
    
    data_to_write = pd.read_sql(sql,conn_sq)
    
    writer = pd.ExcelWriter(r"H:\Pitt_Final_Data3.xlsx")
    data_to_write.to_excel(writer,index=False)
    writer.save()
     

if __name__ == "__main__":
    conn_sq = sqlite3.connect("Pittsburgh.db")
    
    [data,data_walk_all] = capture_data(conn_sq)
    [data_cy,data_walk,start_cy,end_cy,dates_cy,brand,dp] = current_yr_data(data,data_walk_all)
    prior_yr_data(data,start_cy,end_cy)
    res_hold()
    pros_forecast()
    yoy = yoy_calc()
    
    
    
    trend_data = trend_adj(data,dates_cy,brand,dp)
    timeseries_data = timeseries_detailed(data,dates_cy,brand,dp)
    
    write_data()
    
    
    
    conn_sq.close()
    
    

    
    