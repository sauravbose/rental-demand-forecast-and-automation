# -*- coding: utf-8 -*-
"""
Created on Fri Jun 23 09:40:07 2017

@author: bosesaur
"""

import pyodbc
import sqlite3
import pandas as pd
import numpy as np
import calendar


def start_connection(databasepath, dsn, oracle, sqlit):
    if oracle:
        conn_orc = pyodbc.connect(DSN = dsn , uid = "fanlinli", pwd = "jun2017avis")
        cursor_orc = conn_orc.cursor()
        return [conn_orc,cursor_orc]
    
    elif sqlit:
        conn_sqlit = sqlite3.connect(databasepath)
        cursor_sqlit = conn_sqlit.cursor()
        return [conn_sqlit, cursor_sqlit]
    

def update_priorday_fore(conn,forecast_today):
    priorday = pd.read_sql_query("select * from Todays_forecast",conn)
    if(priorday.equals(forecast_today) == False):
        print 'executing'
        writetosqlite(priorday,"Priordays_forecast",conn)
       

def capture_todays_forecast(conn, conn_sql):
    
    SQL_currentdayforecastcapture = "select LOC.COUNTRY, LOC.REGION, LOC.POOL, LOC.MARKET_GROUP,FCS.BRAND, FCS.DFP_SUPER_SEGMENT, \
    FCS.FCST_DATE, FCS.CURRENT_STRATEGY, FCS.PRODUCT_CATEGORY, \
    SUM(FCS.CURRENT_STRATEGY_DEMAND + FCS.FREESELL_UNINFL + FCS.YIELDABLE_UNINFL) AS CURR_STRAT_FCST,\
    SUM(FCS.STRATEGY_7_UNINFL + FCS.FREESELL_UNINFL + FCS.YIELDABLE_UNINFL) AS STRAT_7, \
    SUM(FCS.STRATEGY_7_UNINFL + FCS.INC_STRATEGY_6_UNINFL + FCS.INC_STRATEGY_5_UNINFL + \
    FCS.INC_STRATEGY_4_UNINFL + FCS.INC_STRATEGY_3_UNINFL + FCS.INC_STRATEGY_2_UNINFL + \
    FCS.INC_STRATEGY_1_UNINFL + FCS.FREESELL_UNINFL + FCS.YIELDABLE_UNINFL) AS STRAT_1, \
    SUM(TOTAL_DEMAND_TO_COME) AS TOTAL_DTC, SUM(FCS.FREESELL_UNINFL) AS FREESELL, \
    SUM(FCS.YIELDABLE_UNINFL) AS YIELDABLE, SUM(FCS.INC_STRATEGY_1_UNINFL) AS STRAT_1_INC, \
    SUM(FCS.INC_STRATEGY_2_UNINFL) AS STRAT_2_INC, SUM(FCS.INC_STRATEGY_3_UNINFL) AS STRAT_3_INC,\
    SUM(FCS.INC_STRATEGY_4_UNINFL) AS STRAT_4_INC, SUM(FCS.INC_STRATEGY_5_UNINFL) AS STRAT_5_INC, \
    SUM(FCS.INC_STRATEGY_6_UNINFL) AS STRAT_6_INC \
    FROM PPSS.PA_FCS_DS_RPT FCS INNER JOIN PPSS.PA_CHECKOUT_LOCATION LOC \
    ON FCS.DW_STATION = LOC.DW_STATION WHERE LOC.MARKET_GROUP IN ('JFK_T1','EWR_T1','MCO_T1','PHL_T1',\
    'IAH_T1','ORD_T1','MSY_T1','LAX_T1','SFO_T1','DFW_T1', \
    'MSP_T1','PDX_T1','PIT_T1','BNA_T1','IND_T1','SEA_T1','ABQ_T1','STL_T1','CVG_T1','CLE_T1') \
    AND LOC.NON_REV_INDICATOR = 0 AND LOC.OPEN_LOCATION_IND =1 \
    GROUP BY LOC.COUNTRY, LOC.REGION,LOC.POOL, \
    LOC.MARKET_GROUP,FCS.BRAND, FCS.DFP_SUPER_SEGMENT, \
    FCS.FCST_DATE, FCS.CURRENT_STRATEGY, FCS.PRODUCT_CATEGORY"
    

    d = pd.read_sql_query(SQL_currentdayforecastcapture,conn)
    
    null_ind = []
    for i in range(len(d)):
        if(pd.isnull(d["CURRENT_STRATEGY"][i])):
            null_ind.append(i)
            if(d["DFP_SUPER_SEGMENT"][i]=="GOVERNMENT"):
                d.loc[i,"CURRENT_STRATEGY"]=4
            elif(d["DFP_SUPER_SEGMENT"][i]=="LEISURE"): 
                if(d["PRODUCT_CATEGORY"][i]=="MONTHLY"):
                    d.loc[i,"CURRENT_STRATEGY"]=4
                else:
                    d.loc[i,"CURRENT_STRATEGY"]=5
    
    for i in null_ind:
        cs = int(d.loc[i,"CURRENT_STRATEGY"])
        dem = 0.0
        for j in range(cs,7):
            dem+=d.loc[i,"STRAT_%i_INC"%j]
        dem+=d.loc[i,"STRAT_7"]       
        d.loc[i,"CURR_STRAT_FCST"] = dem
        
    d.to_sql("prosdata_clean",conn_sql,if_exists='replace', index=False)
    
    sql_finaldata = "select prosdata_clean.COUNTRY, prosdata_clean.REGION,prosdata_clean.POOL, \
    prosdata_clean.MARKET_GROUP,prosdata_clean.BRAND,prosdata_clean.FCST_DATE,\
    SUM(prosdata_clean.CURR_STRAT_FCST) AS CURR_STRAT_FCST, SUM(prosdata_clean.STRAT_7) AS STRAT_7,\
    SUM(prosdata_clean.STRAT_1) AS STRAT_1 FROM prosdata_clean GROUP BY prosdata_clean.COUNTRY,\
    prosdata_clean.REGION,prosdata_clean.POOL,prosdata_clean.MARKET_GROUP,prosdata_clean.BRAND,\
    prosdata_clean.FCST_DATE"
    
      
    d = pd.read_sql(sql_finaldata,conn_sql)   
    
    for i in range(len(d)):
        d.loc[i,"CURR_STRAT_FCST"] = d.loc[i,"CURR_STRAT_FCST"].round(decimals = 5)
        d.loc[i,"STRAT_7"] = d.loc[i,"STRAT_7"].round(decimals = 5)
        d.loc[i,"STRAT_1"] = d.loc[i,"STRAT_1"].round(decimals = 5)

    start = datetime.datetime.today().strftime("%Y%m%d")
    end = datetime.datetime.strptime(start,"%Y%m%d")
    
    y = end.year
    m=end.month
    m+=1
    day = calendar.monthrange(y,m)[1]
    
    end = datetime.date(y,m,day)
    end = datetime.datetime.strftime(end,"%Y%m%d")
    
    d = d[(d['FCST_DATE'] > start) & (d['FCST_DATE'] < end)]
    
    return d


def compute_forecastdiff(conn_sqlit):
    SQL_forecastdiff = "Select Todays_forecast.COUNTRY, Todays_forecast.REGION, \
    Todays_forecast.POOL,Todays_forecast.MARKET_GROUP,\
    Todays_forecast.BRAND,Todays_forecast.FCST_DATE,Todays_forecast.CURR_STRAT_FCST AS\
    Todays_forecast, Priordays_forecast.CURR_STRAT_FCST AS Priordays_forecast,\
    Todays_forecast.STRAT_7 AS Todays_STRAT7, Priordays_forecast.STRAT_7 AS Priordays_STRAT7, \
    Todays_forecast.STRAT_1 AS Todays_STRAT1, Priordays_forecast.STRAT_1 AS Priordays_STRAT1, \
    SUM(Todays_forecast.CURR_STRAT_FCST - Priordays_forecast.CURR_STRAT_FCST)\
    AS CURR_Forecastdiff,  SUM(Todays_forecast.STRAT_7 - Priordays_forecast.STRAT_7)\
    AS STRAT_7_Forecastdiff, SUM(Todays_forecast.STRAT_1 - Priordays_forecast.STRAT_1)\
    AS STRAT_1_Forecastdiff FROM Priordays_forecast Inner Join Todays_forecast ON\
    (Priordays_forecast.POOL=Todays_forecast.POOL) AND\
    (Priordays_forecast.MARKET_GROUP=Todays_forecast.MARKET_GROUP) AND\
    (Priordays_forecast.BRAND=Todays_forecast.BRAND) AND \
    (Priordays_forecast.FCST_DATE=Todays_forecast.FCST_DATE) GROUP BY \
    Todays_forecast.COUNTRY, Todays_forecast.REGION, Todays_forecast.POOL, \
    Todays_forecast.MARKET_GROUP, Todays_forecast.BRAND,Todays_forecast.FCST_DATE, \
    Todays_forecast.CURR_STRAT_FCST, Priordays_forecast.CURR_STRAT_FCST,\
    Todays_forecast.STRAT_7, Priordays_forecast.STRAT_7, \
    Todays_forecast.STRAT_1, Priordays_forecast.STRAT_1"
    
    fore_diff = pd.read_sql_query(SQL_forecastdiff,conn_sqlit)
    
    writetosqlite(fore_diff,"Summary_forecastdiff",conn_sqlit)
    
    write_data("pros_forecastdiff.xlsx",fore_diff)


def daterange(conn_sqlit):
    start = pd.datetime(2007,1,1)
    end = pd.datetime(2017,12,31)
    rng = pd.date_range(start,end)
    
    dates = pd.DataFrame()
    dates["date"] = rng
    dates["date"]= dates["date"].apply(lambda x: x.date())    
    dates["date"]= dates["date"].apply(lambda x: x.strftime('%Y%m%d'))
     
    dates["month"] = pd.DatetimeIndex(dates["date"]).month
    dates["month"] = dates["month"].apply(lambda x: calendar.month_abbr[x])
    
    dates["year"] = pd.DatetimeIndex(dates["date"]).year
    
    writetosqlite(dates,"Dates",conn_sqlit)
        

def summarize_forecastdiff(conn_sqlit):
    SQL_finalforecastchange = "SELECT Summary_forecastdiff.COUNTRY,\
    Summary_forecastdiff.REGION,Summary_forecastdiff.POOL, Summary_forecastdiff.BRAND, \
    Dates.month, Dates.year,\
    Sum(Summary_forecastdiff.Priordays_forecast) AS Curr_Strat_Demand_Yesterday,\
    Sum(Summary_forecastdiff.Todays_forecast) AS Curr_Strat_Demand_Today,\
    Sum(Summary_forecastdiff.CURR_Forecastdiff) AS Curr_Strat_TotalChange,\
    Sum(Summary_forecastdiff.CURR_Forecastdiff)/Sum(Summary_forecastdiff.Priordays_forecast)\
    AS Curr_Strat_PercentChange FROM Summary_forecastdiff\
    INNER JOIN Dates ON Summary_forecastdiff.FCST_DATE = Dates.date\
    GROUP BY Summary_forecastdiff.COUNTRY, Summary_forecastdiff.REGION, \
    Summary_forecastdiff.POOL, Summary_forecastdiff.BRAND, Dates.month, Dates.year"
    
    sum_forecastdiff = pd.read_sql_query(SQL_finalforecastchange,conn_sqlit)
    writetosqlite(sum_forecastdiff,"Final_forecast_change",conn_sqlit)
    write_data("pros_final_forecast_change.xlsx",sum_forecastdiff)

def close_connection(conn):
    conn.close()


def writetosqlite(data,tablename,conn):
    data.to_sql(tablename,conn,if_exists='replace',index = False)
    
    

def write_data(filename,data):
    writer = pd.ExcelWriter(filename,engine = 'xlsxwriter')
    data.to_excel(writer,sheet_name = "Sheet1", index = False)
    writer.save()

if __name__=="__main__":
    
    [conn_orc,cursor_orc] = start_connection(None, "ProsUAT", oracle = True, sqlit = False)
    [conn_sqlit,cursor_sqlit] = start_connection(r"H:\pros_forecast_tracking.db", "ProsUAT", oracle = False, sqlit = True)
    
    #Capture today's forecast
    forecast_today = capture_todays_forecast(conn_orc,conn_sqlit)
    write_data("pros_todaysforecast.xlsx", forecast_today)
    close_connection(conn_orc)
    
           
    #Replace Prior day's forecast with Today's forecast
    try:
        update_priorday_fore(conn_sqlit,forecast_today)
    except:
        writetosqlite(forecast_today,"Todays_forecast",conn_sqlit)
        update_priorday_fore(conn_sqlit,forecast_today)
        
    
    #Write today's captured forecast to database
    writetosqlite(forecast_today,"Todays_forecast",conn_sqlit)
    
    #Compute forecast diff
    compute_forecastdiff(conn_sqlit)
    
    #Create date range
    daterange(conn_sqlit)
    
    #Compute forcast at appropriate level
    summarize_forecastdiff(conn_sqlit)
    
    close_connection(conn_sqlit)
    