# -*- coding: utf-8 -*-
"""
Created on Tue Jun 13 14:27:21 2017

@author: bosesaur
"""

import pyodbc
import sqlite3
import pandas as pd
import numpy as np
import calendar


def start_connection(databasepath, dsn, oracle, sqlit):
    if oracle:
        conn_orc = pyodbc.connect(DSN = dsn , uid = "fanlinli", pwd = "Nino0617")
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
       

def capture_todays_forecast(conn):
    
    SQL_currentdayforecastcapture = "SELECT EP.EntityName, Div.Division, YP.YieldPool,\
    YL.YieldLocation, YP.PoolDescription, YL.LocationDescription, LF.CheckOutDate, \
    DW.DowCode, DW.DayOfWeekDesc, LF.Forecast \
    FROM YieldLocation YL, YieldPool YP, LocationForecast LF, EntityPool EP, DayOfWeek DW,\
    Division Div, LocationBrand LB,  yld_pros_frcst_mait Flag \
    WHERE (YP.YieldPool=YL.YieldPool AND LF.YieldLocation=YL.YieldLocation \
    AND EP.YieldPool=YP.YieldPool AND DW.DowCode=LF.DowCode AND Div.Division=YP.Division \
    AND YL.YieldLocation=LB.yld_loc_cd)  AND (Div.Division NOT  IN ('AU', 'NZ') \
    AND (LB.brand = 'A')) and Flag.yld_pool_desc=YP.PoolDescription and Flag.forecast_flag='Y'\
    ORDER BY LF.CheckOutDate, EP.EntityName, Div.Division, YP.PoolDescription, \
    YL.LocationDescription"
    

    data = pd.read_sql_query(SQL_currentdayforecastcapture,conn)
    data["CheckOutDate"]= data["CheckOutDate"].apply(lambda x: x.date())  
    for i in range(len(data["CheckOutDate"])):
        data.loc[i,"CheckOutDate"] = unicode(data.loc[i,"CheckOutDate"])
    
    return data


def compute_forecastdiff(conn_sqlit):
    SQL_forecastdiff = "Select Todays_forecast.EntityName, Todays_forecast.Division, \
    Todays_forecast.PoolDescription,Todays_forecast.LocationDescription,\
    Todays_forecast.CheckOutDate,Todays_forecast.DayOfWeekDesc,Todays_forecast.Forecast AS\
    Todays_forecast, Priordays_forecast.Forecast AS Priordays_forecast,\
    SUM(Todays_forecast.Forecast - Priordays_forecast.Forecast)\
    AS Forecastdiff FROM Priordays_forecast Inner Join Todays_forecast ON\
    (Priordays_forecast.PoolDescription=Todays_forecast.PoolDescription) AND\
    (Priordays_forecast.LocationDescription=Todays_forecast.LocationDescription) AND\
    (Priordays_forecast.CheckOutDate=Todays_forecast.CheckOutDate) GROUP BY \
    Todays_forecast.EntityName, Todays_forecast.Division, \
    Todays_forecast.PoolDescription,Todays_forecast.LocationDescription,\
    Todays_forecast.CheckOutDate,Todays_forecast.DayOfWeekDesc,\
    Todays_forecast.Forecast,Priordays_forecast.Forecast"
    
    fore_diff = pd.read_sql_query(SQL_forecastdiff,conn_sqlit)
    
    writetosqlite(fore_diff,"Summary_forecastdiff",conn_sqlit)
    
    write_data("forecastdiff.xlsx",fore_diff)


def daterange(conn_sqlit):
    start = pd.datetime(2007,1,1)
    end = pd.datetime(2017,12,31)
    rng = pd.date_range(start,end)
    
    dates = pd.DataFrame()
    dates["date"] = rng
    dates["date"]= dates["date"].apply(lambda x: x.date())    
    
    dates["month"] = pd.DatetimeIndex(dates["date"]).month
    dates["month"] = dates["month"].apply(lambda x: calendar.month_abbr[x])
    
    dates["year"] = pd.DatetimeIndex(dates["date"]).year
    
    writetosqlite(dates,"Dates",conn_sqlit)
        

def summarize_forecastdiff(conn_sqlit):
    SQL_finalforecastchange = "SELECT Summary_forecastdiff.EntityName,\
    Summary_forecastdiff.PoolDescription, Dates.month, Dates.year,\
    Sum(Summary_forecastdiff.Priordays_forecast) AS Demand_Yesterday,\
    Sum(Summary_forecastdiff.Todays_forecast) AS Demand_Today,\
    Sum(Summary_forecastdiff.Forecastdiff) AS TotalChange,\
    Sum(Summary_forecastdiff.Forecastdiff)/Sum(Summary_forecastdiff.Priordays_forecast)\
    AS PercentChange FROM Summary_forecastdiff\
    INNER JOIN Dates ON Summary_forecastdiff.CheckOutDate = Dates.date\
    GROUP BY Summary_forecastdiff.Entityname, Summary_forecastdiff.Pooldescription,\
    Dates.month, Dates.year"
    
    sum_forecastdiff = pd.read_sql_query(SQL_finalforecastchange,conn_sqlit)
    writetosqlite(sum_forecastdiff,"Final_forecast_change",conn_sqlit)
    write_data("final_forecast_change.xlsx",sum_forecastdiff)

def close_connection(conn):
    conn.close()


def writetosqlite(data,tablename,conn):
    data.to_sql(tablename,conn,if_exists='replace',index = False)
    
    

def write_data(filename,data):
    writer = pd.ExcelWriter(filename,engine = 'xlsxwriter')
    data.to_excel(writer,sheet_name = "Sheet1", index = False)
    writer.save()

if __name__=="__main__":
    
    [conn_orc,cursor_orc] = start_connection(None, "Yieldprod", oracle = True, sqlit = False)
    
    #Capture today's forecast
    forecast_today = capture_todays_forecast(conn_orc)
    write_data("todaysforecast.xlsx", forecast_today)
    close_connection(conn_orc)
    
    [conn_sqlit,cursor_sqlit] = start_connection(r"H:\forecast_tracking.db", "Yieldprod", oracle = False, sqlit = True)
    
        
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
    
    
    
    