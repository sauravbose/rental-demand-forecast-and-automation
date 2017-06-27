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
import datetime
import win32com.client
import string


def start_connection(databasepath, dsn, oracle, sqlit):
    if oracle:
        conn_orc = pyodbc.connect(DSN = dsn , uid = "", pwd = "")
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
        d.loc[i,"CURR_STRAT_FCST"] = d.loc[i,"CURR_STRAT_FCST"].round(decimals = 0)
        d.loc[i,"STRAT_7"] = d.loc[i,"STRAT_7"].round(decimals = 0)
        d.loc[i,"STRAT_1"] = d.loc[i,"STRAT_1"].round(decimals = 0)

    start = datetime.datetime.today().strftime("%Y%m%d")
    end = datetime.datetime.strptime(start,"%Y%m%d")
    
    y = end.year
    m=end.month
    m+=1
    day = calendar.monthrange(y,m)[1]
    
    end = datetime.date(y,m,day)
    end = datetime.datetime.strftime(end,"%Y%m%d")
    
    d = d[(d['FCST_DATE'] > start) & (d['FCST_DATE'] < end)]
    d = d.reset_index()
    d = d.drop("index",1)
    
    return d


def compute_forecastdiff(conn_sqlit):
    region_dict = {"CENTRAL REGION":"CENT", "NORTHEAST REGION":"NE", "SOUTHEAST REGION":"SE", "WESTERN REGION":"WEST"}

    
    SQL_forecastdiff = "Select Todays_forecast.COUNTRY AS Country, Todays_forecast.REGION AS Region, \
    Todays_forecast.POOL AS Pool,Todays_forecast.MARKET_GROUP AS Mrkt_Grp,\
    Todays_forecast.BRAND AS Brand,Todays_forecast.FCST_DATE AS Co_Date, \
    Todays_forecast.CURR_STRAT_FCST AS Dmnd_Today, \
    Priordays_forecast.CURR_STRAT_FCST AS Dmnd_Yesterday,\
    SUM(Todays_forecast.CURR_STRAT_FCST - Priordays_forecast.CURR_STRAT_FCST) AS Total_Change,\
    Todays_forecast.STRAT_1 AS Dmnd_Today_S1, Priordays_forecast.STRAT_1 AS Dmnd_Yesterday_S1, \
    SUM(Todays_forecast.STRAT_1 - Priordays_forecast.STRAT_1) AS Total_Change_S1, \
    Todays_forecast.STRAT_7 AS Dmnd_Today_S7, Priordays_forecast.STRAT_7 AS Dmnd_Yesterday_S7, \
    SUM(Todays_forecast.STRAT_7 - Priordays_forecast.STRAT_7) AS Total_Change_S7 \
    FROM Priordays_forecast Inner Join Todays_forecast ON\
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
    
    fore_diff_excel = fore_diff.copy()
    for i in range(len(fore_diff_excel)):
        fore_diff_excel.loc[i,"Region"] = region_dict[fore_diff_excel.loc[i,"Region"]]
        fore_diff_excel.loc[i,"Co_Date"] = datetime.datetime.strptime(fore_diff_excel.loc[i,"Co_Date"],"%Y%m%d")
        fore_diff_excel.loc[i,"Co_Date"] = datetime.datetime.strftime(fore_diff_excel.loc[i,"Co_Date"],"%m/%d/%Y")
    
    #write_data("pros_forecastdiff.xlsx","Detailed",fore_diff)
    write_data(writer,"Detailed",fore_diff_excel)
    
    num2alpha = dict(zip(range(1, 27), string.ascii_uppercase))
    workbook  = writer.book
    worksheet = writer.sheets["Detailed"]
    worksheet.autofilter('A1:{0}{1}'.format(num2alpha[len(fore_diff.columns)],len(fore_diff)))


def daterange(conn_sqlit):
    start = pd.datetime(2007,1,1)
    end = pd.datetime(2017,12,31)
    rng = pd.date_range(start,end)
    
    dates = pd.DataFrame()
    dates["Date"] = rng
    dates["Date"]= dates["Date"].apply(lambda x: x.date())    
    dates["Date"]= dates["Date"].apply(lambda x: x.strftime('%Y%m%d'))
     
    dates["Month"] = pd.DatetimeIndex(dates["Date"]).month
    dates["Month"] = dates["Month"].apply(lambda x: calendar.month_abbr[x])
    
    dates["Year"] = pd.DatetimeIndex(dates["Date"]).year
    
    writetosqlite(dates,"Dates",conn_sqlit)
        

def summarize_forecastdiff(conn_sqlit):
    
    region_dict = {"CENTRAL REGION":"CENT", "NORTHEAST REGION":"NE", "SOUTHEAST REGION":"SE", "WESTERN REGION":"WEST"}
    month_dict = {"Jan":"[1]JAN","Feb":"[2]FEB","Mar":"[3]MAR", "Apr":"[4]APR","May":"[5]MAY","Jun":"[6]JUN", "Jul":"[7]JUL", "Aug":"[8]AUG", "Sep":"[9]SEP", "Oct":"[10]OCT", "Nov":"[11]NOV", "Dec":"[12]DEC"}
    
    SQL_finalforecastchange = "SELECT Summary_forecastdiff.Country,\
    Summary_forecastdiff.Region,Summary_forecastdiff.Pool, \
    Summary_forecastdiff.Brand, Dates.Month, Dates.Year,\
    Sum(Summary_forecastdiff.Dmnd_Yesterday) AS Dmnd_Yesterday,\
    Sum(Summary_forecastdiff.Dmnd_Today) AS Dmnd_Today,\
    Sum(Summary_forecastdiff.Total_Change) AS Total_Change,\
    Sum(Summary_forecastdiff.Total_Change)/Sum(Summary_forecastdiff.Dmnd_Yesterday)\
    AS Perc_Change FROM Summary_forecastdiff\
    INNER JOIN Dates ON Summary_forecastdiff.Co_Date = Dates.Date\
    GROUP BY Summary_forecastdiff.Country, Summary_forecastdiff.Region, \
    Summary_forecastdiff.Pool, Summary_forecastdiff.Brand, Dates.Month, Dates.Year"
    
    sum_forecastdiff = pd.read_sql_query(SQL_finalforecastchange,conn_sqlit)
    
    for i in range(len(sum_forecastdiff)):
        sum_forecastdiff.loc[i,"Perc_Change"] = sum_forecastdiff.loc[i,"Perc_Change"].round(decimals = 2)
   
   
    for i in range(len(sum_forecastdiff)):
        sum_forecastdiff.loc[i,"Region"] = region_dict[sum_forecastdiff.loc[i,"Region"]]
        sum_forecastdiff.loc[i,"Month"] =  month_dict[sum_forecastdiff.loc[i,"Month"]]
             
    writetosqlite(sum_forecastdiff,"Final_forecast_change",conn_sqlit)
    write_data(writer,"Final",sum_forecastdiff)
    
    num2alpha = dict(zip(range(1, 27), string.ascii_uppercase))
    workbook  = writer.book
    worksheet = writer.sheets["Final"]
    format2 = workbook.add_format({'num_format': '0%'})
    worksheet.set_column('J:J', None, format2)
    worksheet.autofilter('A1:{0}{1}'.format(num2alpha[len(sum_forecastdiff.columns)],len(sum_forecastdiff)))

def close_connection(conn):
    conn.close()


def writetosqlite(data,tablename,conn):
    data.to_sql(tablename,conn,if_exists='replace',index = False)
    
    

def write_data(writer,sheetname,data):
    
    data.to_excel(writer,sheet_name = sheetname, index = False)
    

def format_excel():
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(r'H:\pros_forecast_change_tracking.xlsx')
    ws1 = wb.Worksheets("Final")
    ws1.Columns.AutoFit()
    ws2 = wb.Worksheets("Detailed")
    ws2.Columns.AutoFit()
    for worksheet in wb.Sheets:
        if worksheet.Name == "Final":
           worksheet.Move(Before=wb.Sheets("Detailed"))
    wb.Save()
    excel.Application.Quit()
    
    
    
if __name__=="__main__":
    
    writer = pd.ExcelWriter("pros_forecast_change_tracking.xlsx",engine = 'xlsxwriter')
    
    [conn_orc,cursor_orc] = start_connection(None, "ProsUAT", oracle = True, sqlit = False)
    [conn_sqlit,cursor_sqlit] = start_connection(r"H:\pros_forecast_tracking.db", "ProsUAT", oracle = False, sqlit = True)
    
    #Capture today's forecast
    forecast_today = capture_todays_forecast(conn_orc,conn_sqlit)
    #write_data(writer, "pros_today",forecast_today)
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
    writer.save()
    
    format_excel()
    
    
