# -*- coding: utf-8 -*-
"""
Created on Thu Jun 01 18:02:13 2017

@author: bosesaur
"""

import pyodbc
import operator
import numpy as np
import pandas as pd
import win32com.client

def get_data():
    
    conn = pyodbc.connect(DSN="Yieldprod",uid="fanlinli",pwd="Nino0617" )
    
    cursor = conn.cursor()
    
    sql_getdata = "select yld_pool_desc, forecast_flag from yld_pros_frcst_mait where forecast_flag = 'Y'"
    
    cursor.execute(sql_getdata)
    
    yield_prod = cursor.fetchall()
    
    print yield_prod
    
    pros_on = []
    for i in yield_prod:
        pros_on.append(i[0])
    
    print pros_on
    conn.close()
    
    conn = pyodbc.connect(DSN="Yieldtest",uid="fanlinli",pwd="Nino0617" )
    
    cursor = conn.cursor()
    
    yield_test = []
    for i in pros_on:
        sql_getprosonfromyieldtest = "select yld_pool_desc, forecast_flag from yld_pros_frcst_mait where yld_pool_desc = '%s'"%i
        cursor.execute(sql_getprosonfromyieldtest)
        res_temp  = cursor.fetchall()
        yield_test.append(res_temp)
    
    yield_test = reduce(operator.concat,yield_test)
    print yield_test
    
    conn.close()
    
    return [yield_prod,pros_on,yield_test]

def write_data(yield_prod,pros_on,yield_test):
    
    data = pd.DataFrame()
    data["Pool"]=pros_on
    
    yld_prod = []
    for i in yield_prod:
        yld_prod.append(i[1])
    data["Yield Prod"]=yld_prod
    
    yld_qa=[]    
    for i in yield_test:
        yld_qa.append(i[1])
    data["Yield Test"]=yld_qa
    
    data=data.set_index(data["Pool"])   
    del data["Pool"]
    
    writer = pd.ExcelWriter("statusflags.xlsx", engine = 'xlsxwriter')
    data.to_excel(writer, sheet_name = 'Sheet1')
    writer.save()
    print data
    return data

def send_email(data):
    o = win32com.client.Dispatch("Outlook.Application")
    mail = o.CreateItem(0)
    mail.To = "Linlin.Fan@avisbudget.com"
    mail.CC = "Saurav.Bose@avisbudget.com"
    mail.Subject = "Today's Status"
    mail.Body = "Hi,\nPFA today's status as an Excel file.\nBest,\nSaurav Bose"
    attachment1 = r"H:\statusflags.xlsx"
    mail.Attachments.Add(attachment1)
    mail.Send()
        

if __name__=="__main__":
    [yield_prod,pros_on,yield_test]= get_data()
    data = write_data(yield_prod,pros_on,yield_test)
    send_email(data)