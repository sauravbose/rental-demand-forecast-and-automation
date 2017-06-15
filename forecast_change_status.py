# -*- coding: utf-8 -*-
"""
Created on Wed Jun 07 13:42:26 2017

@author: bosesaur
"""

import pandas as pd
import win32com.client

def read_data():
    df_a = pd.read_excel(r"J:\Decision Technology\PersonalFolders\Saurav\All_Significant_Changes.xlsx",sheetname = "A SummaryDiff_Listing")
    df_b = pd.read_excel(r"J:\Decision Technology\PersonalFolders\Saurav\All_Significant_Changes.xlsx",sheetname = "B SummaryDiff_Listing")
    df_p = pd.read_excel(r"J:\Decision Technology\PersonalFolders\Saurav\All_Significant_Changes.xlsx",sheetname = "P SummaryDiff_Listing")

    return [df_a,df_b,df_p]

def generate_status(data,sheetname):
    flag_row =[]
    perc_err = data["PercentChange"]
    
    for i in range(len(perc_err)):
        if(abs(perc_err[i])>=0.01):
            flag_row.append(i)
    
    
    column_header = data.columns.values.tolist()
    out_data = pd.DataFrame(columns=column_header)
    
    for i in flag_row:
        out_data = out_data.append(data.iloc[i,:])
    
    out_data = out_data.set_index(out_data["Entityname"])
    del out_data["Entityname"]    
    
    write_data(out_data,sheetname,writer)
    
def write_data(content,sheetname,writer):
    content.to_excel(writer, sheet_name = sheetname)
       
def send_email():
    o = win32com.client.Dispatch("Outlook.Application")
    mail = o.CreateItem(0)
    mail.To = "Linlin.Fan@ab.com"
    mail.CC = "Saurav.Bose@ab.com"
    mail.Subject = "Percent Change in daily forecast"
    mail.Body = "Hi,\nPFA any flags with regard to percent change in daily forecast.\nBest,\nSaurav Bose"
    attachment1 = r"H:\forecast_change_status.xlsx"
    mail.Attachments.Add(attachment1)
    mail.Send()
    
if __name__=="__main__":
    [df_a,df_b,df_p] = read_data()
    writer = pd.ExcelWriter("forecast_change_status.xlsx", engine = 'xlsxwriter')
    generate_status(df_a,"A")
    generate_status(df_b,"B")
    generate_status(df_p,"P")
    writer.save() 
    send_email()
