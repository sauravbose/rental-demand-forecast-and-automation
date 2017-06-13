# -*- coding: utf-8 -*-
"""
Created on Wed Jun 07 13:42:26 2017

@author: bosesaur
"""

import pandas as pd
import win32com.client

def read_data():
    df_avis = pd.read_excel(r"J:\Decision Technology\PersonalFolders\Saurav\All_Significant_Changes.xlsx",sheetname = "Avis SummaryDiff_Listing")
    df_budget = pd.read_excel(r"J:\Decision Technology\PersonalFolders\Saurav\All_Significant_Changes.xlsx",sheetname = "Budget SummaryDiff_Listing")
    df_payless = pd.read_excel(r"J:\Decision Technology\PersonalFolders\Saurav\All_Significant_Changes.xlsx",sheetname = "Payless SummaryDiff_Listing")

    return [df_avis,df_budget,df_payless]

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
    mail.To = "Linlin.Fan@avisbudget.com"
    mail.CC = "Saurav.Bose@avisbudget.com"
    mail.Subject = "Percent Change in daily forecast"
    mail.Body = "Hi,\nPFA any flags with regard to percent change in daily forecast.\nBest,\nSaurav Bose"
    attachment1 = r"H:\forecast_change_status.xlsx"
    mail.Attachments.Add(attachment1)
    mail.Send()
    
if __name__=="__main__":
    [df_avis,df_budget,df_payless] = read_data()
    writer = pd.ExcelWriter("forecast_change_status.xlsx", engine = 'xlsxwriter')
    generate_status(df_avis,"Avis")
    generate_status(df_budget,"Budget")
    generate_status(df_payless,"Payless")
    writer.save() 
    send_email()