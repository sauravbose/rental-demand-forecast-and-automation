# -*- coding: utf-8 -*-
"""
Created on Wed Jun 07 09:28:26 2017

@author: bosesaur
"""

import pandas as pd
import numpy as np
import win32com.client as win32
import re

def read_data():
    data_file = ['comp_codate_vs_dp_data','comp_dow_vs_dp_data','test']
    df = pd.read_excel(r'J:\Decision Technology\PersonalFolders\Saurav\{}.xlsx'.format(data_file[0]))
    df_dow = pd.read_excel(r'J:\Decision Technology\PersonalFolders\Saurav\{}.xlsx'.format(data_file[1]))  
    
    return[df,df_dow]

def start_summary(df):
    df2 = df[['MRKT_GRP','RUN_DP','BRAND','PROS_PERC_ERR']]
    mrkt_grp = []
    for i in df2['MRKT_GRP']:
        mrkt_grp.append(i)
    mrkt_grp = np.unique(mrkt_grp)
    
    brand = []
    for i in df2['BRAND']:
        brand.append(i)
    brand = np.unique(brand)
    
    days_prior = []
    for i in df2['RUN_DP']:
        days_prior.append(i)
    days_prior = np.unique(days_prior) 
    
    d_detailed = df2.copy()
    out_detailed = d_detailed.groupby(['MRKT_GRP','BRAND','RUN_DP']).sum()
    del out_detailed['PROS_PERC_ERR']
        
    d_final = df2.copy()
    del d_final['RUN_DP']
    out_final = d_final.groupby(['MRKT_GRP','BRAND']).sum()    
    del out_final['PROS_PERC_ERR']  
   
    return [out_detailed,out_final,mrkt_grp,brand,days_prior]
    
    
def generate_summary(df,out_detailed,out_final,in_column_header,out_column_header,mrkt_grp,brand,days_prior):
            
    df2 = df[['MRKT_GRP','RUN_DP','BRAND',in_column_header]]
    
    d1 = df2.groupby(['MRKT_GRP','BRAND','RUN_DP'])
    out_data = d1.sum()
    
    d_err = out_data[in_column_header]
    pros_perc_error = []
    for i in range(len(d_err)):
        pros_perc_error.append(d_err[i])
    
    comment =[]
    for i in range(len(pros_perc_error)):
        if(abs(pros_perc_error[i]-0.0)<0.00000001):
            comment.append('NAN')
        elif(pros_perc_error[i]>-0.1 and pros_perc_error[i]<0.1):
            comment.append('Good')
        elif(pros_perc_error[i]>0.1): 
            comment.append('Overforecast')
        else:
            comment.append('Underforecast')
    
    
    out_detailed[out_column_header] = comment
    
    flag = []
    for i in range(len(comment)):
        if(comment[i]=='Good'):
            flag.append(1)
        else:
            flag.append(0)
    
     
    out_detailed['Flag_%s'%out_column_header] = flag    
    
    final_flag = {}
    final_flag = dict.fromkeys(mrkt_grp)
    for i in final_flag:
        final_flag[i]=dict.fromkeys(brand)
    for i in final_flag:
        for j in final_flag[i]:
            final_flag[i][j] = []
    
    comment_index = 0        
    for m in mrkt_grp:
        for b in brand:
            for i in range(len(days_prior)):
                if comment[comment_index]!= 'NAN':
                    final_flag[m][b].append(flag[comment_index])
                comment_index+=1    
    
    out_mean =[]
    for m in mrkt_grp:
        for b in brand:
            final_flag[m][b] = np.mean(final_flag[m][b])
            out_mean.append(final_flag[m][b])
    
    final_comment = []
    for i in range(len(out_mean)):
        if (out_mean[i]>=0.5):
            final_comment.append('Good')
        else: 
            final_comment.append('Bad')
            
    out_final[out_column_header] = final_comment 
                  
 
def generate_dow_summary(df_dow,in_col_head,out_final, mrkt_grp,brand):
    dow_dict = {1:'Sun',2:'Mon',3:'Tue',4:'Wed',5:'Thu',6:'Fri',7:'Sat'}
    dow = dow_dict.keys()             
            
    df2_dow = df_dow[['MRKT_GRP','RUN_DP','BRAND','DOW',in_col_head]]
    df2_dow = df2_dow.dropna()
    d2 = df2_dow.groupby(['MRKT_GRP','BRAND','DOW'])
    d3 = d2.mean()
    d3 = d3.drop(['RUN_DP'],axis=1)
    
    dow_flag ={}
    dow_flag = dict.fromkeys(mrkt_grp)
    for i in dow_flag:
        dow_flag[i]=dict.fromkeys(brand)
    for i in dow_flag:
        for j in dow_flag[i]:
            dow_flag[i][j] = []
    
    dow_com_index = 0        
    for m in mrkt_grp:
        for b in brand:
            for i in range(len(dow)):
                dow_flag[m][b].append((i+1,d3[in_col_head][dow_com_index]))
                dow_com_index+=1    
    
    
    for m in mrkt_grp:
        for b in brand:
            undr_frcst = []
            for i in range(len(dow)):
                if(dow_flag[m][b][i][1]<-0.05):
                    undr_frcst.append(dow_flag[m][b][i][0])
            for j in range(len(undr_frcst)):
                undr_frcst[j]=dow_dict[undr_frcst[j]]
            dow_flag[m][b]=undr_frcst
    
    out_dow_flag = []
    for m in mrkt_grp:
        for b in brand:
            out_dow_flag.append(dow_flag[m][b])
    
  
    out_final['Underforecast_%s'%in_col_head] = out_dow_flag
                    
def generate_week_summary(df,in_col,out_col):
    df2 = df.loc[df['CO_DATE']>=pd.to_datetime('2017/05/08')]
    df3 = df2.loc[df2['CO_DATE']<=pd.to_datetime('2017/05/15')]
    
    generate_summary(df3,out_detailed,out_final,in_col,out_col,mrkt_grp,brand,days_prior)

     
def write_data(writer,sheetname,content):
    
    content.to_excel(writer, sheet_name = sheetname)
    
def format_output(d_final,d_detailed):
    
    d_final["Underforecast_PROS_PERC_ERR_DATA"]= d_final["Underforecast_PROS_PERC_ERR_DATA"].apply(lambda x: re.sub(r"[\['\]]",'',str(x)))
    d_final["Underforecast_ERR_DIFF_PROS_YLD"]= d_final["Underforecast_ERR_DIFF_PROS_YLD"].apply(lambda x: re.sub(r"[\['\]]",'',str(x)))
    
     
    write_data(writer,'Final',d_final)
    write_data(writer,'Detailed',d_detailed)  
    writer.save()
    
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(r'H:\Weekly_Summary.xlsx')
    ws1 = wb.Worksheets("Final")
    ws1.Columns.AutoFit()
    
    d = pd.read_excel(r"H:\Weekly_Summary.xlsx")
    
    a = d.columns.get_loc("vs Actual (Overall)")
    a+=1
    for i in range(len(d)):
        if d.loc[i,"vs Actual (Overall)"]=='Bad':
            ws1.Cells(i+2,a).Interior.ColorIndex = 3
        else:
            ws1.Cells(i+2,a).Interior.ColorIndex = 4
            
    a = d.columns.get_loc("vs Actual (Past Week)")
    a+=1
    for i in range(len(d)):
        if d.loc[i,"vs Actual (Past Week)"]=='Bad':
            ws1.Cells(i+2,a).Interior.ColorIndex = 3
        else:
            ws1.Cells(i+2,a).Interior.ColorIndex = 4
            
    a = d.columns.get_loc("vs Yield (Overall)")
    a+=1
    for i in range(len(d)):
        if d.loc[i,"vs Yield (Overall)"]=='Bad':
            ws1.Cells(i+2,a).Interior.ColorIndex = 3
        else:
            ws1.Cells(i+2,a).Interior.ColorIndex = 4
            
    a = d.columns.get_loc("vs Yield (Past Week)")
    a+=1
    for i in range(len(d)):
        if d.loc[i,"vs Yield (Past Week)"]=='Bad':
            ws1.Cells(i+2,a).Interior.ColorIndex = 3
        else:
            ws1.Cells(i+2,a).Interior.ColorIndex = 4
            
    for i in range(1,9):
        for j in range(len(d)):
            ws1.Cells(j+2,i).BorderAround()
            
    ws1.Columns.BorderAround()        
    ws2 = wb.Worksheets("Detailed")
    ws2.Columns.AutoFit()

    wb.Save()
    excel.Application.Quit()


if __name__=='__main__':
    writer = pd.ExcelWriter('Weekly_Summary.xlsx', engine = 'xlsxwriter')
    
    [df,df_dow]=read_data()
    [out_detailed,out_final,mrkt_grp,brand,days_prior] = start_summary(df)
    
    generate_summary(df,out_detailed,out_final,'PROS_PERC_ERR','vs Actual (Overall)',mrkt_grp,brand,days_prior)
    generate_dow_summary(df_dow,'PROS_PERC_ERR_DATA', out_final, mrkt_grp,brand)
    generate_week_summary(df,'PROS_PERC_ERR','vs Actual (Past Week)')
    
    generate_summary(df,out_detailed,out_final,'ERR_DIFF_PROS_YLD','vs Yield (Overall)',mrkt_grp,brand,days_prior)
    generate_dow_summary(df_dow,'ERR_DIFF_PROS_YLD', out_final, mrkt_grp,brand)
    generate_week_summary(df,'ERR_DIFF_PROS_YLD','vs Yield (Past Week)')
    
    #Write the output dataframe to file          
    format_output(out_final,out_detailed)
      
    
    