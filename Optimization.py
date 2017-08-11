# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 18:09:27 2017

@author: bosesaur
"""

import sqlite3
import pandas as pd
import numpy as np
import datetime
import math
from scipy.optimize import minimize
import matplotlib.pyplot as plt


data = pd.DataFrame()


days = np.arange(1,32,1)
dp = [1,3,7,14,30,55]


for i in days:
    for j in dp:
        data2 = pd.read_csv(r"C:\Files\autoinfl\auto_infl_yoy_{}_{}.txt".format(str(i),str(j)),delimiter = '|',header=None)
        data = data.append(data2,ignore_index=True)

col = ["Co_Date","Mrkt_Grp","Brand","Sup_Seg","Prod_Cat","Core","Days_Prior","CY_Res_Hold","PY_Res_Hold","CY_Dmnd","PY_Dmnd","CY_Dmnd_8Wk","PY_Dmnd_8Wk"]
data.columns = col

writer = pd.ExcelWriter(r"C:\Users\bosesaur\Desktop\Optimization\July_Data.xlsx")
data.to_excel(writer,index=False)
writer.save()

data = data[data["PY_Res_Hold"]!=0]
data = data[data["PY_Dmnd_8Wk"]!=0]

data["Perc_Res_Hold"] = (data["CY_Res_Hold"]/data["PY_Res_Hold"])-1
data["Perc_8Wk"] = (data["CY_Dmnd_8Wk"]/data["PY_Dmnd_8Wk"])-1

data["NF_Res_Hold"]  = data["PY_Dmnd"]*(data["Perc_Res_Hold"]+1)
data["NF_8Wk"] = data["PY_Dmnd"]*(data["Perc_8Wk"]+1)

data["Co_Date"] = data["Co_Date"].apply(lambda x: datetime.datetime.strptime(x,"%d-%b-%y"))
data["Co_Date"] = data["Co_Date"].apply(lambda x: x.date())

data = data.reset_index()
del data["index"]

frcst_avis = pd.read_excel(r"C:\Users\bosesaur\Desktop\Optimization\forecast_avis.xlsx",sheetname="Forecast")
frcst_avis = frcst_avis.drop(["Pool","Aif Pros Curr", "Lor Grp"],1)

col = frcst_avis.columns
col = list(col)

for i in range(len(col)):
    col[i] = col[i].replace(" ","_")

frcst_avis.columns = col

car_grp_mapping = pd.read_excel(r"C:\Users\bosesaur\Desktop\Optimization\forecast_avis.xlsx",sheetname="Map")
car_grp_mapping = car_grp_mapping.rename(columns={"Car Group":"Car_Grp"})

mapping = pd.Series(car_grp_mapping.Core.values,index=car_grp_mapping.Car_Grp).to_dict()

frcst_avis["Core"] = frcst_avis["Car_Grp"]
frcst_avis["Core"] = frcst_avis["Core"].map(mapping)
frcst_avis = frcst_avis.drop(["Car_Grp"],1)

frcst_avis["Co_Date"] = frcst_avis["Co_Date"].apply(lambda x: x.date())

col_order = ["Co_Date","Mrkt_Grp","Brand","Sup_Seg","Prod_Cat","Core","Run_Dp", "Pros_Curr","Actual"]
frcst_avis = frcst_avis[col_order]

frcst_budget = pd.read_excel(r"C:\Users\bosesaur\Desktop\Optimization\forecast_budget.xlsx",sheetname="Forecast")
frcst_budget = frcst_budget.drop(["Pool","Lor Grp", "Aif Pros Curr"],1)

frcst_budget.columns = col

frcst_budget["Core"] = frcst_budget["Car_Grp"]
frcst_budget["Core"] = frcst_budget["Core"].map(mapping)

frcst_budget  = frcst_budget[col_order]

frcst_budget["Co_Date"] = frcst_budget["Co_Date"].apply(lambda x: x.date())

frcst = pd.DataFrame()
frcst = frcst.append(frcst_avis)
frcst = frcst.append(frcst_budget,ignore_index = True)

frcst = frcst[(frcst["Co_Date"]>=datetime.date(2017,7,1)) & (frcst["Co_Date"]<=datetime.date(2017,7,31))]

frcst = frcst.reset_index()
del frcst["index"]

conn_sq = sqlite3.connect(r"C:\Users\bosesaur\Desktop\Optimization\Optim.db")

data.to_sql("Clean_July_Database_Stuff",conn_sq,if_exists = 'replace')
frcst.to_sql("July_Forecast",conn_sq,if_exists = 'replace')

sql_agg = "select Co_Date, Mrkt_Grp, Brand, Sup_Seg, Prod_Cat, Core, Run_Dp, sum(Pros_Curr) as Pros_Curr, sum(Actual) as Actual from July_Forecast group by Co_Date, Mrkt_Grp, Brand, Sup_Seg, Prod_Cat, Core, Run_Dp" 
frcst = pd.read_sql(sql_agg, conn_sq)

frcst.to_sql("July_Forecast",conn_sq,if_exists = 'replace')

sql_join = "select C.*, F.Pros_Curr from Clean_July_Database_Stuff C, July_Forecast F where C.Co_Date = F.Co_Date and C.Mrkt_Grp = F.Mrkt_Grp and C.Brand = F.Brand and C.Sup_Seg = F.Sup_Seg and C.Prod_Cat = F.Prod_Cat and C.Core = F.Core and C.Days_Prior = F.Run_Dp"  

july_data = pd.read_sql(sql_join,conn_sq)

#writer = pd.ExcelWriter(r"C:\Users\bosesaur\Desktop\Optimization\Test.xlsx")
#data.to_excel(writer,sheet_name="Data")
#frcst.to_excel(writer,sheet_name="Frcst")
#july_data.to_excel(writer,sheet_name="July")
#writer.save()

del july_data["index"]

jun_data = pd.read_excel(r"C:\Users\bosesaur\Desktop\Optimization\Clean_Data.xlsx",sheetname="Train")

jun_data["CO_DATE"] = jun_data["CO_DATE"].apply(lambda x: x.date())

col_jun = jun_data.columns
col_jun = list(col_jun)

for i in range(len(col_jun)):
    col_jun[i] = col_jun[i].replace(" ","_")
    col_jun[i] = col_jun[i].replace("%","Perc")

jun_data.columns = col_jun

ab_err_pros = abs(jun_data["PROS_CURR"] - jun_data["CY_DMND"])
jun_data["Abs_Err_Pros"] = ab_err_pros

ab_err_reshold = abs(jun_data["NF_RES_HOLD"] - jun_data["CY_DMND"])
jun_data["Abs_Err_Res_Hold"] = ab_err_reshold

ab_err_8wk = abs(jun_data["NF_8WK"] - jun_data["CY_DMND"])
jun_data["Abs_Err_8Wk"] = ab_err_8wk

july_data["Co_Date"] = july_data["Co_Date"].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d"))
july_data["Co_Date"] = july_data["Co_Date"].apply(lambda x: x.date())
july_data["Dow"] = july_data["Co_Date"]
july_data["Dow"] = july_data["Dow"].apply(lambda x: x.weekday()+1)

july_data = july_data[july_data["CY_Dmnd"]!=0]

ab_err_pros = abs(july_data["Pros_Curr"] - july_data["CY_Dmnd"])
july_data["Abs_Err_Pros"] = ab_err_pros

perc_err_pros = ab_err_pros/july_data["CY_Dmnd"]
july_data["Perc_Err_Pros"] = perc_err_pros

ab_err_reshold = abs(july_data["NF_Res_Hold"] - july_data["CY_Dmnd"])
july_data["Abs_Err_Res_Hold"] = ab_err_reshold

perc_err_reshold = ab_err_reshold/july_data["CY_Dmnd"]
july_data["Perc_Err_Res_Hold"] = perc_err_reshold

ab_err_8wk = abs(july_data["NF_8Wk"] - july_data["CY_Dmnd"])
july_data["Abs_Err_8Wk"] = ab_err_8wk

perc_err_8wk = ab_err_8wk/july_data["CY_Dmnd"]
july_data["Perc_Err_8Wk"] = perc_err_8wk 

july_data.to_sql("July_data", conn_sq,if_exists='replace')
jun_data.to_sql("Jun_Data",conn_sq,if_exists='replace')

july_data_col_order = ["Co_Date","Dow","Mrkt_Grp","Brand","Sup_Seg","Prod_Cat","Core","Days_Prior","CY_Res_Hold","PY_Res_Hold", "CY_Dmnd", "PY_Dmnd","CY_Dmnd_8Wk", "PY_Dmnd_8Wk", "Perc_Res_Hold", "Perc_8Wk", "Pros_Curr", "Abs_Err_Pros","Perc_Err_Pros","NF_Res_Hold","Abs_Err_Res_Hold", "Perc_Err_Res_Hold","NF_8Wk","Abs_Err_8Wk", "Perc_Err_8Wk"]

july_data = july_data[july_data_col_order]

jun_data_col_order = ["CO_DATE","DOW","MRKT_GRP","BRAND","SUP_SEG","PROD_CAT","CORE","RUN_DP","CY_RES_HOLD","PY_RES_HOLD", "CY_DMND", "PY_DMND","CY_DMND_8WK", "PY_DMND_8WK", "Perc_RES_HOLD", "Perc_8WK", "PROS_CURR", "Abs_Err_Pros","Perc_ERROR_PROS","NF_RES_HOLD","Abs_Err_Res_Hold", "Perc_ERROR_RES_HOLD","NF_8WK","Abs_Err_8Wk", "Perc_ERROR_8WK"]

jun_data = jun_data[jun_data_col_order]

col_jul = july_data.columns
jun_data.columns = col_jul

all_data = pd.DataFrame()
all_data = all_data.append(jun_data)
all_data = all_data.append(july_data,ignore_index=True)

all_data.to_sql("All_Data",conn_sq,if_exists='replace')

all_data = pd.read_sql("select * from All_Data Order by Co_Date",conn_sq)

writer = pd.ExcelWriter(r"C:\Users\bosesaur\Desktop\Optimization\All_Data.xlsx")
all_data.to_excel(writer,index=False)
writer.save()

conn_sq.close()

all_data["Co_Date"] = all_data["Co_Date"].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d"))
all_data["Co_Date"] = all_data["Co_Date"].apply(lambda x: x.date())

del all_data["index"]

all_data_temp = all_data.copy()

training_data = all_data_temp[(all_data_temp["Co_Date"]>=datetime.date(2017,6,1)) & (all_data_temp["Co_Date"]<=datetime.date(2017,7,15))]
test_data = all_data_temp[(all_data_temp["Co_Date"]>=datetime.date(2017,7,15)) & (all_data_temp["Co_Date"]<=datetime.date(2017,7,31))]

writer = pd.ExcelWriter(r"C:\Users\bosesaur\Desktop\Optimization\Optimization_Data_3.xlsx")
training_data.to_excel(writer,sheet_name="Training", index = False)
test_data.to_excel(writer,sheet_name="Test", index = False)
writer.save()


# OPTIMIZATION STARTS HERE

def objective(w):
    w1 = w[0]
    w2 = w[1]
    w3 = w[2]
    
    obj = 0.0
    for i in range(len(data_to_opt)):
        obj = obj + (w1*data_to_opt.loc[i,"Pros_Curr"] + w2*data_to_opt.loc[i,"NF_Res_Hold"] + w3*data_to_opt.loc[i,"NF_8Wk"] - data_to_opt.loc[i,"CY_Dmnd"])**2.0
   
    return obj

def constraint1(w):
    con1 = w[0]+w[1]+w[2] -1.0
    return con1

def constraint2(w):
    con2 = w[0] 
    return con2


def constraint3(w):
    con3 = w[1] 
    return con3

def constraint4(w):
    con4 = w[2] 
    return con4


def get_constraints():
    cons1 = {'type':'eq','fun':constraint1}
    cons2 = {'type':'ineq','fun':constraint2}
    cons3 = {'type':'ineq','fun':constraint3}
    cons4 = {'type':'ineq','fun':constraint4}
    
    cons = [cons1,cons2,cons3,cons4]
    
    return cons




conn_sq = sqlite3.connect(r"H:/Optimization.db")
  

data_all = pd.read_excel(r"C:\Users\bosesaur\Desktop\Optimization\Optimization_Data_3.xlsx",sheetname="Training")

data_to_opt = data_all[["Pros_Curr","CY_Dmnd","NF_Res_Hold","NF_8Wk"]]
 
dat = data_all.copy()

dow = dat["Dow"].unique()
mrkt_grp = dat["Mrkt_Grp"].unique()

brand = dat["Brand"].unique()

sup_seg = dat["Sup_Seg"].unique()

prod_cat = dat["Prod_Cat"].unique()

core = dat["Core"].unique()

run_dp=dat["Days_Prior"].unique()   

weights = {}
weights = dict.fromkeys(dow)
for a in weights:
    weights[a] = dict.fromkeys(mrkt_grp)
    for b in weights[a]:
        weights[a][b] = dict.fromkeys(brand)
        for c in weights[a][b]:
            weights[a][b][c] = dict.fromkeys(sup_seg)
            for d in weights[a][b][c]:
                 weights[a][b][c][d]= dict.fromkeys(prod_cat)
                 for e in  weights[a][b][c][d]:
                      weights[a][b][c][d][e]= dict.fromkeys(core)
                      for f in weights[a][b][c][d][e]:
                          weights[a][b][c][d][e][f]= dict.fromkeys(run_dp)
                          for g in weights[a][b][c][d][e][f]:
                              weights[a][b][c][d][e][f][g]= []
                              
                
    
        
            
cons = get_constraints()

for a in dow:
    for b in mrkt_grp:
        for c in brand:
            for d in sup_seg:
                for e in prod_cat:
                    for f in core:
                        for g in run_dp:
                            w0= (0.4,0.3,0.2)
                            data_to_opt = dat[(dat["Dow"]==a) & (dat["Mrkt_Grp"]==b) & (dat["Brand"]==c) & (dat["Sup_Seg"]==d) & (dat["Prod_Cat"]==e) & (dat["Core"]==f) & (dat["Days_Prior"]==g)]
                            data_to_opt = data_to_opt.reset_index()
                            sol = minimize(objective,w0,method = 'SLSQP',constraints=cons)
                            weights[a][b][c][d][e][f][g].append([sol.x[0],sol.x[1],sol.x[2]])
                            


data_test= pd.read_excel(r"C:\Users\bosesaur\Desktop\Optimization\Optimization_Data_3.xlsx",sheetname = "Test")



for i in range(len(data_test)):
    a = data_test.loc[i,"Dow"]
    b = data_test.loc[i,"Mrkt_Grp"]
    c = data_test.loc[i,"Brand"]
    d = data_test.loc[i,"Sup_Seg"]
    e = data_test.loc[i,"Prod_Cat"]
    f = data_test.loc[i,"Core"]
    g = data_test.loc[i,"Days_Prior"]
    w = weights[a][b][c][d][e][f][g][0]
    data_test.loc[i,"NF_Weighted"] = w[0]*data_test.loc[i,"Pros_Curr"] + w[1]*data_test.loc[i,"NF_Res_Hold"] + w[2]*data_test.loc[i,"NF_8Wk"]   

data_test["Abs_Err_Weighted"] = abs(data_test["NF_Weighted"]-data_test["CY_Dmnd"])

for i in range(len(data_test)):
    if data_test.loc[i,"CY_Dmnd"]!=0:
        #data_test.loc[i,"Perc_Err_NF_Weighted"] = abs((data_test.loc[i,"NF_WEIGHTED"]/data_test.loc[i,"CY_DMND"])-1.0)
        data_test.loc[i,"Perc_Err_NF_Weighted"] = data_test.loc[i,"Abs_Err_Weighted"]/data_test.loc[i,"CY_Dmnd"]

 
writer = pd.ExcelWriter(r"C:\Users\bosesaur\Desktop\Optimization\Optimize_Test_SLSQP_2.xlsx")
data_test.to_excel(writer,sheet_name = "TEST",index=False)
writer.save()












