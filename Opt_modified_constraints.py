# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 18:09:27 2017

@author: bosesaur
"""


import pandas as pd
from scipy.optimize import minimize
import sqlite3

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












