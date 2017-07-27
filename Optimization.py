# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 13:45:04 2017

@author: bosesaur
"""

import pandas as pd
from scipy.optimize import minimize



def get_data():
    data_all = pd.read_excel(r"H:/export_0721_factor.xlsx",sheetname = "TRAIN")
    data_to_opt = data_all[["PROS_CURR","CY_DMND","NF_RES_HOLD","NF_8WK"]]
    data_test= pd.read_excel(r"H:/export_0721_factor.xlsx",sheetname = "TEST")

    return [data_all,data_to_opt,data_test]

def objective(w):
    w1 = w[0]
    w2 = w[1]
    w3 = w[2]
    
    obj = 0.0
    for i in range(len(data_to_opt)):
        obj = obj + (w1*data_to_opt.loc[i,"PROS_CURR"] + w2*data_to_opt.loc[i,"NF_RES_HOLD"] + w3*data_to_opt.loc[i,"NF_8WK"] - data_to_opt.loc[i,"CY_DMND"])**2.0
   
    return obj

def constraint1(w):
    con1 = w[0]+w[1]+w[2] -1.0
    return con1

def constraint2(w):
    con2 = 1 - w[0]*w[1]*w[2] 
    return con2


def constraint3(w):
    con3 = w[0]*w[1]*w[2] 
    return con3


def get_constraints():
    cons1 = {'type':'eq','fun':constraint1}
    cons2 = {'type':'ineq','fun':constraint2}
    cons3 = {'type':'ineq','fun':constraint3}
    
    cons = [cons1,cons2,cons3]
    
    return cons
    
if __name__=="__main__":
    [data_all, data_to_opt,data_test] = get_data()
    
    w0= (0,0.5,0.5)
    
    cons = get_constraints()
        
    sol = minimize(objective,w0,method = 'SLSQP',constraints=cons)
    
    print sol

    data = data_all.copy()

    data["NF_Weighted"] = sol.x[0]*data["PROS_CURR"] + sol.x[1]*data["NF_RES_HOLD"] + sol.x[2]*data["NF_8WK"]
    
    for i in range(len(data)):
        if data.loc[i,"CY_DMND"]!=0:
            data.loc[i,"PERC_ERR_NF_Weighted"] = (data.loc[i,"NF_Weighted"]/data.loc[i,"CY_DMND"])-1.0
        
    
    data_test["NF_Weighted"] = sol.x[0]*data_test["PROS_CURR"] + sol.x[1]*data_test["NF_RES_HOLD"] + sol.x[2]*data_test["NF_8WK"]
    data_test = data_test.reset_index()
    
    for i in range(len(data_test)):
        if data_test.loc[i,"CY_DMND"]!=0:
            data_test.loc[i,"PERC_ERR_NF"] = (data_test.loc[i,"NF_Weighted"]/data_test.loc[i,"CY_DMND"]) - 1.0             
            data_test.loc[i,"PERC_ERR_PROS"] = (data_test.loc[i,"PROS_CURR"]/data_test.loc[i,"CY_DMND"]) - 1.0             
            data_test.loc[i,"PERC_ERR_8WK"] = (data_test.loc[i,"NF_8WK"]/data_test.loc[i,"CY_DMND"]) - 1.0             
            data_test.loc[i,"PERC_ERR_RES_HOLD"] = (data_test.loc[i,"NF_RES_HOLD"]/data_test.loc[i,"CY_DMND"]) - 1.0             

    
    writer = pd.ExcelWriter(r"H:/Optimize.xlsx")
    data.to_excel(writer,sheet_name = "TRAIN",index=False)
    data_test.to_excel(writer,sheet_name="TEST",index=False) 
    writer.save() 