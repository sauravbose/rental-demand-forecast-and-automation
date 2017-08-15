import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn import preprocessing, svm
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import f_regression
import datetime
import math
import matplotlib.pyplot as plt

data_train = pd.read_excel(r"C:\Users\bosesaur\Desktop\Optimization_Final\Optimization_Data.xlsx",sheetname="Training")
data_test = pd.read_excel(r"C:\Users\bosesaur\Desktop\Optimization_Final\Optimization_Data.xlsx",sheetname="Test")

data_test = data_test[data_test["CY_Dmnd"]!=0]
data_test = data_test[data_test["PY_Res_Hold"]!=0]
data_test = data_test[data_test["PY_Dmnd_8Wk"]!=0]
data_test = data_test.reset_index()
del data_test["index"]

data_train = data_train.rename(columns={"CY_Dmnd":"Label"})
data_test = data_test.rename(columns={"CY_Dmnd":"Label"})

data_train = data_train[["Co_Date", "Dow","Brand","Sup_Seg","Prod_Cat","Core","Days_Prior","CY_Res_Hold","PY_Res_Hold","PY_Dmnd","CY_Dmnd_8Wk","PY_Dmnd_8Wk","Label"]]

data_train = pd.get_dummies(data_train, columns = ["Dow","Brand","Sup_Seg","Prod_Cat","Core","Days_Prior"])

X_train = np.array(data_train.drop(["Co_Date","Label"],1))
y_train = np.array(data_train["Label"])

model = LinearRegression()
model.fit(X_train,y_train)
print model.score(X_train,y_train)

data_test = data_test[["Co_Date", "Dow","Brand","Sup_Seg","Prod_Cat","Core","Days_Prior","CY_Res_Hold","PY_Res_Hold","PY_Dmnd","CY_Dmnd_8Wk","PY_Dmnd_8Wk","Label"]]
data_test.head()

data_test = pd.get_dummies(data_test, columns = ["Dow","Brand","Sup_Seg","Prod_Cat","Core","Days_Prior"])

X_test = np.array(data_test.drop(["Co_Date","Label"],1))
y_test = np.array(data_test["Label"])

print model.score(X_test,y_test)
predictions = model.predict(X_test)

d_final = data_test.copy()
d_final["Label"] = y_test
d_final["Predictions"] = predictions

writer = pd.ExcelWriter(r"C:\Users\bosesaur\Desktop\Optimization_Final\Predictions_test.xlsx")
d_final.to_excel(writer,index=False)
writer.save()

predict_train = model.predict(X_train)

predict_train= pd.DataFrame(predict_train)
writer = pd.ExcelWriter(r"C:\Users\bosesaur\Desktop\Optimization_Final\Predictions_train.xlsx")
predict_train.to_excel(writer,index=False)
writer.save()

