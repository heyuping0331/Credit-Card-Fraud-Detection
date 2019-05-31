# -*- coding: utf-8 -*-
"""
Created on Sat Mar 23 11:56:52 2019

@author: heyup
"""

import os
import pandas as pd
#from mlxtend.feature_selection import SequentialFeatureSelector as sfs
from sklearn.linear_model import LogisticRegression
from sklearn.feature_selection import RFE
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import scale


# import data
os.chdir('C:\\Users\\heyup\\Documents\\DSO562\\Credit Card Transactions')
os.getcwd()

d = pd.read_csv('CC Data Full.csv')

# Excluding OOT set
d['Date']  = pd.to_datetime(d['Date'])
d = d[d['Date'] <="2010-10-31"]

# 186 Candidate variables
variables = list(pd.read_excel('KS & FDR Ranks.xlsx', sheet_name='half')['Variables'])
variables.append('Fraud')
d_model = d.loc[:, variables]
Y = d_model.loc[:,'Fraud']
X = d_model.drop('Fraud', axis=1)

# Recursive feature selection: backward
model = LogisticRegression()
rfe = RFE(model, 20)
fit = rfe.fit(X, Y)
print("Num Features: %s" % (fit.n_features_))
# Final 20 features
final_variables = list(X.columns.values[fit.support_])


## Split Train and test set
# 
d_full = pd.read_csv('CC Data Full.csv')
d_full['Date']  = pd.to_datetime(d_full['Date'])



final_variables.extend(['Fraud','Recnum', 'Date'])

d_full = d_full[final_variables]

d_oot = d_full[d_full['Date'] >"2010-10-31"]
d_int = d_full[d_full['Date'] <="2010-10-31"]

d_oot = d_oot.drop('Date',axis=1)
d_int = d_int.drop('Date',axis=1)

X_train, X_test, y_train, y_test = train_test_split(d_int.iloc[:,:20], d_int['Fraud'], test_size=0.3, random_state=42)

# Z-scale

X_train_scaled = pd.DataFrame(scale(X_train),
                              columns = d_int.iloc[:,:20].columns.values)

X_test_scaled = pd.DataFrame(scale(X_test),
                              columns = d_int.iloc[:,:20].columns.values)

X_oot_scaled = pd.DataFrame(scale(d_oot.iloc[:,:20]),
                            columns = d_oot.iloc[:,:20].columns.values)

y_oot = pd.DataFrame(d_oot['Fraud'])
y_test = pd.DataFrame(y_test)
y_train = pd.DataFrame(y_train)

# Export
X_train_scaled.to_csv("X_train_scaled.csv", index=False)

X_test_scaled.to_csv("X_test_scaled.csv", index=False)

X_oot_scaled.to_csv("X_oot_scaled.csv", index=False)

y_oot.to_csv("y_oot.csv", index=False)
y_test.to_csv("y_test.csv", index=False)
y_train.to_csv("y_train.csv", index=False)














