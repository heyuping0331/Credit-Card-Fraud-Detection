# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 21:53:32 2019

@author: heyup
"""

import os
import pandas as pd
import numpy as np

os.getcwd()
os.chdir('C:\\Users\\heyup\\Documents\\DSO562\\Credit Card Transactions')


# Read data
c = pd.read_csv("CC_cleaned.csv")

# Check null values
c.isna().sum()
# All credit card transactions
c['Transtype'].value_counts()
c.columns.values

c['Date'] = pd.to_datetime(c['Date'])
c.info()

# Amount variables
c = c.sort_values(['Cardnum', 'Date']) # sort in this way for easy merge later
for agg in ['mean', 'max', 'median', 'sum']: 
    for days in ['1d', '2d', '4d', '8d', '15d', '31d']:
        c['amt_'+agg+'_' +"card_" +days]  = c.sort_values('Recnum').set_index('Date').groupby(['Merchnum'])['Amount'].rolling(days).agg(agg).values
        c['Actual/amt_'+agg+'_' +"card_"+days] = c['Amount']/c['amt_'+agg+'_' +"card_" +days]
    
c = c.sort_values(['Merchnum','Date']) # sort in this way for easy merge later
for agg in ['mean', 'max', 'median', 'sum']: 
    for days in ['1d', '2d', '4d', '8d', '15d', '31d']:
        c['amt_'+agg+'_' +"merch_" +days]  = c.sort_values('Recnum').set_index('Date').groupby(['Merchnum'])['Amount'].rolling(days).agg(agg).values
        c['Actual/amt_'+agg+'_' +"merch_"+days] = c['Amount']/c['amt_'+agg+'_' +"merch_" +days]

c = c.sort_values(['Cardnum', 'Merchnum', 'Date']) # sort in this way for easy merge later
for agg in ['mean', 'max', 'median', 'sum']: 
    for days in ['1d', '2d', '4d', '8d', '15d', '31d']:
        c['amt_'+agg+'_' +"cardmerch_" +days]  = c.sort_values('Recnum').set_index('Date').groupby(['Cardnum','Merchnum'])['Amount'].rolling(days).agg(agg).values
        c['Actual/amt_'+agg+'_' +"cardmerch_"+days] = c['Amount']/c['amt_'+agg+'_' +"cardmerch_" +days]

    
c = c.sort_values(['Cardnum', 'Merch zip', 'Date']) # sort in this way for easy merge later
for agg in ['mean', 'max', 'median', 'sum']: 
    for days in ['1d', '2d', '4d', '8d', '15d', '31d']:
        c['amt_'+agg+'_' +"cardzip_" +days]  = c.sort_values('Recnum').set_index('Date').groupby(['Cardnum','Merch zip'])['Amount'].rolling(days).agg(agg).values
        c['Actual/amt_'+agg+'_' +"cardzip_"+days] = c['Amount']/c['amt_'+agg+'_' +"cardzip_" +days]
    

c = c.sort_values(['Cardnum', 'Merch state', 'Date']) # sort in this way for easy merge later
for agg in ['mean', 'max', 'median', 'sum']: 
    for days in ['1d', '2d', '4d', '8d', '15d', '31d']:
        c['amt_'+agg+'_' +"cardstate_" +days]  = c.sort_values('Recnum').set_index('Date').groupby(['Cardnum','Merch state'])['Amount'].rolling(days).agg(agg).values
        c['Actual/amt_'+agg+'_' +"cardstate_"+days] = c['Amount']/c['amt_'+agg+'_' +"cardstate_" +days]
        



# Frequency variables
c = c.sort_values(['Cardnum', 'Date']) # sort in this way for easy merge later
for days in ['1d', '2d', '4d', '8d', '15d', '31d']:
    c['freq_card_'+days] = c.sort_values('Recnum').set_index('Date').groupby(['Cardnum'])['Recnum'].rolling(days).agg('count').values

c = c.sort_values(['Merchnum', 'Date']) # sort in this way for easy merge later
for days in ['1d', '2d', '4d', '8d', '15d', '31d']:
    c['freq_merch_'+days] = c.sort_values('Recnum').set_index('Date').groupby(['Merchnum'])['Recnum'].rolling(days).agg('count').values

c = c.sort_values(['Cardnum', 'Merchnum', 'Date']) # sort in this way for easy merge later
for days in ['1d', '2d', '4d', '8d', '15d', '31d']:
    c['freq_cardmerch_'+days] = c.sort_values('Recnum').set_index('Date').groupby(['Cardnum','Merchnum'])['Recnum'].rolling(days).agg('count').values

c = c.sort_values(['Cardnum', 'Merch zip', 'Date']) # sort in this way for easy merge later
for days in ['1d', '2d', '4d', '8d', '15d', '31d']:
    c['freq_cardzip_'+days] = c.sort_values('Recnum').set_index('Date').groupby(['Cardnum', 'Merch zip'])['Recnum'].rolling(days).agg('count').values

c = c.sort_values(['Cardnum', 'Merch state', 'Date']) # sort in this way for easy merge later
for days in ['1d', '2d', '4d', '8d', '15d', '31d']:
    c['freq_cardstate_'+days] = c.sort_values('Recnum').set_index('Date').groupby(['Cardnum', 'Merch state'])['Recnum'].rolling(days).agg('count').values



# Velocity variables


c = c.sort_values(['Cardnum', 'Date']) # sort in this way for easy merge later
for agg in ['count','sum']: 
    for days in ['1d', '2d']:
        c['tran_'+agg+'_' +"card_" +days]  = c.sort_values('Recnum').set_index('Date').groupby(['Cardnum'])['Amount'].rolling(days).agg(agg).values
 
c = c.sort_values(['Merchnum', 'Date']) # sort in this way for easy merge later
for agg in ['count','sum']: 
    for days in ['1d', '2d']:
        c['tran_'+agg+'_' +"Merch_" +days]  = c.sort_values('Recnum').set_index('Date').groupby(['Merchnum'])['Amount'].rolling(days).agg(agg).values

# avg Daily
for agg in ['count','sum']: 
    for groupbyvar in ['Merchnum','Cardnum']:
        c = c.sort_values([groupbyvar, 'Date'])
        c['avgdaily_tran_'+agg+'_'+groupbyvar +"_8d" ]= c.sort_values('Recnum').set_index('Date').groupby(groupbyvar)['Amount'].rolling('8d').agg(agg).values/7


for agg in ['count','sum']: 
    for groupbyvar in ['Merchnum','Cardnum']:
        c = c.sort_values([groupbyvar, 'Date'])
        c['avgdaily_tran_'+agg+'_'+groupbyvar +"_15d" ]= c.sort_values('Recnum').set_index('Date').groupby(groupbyvar)['Amount'].rolling('15d').agg(agg).values/15


for agg in ['count','sum']: 
    for groupbyvar in ['Merchnum','Cardnum']:
        c = c.sort_values([groupbyvar, 'Date'])
        c['avgdaily_tran_'+agg+'_'+groupbyvar +"_31d" ]= c.sort_values('Recnum').set_index('Date').groupby(groupbyvar)['Amount'].rolling('31d').agg(agg).values/31

temp = pd.DataFrame()

for col in c.columns.values[10:18]:
    for col2 in c.columns.values[18:]:
        temp[col+'/'+col2] = c[col] / c[col2]

c = c.drop(['tran_count_card_1d', 'tran_count_card_2d', 'tran_sum_card_1d',
       'tran_sum_card_2d', 'tran_count_Merch_1d', 'tran_count_Merch_2d',
       'tran_sum_Merch_1d', 'tran_sum_Merch_2d',
       'avgdaily_tran_count_Merchnum_8d',
       'avgdaily_tran_count_Cardnum_8d', 'avgdaily_tran_sum_Merchnum_8d',
       'avgdaily_tran_sum_Cardnum_8d', 'avgdaily_tran_count_Merchnum_15d',
       'avgdaily_tran_count_Cardnum_15d',
       'avgdaily_tran_sum_Merchnum_15d', 'avgdaily_tran_sum_Cardnum_15d',
       'avgdaily_tran_count_Merchnum_31d',
       'avgdaily_tran_count_Cardnum_31d',
       'avgdaily_tran_sum_Merchnum_31d', 'avgdaily_tran_sum_Cardnum_31d'], axis=1)
c = pd.concat([c, temp], axis=1)

# Days since variables

#Create days since variables
for groupbyvar in [['Cardnum'], ['Merchnum'], ['Cardnum', 'Merchnum'], ['Cardnum', 'Merch zip'], ['Cardnum', 'Merch state']]:
    sortCols = groupbyvar[:]
    sortCols.append('Date')
    c = c.sort_values(by = sortCols)
    if len(groupbyvar) == 1:
        c['Days_since_per_' + groupbyvar[0]] = c.groupby(groupbyvar)['Date'].apply(lambda x: (x - x.shift(1)).astype('timedelta64[D]')).fillna(365).values 
    else:
        c['Days_since_per_Cardnum_' + groupbyvar[1]] = c.groupby(groupbyvar)['Date'].apply(lambda x: (x -x.shift(1)).astype('timedelta64[D]')).fillna(365).values 

c.iloc[:,111].describe()
