# -*- coding: utf-8 -*-
"""
Created on Sat Oct 10 14:57:43 2020

@author: Sandra
"""


import os
import pandas as pd
import datetime        
import numpy as np
from datetime import datetime as Datetime
from os.path import exists, isfile, join


Variables_file='C:\\Users\\sandr\\Documents\\gei\\scripts\\Site_variables_gei.xlsx'
Headers_file= 'C:\\Users\\sandr\\Documents\\gei\\scripts\\Headers.xlsx'
L1_filters='C:\\Users\\sandr\\Documents\\gei\\scripts\\L1_filters.xlsx'


def merge_files(folder_path,include = [], exclude = '...', skip_rows = []):
    onlyfiles=[]
    for root, dirs, files in os.walk(folder_path, topdown=False):
        for name in files:
            a=os.path.join(root, name)
            if os.path.isfile(a):
                onlyfiles.append(a)
    appended_data = []
    for s in onlyfiles:
        if any(i in s for i in include) and exclude.lower() not in s.lower():
            data = pd.read_csv(s,delimiter="\s+",encoding='latin1', skiprows = skip_rows, na_values = ['NAN','null','NULL','-99999','-9999'],error_bad_lines=False)
            appended_data.append(data)
    appended_data = pd.concat(appended_data,sort=True) 
    return appended_data




def col_name(site, df,Time = False, sheet='gei_1'):
    
    site_variables = pd.read_excel(Variables_file, sheet_name=sheet,header = [0,1]) #dataframe con la info de excel
    V, U = zip(*site_variables.columns.tolist())
    a = pd.notnull(site_variables.loc[site]).tolist()
    variables = []; units = []
    for i in range(0,len(site_variables.columns)):
        if a[i] == True and V[i] in df.columns:
            variables.append(V[i])
            units.append(U[i])
    if Time:
        variables = ['DATE']+['TIME'] + variables
        units = ['yyyy-mm-dd'] + ['HH:MM:SS'] + units
    return variables, units


def add_elevation(df, site, sheet_name):
    
    
    site_variables = pd.read_excel(Variables_file, sheet_name="altura_valvula", header=(0)) 
    
    
    df['Height'] = float(site_variables.loc[site_variables["Site"]==site]["Height"])

    df.loc[((df["species"] != 2 ) & (df["species"] != 3)), "CO2_dry"]= None
    df.loc[(df["species"]!= 3), "CH4_dry"]= None
    df.loc[((df["species"] != 1 ) & (df["species"] != 4)), "CO"]= None

    
    UTC = float(site_variables.loc[site_variables["Site"]==site]["UTC"])
    
    df["Time"]=df["Time"]+datetime.timedelta(hours=UTC)
    
    df=df.drop(["species"], axis=1)

    return df, UTC

def fill_missing_measurements(df, Time_column = 'Time', mtd = None, tol = None):
    if np.count_nonzero(df.duplicated([Time_column])) != 0:
        df = df.drop_duplicates([Time_column])
    df = df.sort_values([Time_column])
    df = df.reset_index(drop = True)
    oldest = df[Time_column][0]    
    youngest = df[Time_column][df.index[-1]] 
    begin = oldest.strftime('%Y') + '/' + oldest.strftime('%m') + '/1 00:00:00'
    final = youngest.strftime('%Y/%m/%d %H:%M ')
    dates_min = pd.date_range(start = begin, end = final, freq = '1min')
    df = df.set_index([Time_column])

    size_CO=(df["CO"].dropna()).groupby(pd.Grouper(freq='1Min')).size()
    size_CO2=(df["CO2_dry"].dropna()).groupby(pd.Grouper(freq='1Min')).size()
    size_CH4=(df["CH4_dry"].dropna()).groupby(pd.Grouper(freq='1Min')).size()
    
    df_std=df.groupby(pd.Grouper(freq='1Min')).std()
    df=df.groupby(pd.Grouper(freq='1Min')).mean()
    df["CO_SD"]=df_std["CO"]
    df["CO2_SD"]=df_std["CO2_dry"]
    df["CH4_SD"]=df_std["CH4_dry"]

    for i in range(len(size_CO)):
        if (size_CO[i]<30): df['CO'].iloc[i]=None
        if (size_CO[i]<30): df['CO_SD'].iloc[i]=None
        if (size_CO2[i]<30): df['CO2_dry'].iloc[i]=None
        if (size_CO2[i]<30): df['CO2_SD'].iloc[i]=None
        if (size_CH4[i]<15): df['CH4_dry'].iloc[i]=None
        if (size_CH4[i]<15): df['CH4_SD'].iloc[i]=None
    df = df.reindex(dates_min, method = mtd, tolerance = tol)
    
    
    return df , oldest, youngest



def record(site, Rrd, oldest, youngest):
    
    # if it doesn't exist, then begin with the first date of the whole data
    if not os.path.exists(Rrd):      
        # Does the file exist?
        F = 0;
        # the oldest date allowed
        FileDate = oldest
        # and create the record file
        R1 = {'Last ' + site + ' Updload Date' : [youngest]}
        # convert it to Dataframe
        R1 = pd.DataFrame(R1)
        # save the record file
        R1.to_csv(Rrd,index=False)
    # if it exists, then begin with the last uploaded date
    else:
        # Does the file exist?
        F = 1;
        # read the file
        R1 = pd.read_csv(Rrd)
        # set datetime64 into the 'Last Updload Date' column
        R1['Last '+ site +' Updload Date'] = pd.to_datetime(R1['Last '+ site +' Updload Date'])  
        # find the youngest uploaded data   #*youungest
        FileDate = max(R1['Last '+ site +' Updload Date'])  #tenía: max

    
    return F, R1, FileDate




def add_header(site, file_path, data = 'Greenhouse Gas'):
    
    # read the 'Header.xlsx' file
    headers = pd.read_excel(Headers_file, converters = {'Name': str, 'State': str, 'North': str, 'West': str, 'MASL': str, 'UT': str})
    # set the site as the index                
    headers = headers.set_index([('Site')]) 
    # extract the header info for each site
    name =  headers['Name'].loc[site]
    state = headers['State'].loc[site]
    N = headers['North'].loc[site]
    W = headers['West'].loc[site]
    masl = headers['MASL'].loc[site]
    UT = headers['UT'].loc[site]
    # header that is going to be used
    #-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-HEADER-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-           
    line = 'Red Universitaria de Observatorios Atmosfericos (RUOA)\n'+ name + ' (' + site + '), ' + state + '\nLat ' + N + ' N, Lon ' + W +' W, Alt ' + masl + ' masl\nTime UTC-' + UT + 'h\n' + data +' data\n '
    #-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
    # here the files are re-opened to add the missing lines   
    with open(file_path, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)
        
        
        
        
def missing_data_report(site, incoming_route, skip_rows = [0,1,2,3,4,5,7], MPV=False):
    
    # read the dataframe    
    df = pd.read_csv(incoming_route, skiprows = skip_rows, na_values=['null',-9999])
    # identify if the 'RECORD' column is in df   

    del df["CO2_SD"],df["CO_SD"],df["CH4_SD"], df["Height"]

    if  MPV:
         del df["MPVPosition"]
    else:
        pass


    # specify the date column
    df['Time'] = pd.to_datetime(df['Time'], dayfirst = False)
    # set the 'Time' column as the index   
    df = df.set_index(['Time'])
    # count missing data 
    count_nan = 1440 - df.resample('D').count() 
    # define the route where the missing data report is going to be stored
    parts = incoming_route.split('.')
    outcoming_route = parts[0] + '_STAT.csv'
    # save the missing data report   
    count_nan.to_csv(outcoming_route, na_rep='NaN')
    # add the header to the file
    add_header(site, outcoming_route, data = 'Missing')
    
    
    
def write_record(site, youngest, Rrd, F, R1):
    
    if F == 1:
        ### Create the new Dataframe
        R2 = {'Last ' + site + ' Updload Date' : [youngest]}
        # convert it to Dataframe
        R2 = pd.DataFrame(R2)
        # append the new date
        R2  = R2.append(R1, ignore_index=True)
        # save the record file
        R2.to_csv(Rrd, line_terminator = '\r\n', index=False)
        
        
def youngest_date(site, route):

    # auxiliar date
    min_date = Datetime.strptime('9999-01', '%Y-%m')
    # where are the L0 files?
    # all files list
    onlyfiles = [f for f in os.listdir(route) if isfile(join(route, f))]
    # for loop to exclude the 'STAT' files    
    for s in onlyfiles:
        if 'STAT' not in s and 'L0' in s:
            # obtain the date of each file
            s_date = Datetime.strptime(s[0:7], '%Y-%m')
            # compare dates to find the youngest
            if min_date > s_date:
                min_date = s_date
    # get the youngest year found   #oldest
    year = min_date.strftime('%Y')
    # get the youngest month found   #oldest evalua fecha más viejita del directorio
    month = min_date.strftime('%m') 

    return year, month


def df_mean(df, site, grouper):
    # obtain the columns order
    order = df.columns   
    # read the 'L1_filters' file
    NMeasurements = pd.read_excel(L1_filters, header = [0], sheet_name = 'NMeasurements')
    # resample the df, (mean frecuency given by 'rule')
    #!!! aqui esta el problema, hay que hacer que el resample funcione con str distintas a nan
    mean_df = df.groupby(pd.Grouper(freq=grouper), sort= True).mean()
    # order the columns
    mean_df = mean_df[order]
    # count the missing measurements
    C = df.groupby(pd.Grouper(freq='1H'), sort= True).count()
    for col in mean_df.columns:
        # find the columns accuracy 
        try:
            round_n = df_accuracy(df, col)
            mean_df[col] = mean_df[col].round(round_n)
        except:
            pass
        # delete invalid means
        idx = C[C[col] < NMeasurements[col][0]].index
        mean_df.at[idx,col]=np.nan
        
    return mean_df
        
def sigdigits(x):   
    
    if isnan(x):
        return np.nan
    else:
        if '.' in str(x):
            return len( ("%s" % float(x)).split('.')[1])
        else:
            return 0
        
        
        
def df_accuracy(df, variable):
    
    return int(df[variable].apply(sigdigits).mode()[0]) 


def save_month (df, filename, MDR=False ):
                        
    cols, units = col_name(site, df, Time = False, sheet='Header')

    if (os.path.exists(os.path.join(L1,filename))):

        youngest = df.index[-1]
        F, R1, FileDate = record(site, Rrd_L1, oldest, youngest)


        date1 = FileDate
        date2 = youngest.strftime('%Y') + '-' + youngest.strftime('%m') + '-' + youngest.strftime('%d')# +' '+ youngest.strftime('%H')

        df_new = df[date1:date2] 


        incoming_route=os.path.join(L1,filename)
        

        skip_rows = [0,1,2,3,4,5,7]

        df_old = pd.read_csv(incoming_route, skiprows = skip_rows, na_values=['null',-9999])
        df_old['Time']=pd.to_datetime(df_old['Time'],dayfirst = False, errors = 'coerce')
        df_old=df_old.set_index('Time')

        frames = [df_old,df_new] 
        df = pd.concat(frames,sort=False, axis=0).sort_index(axis=0)

#         df=df.reset_index().groupby(['index']).mean()


    else: 
        youngest = df.index[-1]

    header = [['Time'] + cols, ['yyyy-mm-dd HH:MM:SS'] + units]


    df = df.reset_index()
    df.columns  = pd.MultiIndex.from_tuples(list(zip(*header)))

    df.round(4).to_csv(os.path.join(L1, filename), na_rep='null', encoding = 'latin1', index=False) 
    add_header(site, os.path.join(L1, filename),data = 'Greenhouse Gas')

    if MDR: 

        missing_data_report(site, os.path.join(L1, filename), MPV=False)
        
    F, R1, FileDate = record(site, Rrd_L1, oldest, youngest) 
    write_record(site, youngest, Rrd_L1, F, R1)