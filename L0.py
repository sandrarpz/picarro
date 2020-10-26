# -*- coding: utf-8 -*-
"""
Created on Sat Oct 10 15:42:40 2020

@author: Sandra
"""


import pandas as pd
import datetime        
import time
import os
import Master

start = time.time()

#-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
# define the path where the sites folders are stored
path = "C:/Users/sandr/Documents/gei"
# define the path where the month data folders are going to be stored
path_L0 = "C:/Users/sandr/Documents/gei/"

#L0 = os.path.join(path,site,'L0')
#Where the specifies is stored


#-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*  



#topics=["gei"]
#sites=["altz","cham","erno","jqro","ltux","meda","unam"]

# years=["2013","2014","2015","2016","2017","2018","2019","2020"]

years=["2019"]

month_start=8
month_end=9
months=[ str(n).zfill(2) for n in range(month_start,month_end+1)]



topics=["gei"]
sites=["unam"]


for site in sites:
    print ('---',site,'---')
    for topic in topics:
        print (topic)
        L0 = os.path.join(path,site,'L0')
        if not os.path.exists(L0):
            os.makedirs(L0)
            #if "stat" folder is required uncomment next line
            #os.makedirs(L0 + '/stat')
        route0 = os.path.join(path,site,'raw')
        
        for year in years:
            for month in months:
                keywords=[year+month]
                print(keywords)
                keywords='201903'

                try:

                    appended_data = Master.merge_files(route0, include = keywords, exclude='...', skip_rows = [])
                    if topic == 'gei':
                        #add col_name
                        cols, units = Master.col_name(site, appended_data,Time=True,sheet='gei')
                    appended_data = appended_data[cols] 

                    appended_data['Time'] = pd.to_datetime((appended_data["DATE"] +" " + appended_data["TIME"]), dayfirst = False, errors = 'coerce')
                    appended_data=appended_data.drop(["DATE", "TIME"], axis=1)
                    appended_data, UTC = Master.add_elevation(appended_data,site,"altura_valvula")
                    
                    
                    appended_data, oldest, youngest = Master.fill_missing_measurements(appended_data)
                    
                    appended_data = appended_data.rename(columns={'CO2_dry': 'CO2_Avg', 'CH4_dry': 'CH4_Avg','CO': 'CO_Avg'})
 
                    
                    cols, units = Master.col_name(site, appended_data, Time = False, sheet='gei_1')

                    appended_data = appended_data[cols]
            
                    Rrd = os.path.join(path,site,'L0',site+'_record.txt')
    
                    grouped_year_month = appended_data.groupby([lambda x: x.year, lambda x: x.month])
                    
                
                    for group in grouped_year_month:
                        date, df = group
                        Y, M = date
                 
                        filename = str(datetime.date(Y, M,1))[:7] + '-' + site.upper() + '_L0_' + topic + '.dat'
                        
                        
                
                        if (os.path.exists(os.path.join(L0,filename))):
                    
                            youngest = df.index[-1]
                            F, R1, FileDate = Master.record(site, Rrd, oldest, youngest)


                            date1 = FileDate
                            date2 = youngest.strftime('%Y') + '-' + youngest.strftime('%m') + '-' + youngest.strftime('%d')# +' '+ youngest.strftime('%H')

                            df_new = df[date1:date2] 
                            
                            
                            incoming_route=os.path.join(L0,filename)
                            skip_rows = [0,1,2,3,4,5,7]

                            df_old = pd.read_csv(incoming_route, skiprows = skip_rows, na_values=['null',-9999])
                            df_old['Time']=pd.to_datetime(df_old['Time'],dayfirst = False, errors = 'coerce')
                            df_old=df_old.set_index('Time')
                         
                            frames = [df_old,df_new] 
                            df = pd.concat(frames,sort=False, axis=0).sort_index(axis=0)
                            
                            df=df.reset_index().groupby(['index']).mean()
                            
                            

                        else: 
                            youngest = df.index[-1]
                            
                        header = [['Time'] + cols, ['yyyy-mm-dd HH:MM:SS'] + units]
                        

                        df = df.reset_index()
                        df.columns  = pd.MultiIndex.from_tuples(list(zip(*header)))
                
                        df.round(4).to_csv(os.path.join(L0, filename), na_rep='null', encoding = 'latin1', index=False) 
                        Master.add_header(site,os.path.join(L0, filename))
                        Master.missing_data_report(site, os.path.join(L0, filename))


                        F, R1, FileDate = Master.record(site, Rrd, oldest, youngest) 
                        Master.write_record(site, youngest, Rrd, F, R1)


                except:
                    print("No hay archivos")
end = time.time()

