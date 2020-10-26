# -*- coding: utf-8 -*-
"""
Created on Thu Apr  2 11:30:34 2020

@author: Sandra
"""

import pandas as pd
from dateutil.relativedelta import relativedelta
import time
import os
import Master
start_time = time.time()
#-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
# define the path where the site folders are stored
path = "C:/Users/sandr/Documents/gei"
#-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*         
# list with the name of the sites

#topics=["gei"]
#sites=["altz","cham","erno","jqro","ltux","meda","unam"]

# years=["2013","2014","2015","2016","2017","2018","2019","2020"]

years=["2019"]

month_start=9
month_end=12
months=[ str(n).zfill(2) for n in range(month_start,month_end+1)]



topics=["gei"]
sites=["unam"]


for site in sites:
    print('*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#')
    print(site)
    L0 = os.path.join(path,site,'L0') 
    L1 = os.path.join(path,site,'L1') 
    
    if not os.path.exists(L1):
        os.makedirs(L1)
        os.makedirs(os.path.join(L1,'1min'))
        os.makedirs(os.path.join(L1,'5min'))
        os.makedirs(os.path.join(L1,'10min'))    
        os.makedirs(os.path.join(L1,'30min'))
        os.makedirs(os.path.join(L1,'Hora'))        
        os.makedirs(os.path.join(L1,'stat')) 
        
    Rrd_L0 = os.path.join(L0,site+'_record.txt')
    Rrd_L1 = os.path.join(L1,site+'_record_L1.txt')
    
    start_year, start_month = Master.youngest_date(site)
    start = pd.to_datetime(start_year+'-'+start_month)
    _, _, end = Master.record(site, Rrd_L0, None, None)
    F, R1, start = Master.record(site, Rrd_L1, start, end)
    dates_array = pd.date_range(start = start, end = end.date() + relativedelta(months = 1), freq = 'M', closed = 'left').strftime('%Y-%m')  
    print(dates_array)