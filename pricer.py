
import sys, json, os
import urllib.request as ur
from urllib.request import Request, urlopen
import pandas as pd
import requests
import sqlalchemy as sa
import datetime as dt
import numpy as np
import time
import pymysql
import yfinance as yf
from datetime import date, timedelta
from settings import *
#from indicators import *

yf.pdr_override()

start = time.time()


twelvedata_intervals = ['1min', '15min', '30min', '1h', '1day', '1week', '1month']
twelvedata_base = 'https://api.twelvedata.com/time_series?symbol='
twelvedata_interval = '&interval='
twelvedata_output = '&outputsize='

quandle_base = 'http://www.quandl.com/api/v3/datasets/'


apikeys = [apikey1, apikey2, apikey3, apikey4, apikey5, apikey6, apikey7, apikey8, apikey9, apikey10, apikey11, apikey12, apikey13]

api_minute_limit = 12

maxfetch = 5000

technical_lookback_period = 120

counter = 0
total_counter = 0

databases = ['price_1minute', 'price_15minutes', 'price_30minutes', 'price_1hour', 'price_1day', 'price_1week', 'price_1month']

intervals = twelvedata_intervals
show = """SHOW TABLES"""

status = pd.DataFrame()
errors = pd.DataFrame()

status_starttime = time.time()
status_errors = []
status_unavailable = []
status_position = '1'


sqlEngine2 = sa.create_engine('mysql+pymysql://root:password@127.0.0.1:3306/stocks', pool_recycle=3600)
dbConnection2 = sqlEngine2.connect()
conn2 = pymysql.connect(host='127.0.0.1',user='root',password='password',db='stocks')
cursor2 = conn2.cursor()

check2 = cursor2.execute(show)
results2 = cursor2.fetchall()
tables2 = [item[0] for item in results2]


select_tickers = """SELECT * FROM `stocks`.`book`"""

tickers = pd.read_sql(select_tickers, dbConnection2)
tickers = list(tickers['ticker'])

tickers = tickers + indices + forex


try:
    prestatus_completed = report['completed'].iloc[0]
    prestatus_position = report['position'].iloc[0]
except:
    prestatus_completed = 'False'
    prestatus_position = 0



total = len(tickers)
alltotal = total * len(databases)


if prestatus_completed == 'True':
    pass

else:

    restart_interval = int(prestatus_position / total)
    restart_position = int(prestatus_position % total)


try:
    for interval,database in zip(intervals, databases):

        counter_interval = 0


        if counter_interval == restart_interval:
            
            counter_interval = counter_interval + 1

            sqlEngine = sa.create_engine('mysql+pymysql://root:password@127.0.0.1:3306/{}'.format(database), pool_recycle=3600)
            dbConnection = sqlEngine.connect()

            conn = pymysql.connect(host='127.0.0.1',user='root',password='password',db='{}'.format(database))
            cursor = conn.cursor()

            date_df = pd.DataFrame()
            d_url = twelvedata_base + 'AAPL' + twelvedata_interval + interval + twelvedata_output + str(maxfetch) + apikey6
            d_req = Request(d_url, headers={'User-Agent': 'Mozilla/5.0'})
            d_webpage = urlopen(d_req).read()
            d_data = json.loads(d_webpage)
            ddf = pd.json_normalize(d_data['values'])
            ddf = ddf.reset_index()
            ddf = ddf.iloc[::-1]
            ddf = ddf['datetime']

            check = cursor.execute(show)
            results = cursor.fetchall()
            tables = [item[0] for item in results]

            for i,stock in enumerate(tickers):


                if '/' in stock:
                    stock = stock.replace("/", "")
                else:
                    pass

                total_counter = total_counter + 1

                buffer = 100

                counter_position = 0

                if counter_position == restart_position:

                    counter_position = counter_position + 1

                    timeout = time.time() + 10.00
                    x = 0

                    while (time.time() < timeout) and (x == 0): 

                        if counter > ((12*api_minute_limit)-1): counter = 0
                        else: pass                

                        if counter < api_minute_limit:                                          apikey = apikey1
                        elif (counter >= 1*api_minute_limit) & (counter < 2*api_minute_limit):  apikey = apikey2
                        elif (counter >= 2*api_minute_limit) & (counter < 3*api_minute_limit):  apikey = apikey3
                        elif (counter >= 3*api_minute_limit) & (counter < 4*api_minute_limit):  apikey = apikey4
                        elif (counter >= 4*api_minute_limit) & (counter < 5*api_minute_limit):  apikey = apikey5
                        elif (counter >= 5*api_minute_limit) & (counter < 6*api_minute_limit):  apikey = apikey7
                        elif (counter >= 6*api_minute_limit) & (counter < 7*api_minute_limit):  apikey = apikey8
                        elif (counter >= 7*api_minute_limit) & (counter < 8*api_minute_limit):  apikey = apikey9
                        elif (counter >= 8*api_minute_limit) & (counter < 9*api_minute_limit):  apikey = apikey10
                        elif (counter >= 9*api_minute_limit) & (counter < 10*api_minute_limit): apikey = apikey11                                             
                        else:                                                                   apikey = apikey12

                        counter = counter + 1

                        try:
                            
                            table = stock.lower()
                            drop = """DROP TABLE {}""".format(table)

                            if table not in tables:

                                updating = 0                
                                output = maxfetch
                                new = True
                                skip = True

                            else:
                                    
                                updating = 1
                                new = False
                                select = """SELECT datetime FROM {}""".format(table)
                                dates = cursor.execute(select)
                                datelist = cursor.fetchall()
                                datetimes = [val[0] for val in datelist]
                                datelen = len(datetimes)

                                if datelen == 0:
                                    skip = True
                                    output = maxfetch
                                    latest = ddf[maxfetch-1]                                    
                 
                                else:
                                    skip = False                    
                                    latest = datetimes[datelen-1]
                                    index = ddf.index[ddf == latest][0]
                                    newindex = maxfetch - index
                                    output = maxfetch - newindex

                                               
                            if output > 0:

                                output = output + technical_lookback_period

                                if (output + buffer) > 5000:
                                    buffer = buffer - ((output + buffer) - maxfetch)

                                else: pass

                                output = str(output + buffer)
                                url = twelvedata_base + stock + twelvedata_interval + interval + twelvedata_output + output + apikey
                                req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                                webpage = urlopen(req).read()
                                data = json.loads(webpage)

                                try:
                                    
                                    df = pd.json_normalize(data['values'])
                                    df = df.reset_index()
                                    df = df.iloc[::-1]
                                    
                                    df.drop(columns=(['index']), inplace = True)
                                    df.drop(df.columns[df.columns.str.contains('unnamed',case = False)],axis = 1, inplace = True)

                                    if stock == 'VIX':
                                        pass
                                    else:
                                        
                                        df['open'] = pd.to_numeric(df['open'])
                                        df['high'] = pd.to_numeric(df['high'])
                                        df['low'] = pd.to_numeric(df['low'])
                                        df['close'] = pd.to_numeric(df['close'])
                                        df['volume'] = pd.to_numeric(df['volume'])

                                        ave_volume = df.volume.rolling(window=20).mean()

                                        df['returns'] = round(df.close.pct_change(), 3)
                                                 
                                        df['volume_change'] = np.where((df.volume.shift(1) > 0), round(df.volume.pct_change(), 3), 0)
                                        df['rel_volume'] = round(df.volume/ave_volume, 3)
                                        df['high-low'] = round(df['high'] - df['low'], 3)
                                        df['high-close'] = round(abs(df['high'] - df['close']), 3)
                                        df['low-close'] = round(abs(df['low'] - df['close']), 3)         


                                    if skip == False:

                                        last = df.index[df['datetime'] == latest][0]
                                        cutoff = len(df) - last
                                        df = df[cutoff:]

                                    else:
                                        pass
                                        
                                    

                                    df.dropna(inplace=True)

                                    lens = (len(df))
                                    
                                     
                                    if updating == 0:
                                        df.to_sql(table, con=sqlEngine, if_exists='replace', index=False)
                                        print('{}/'.format(i+1) + str(total) + ' CREATED {} WITH {} ROWS: {}'.format(interval, lens, stock))
                                        x = 1

                                    else:
                                       
                                        df.to_sql(table, con=sqlEngine, if_exists='append', index=False)
                                        print('{}/'.format(i+1) + str(total) + ' UPDATED {} WITH {} ROWS: {}'.format(interval, lens, stock))
                                        x = 1



                                except Exception as errorA1:

                                    print(str(stock) + " ERROR A1: " + str(errorA1))# + " @ " + str(url))

                                    # YAHOO FINANCE FOR 1 DAY PRICE DATA ONLY
                                    try:
                                        select = """SELECT datetime FROM {}""".format(table)
                                        dates = cursor.execute(select)
                                        datelist = cursor.fetchall()
                                        datetimes = [val[0] for val in datelist]
                                        datelen = len(datetimes)

                                        df = yf.download(stock)

                                        df.drop(columns='Close', inplace=True)
                                        df.reset_index(inplace=True)
                                        df.rename(columns={'Date':'datetime', 'Open':'open', 'High':'high', 'Low':'low', 'Adj Close':'close', 'Volume':'volume'}, inplace=True)
                                        df.set_index('datetime', inplace=True)
                                        
                                        if stock == 'VIX':
                                            pass
                                        else:
                                            
                                            df['open'] = pd.to_numeric(df['open'])
                                            df['high'] = pd.to_numeric(df['high'])
                                            df['low'] = pd.to_numeric(df['low'])
                                            df['close'] = pd.to_numeric(df['close'])
                                            df['volume'] = pd.to_numeric(df['volume'])                                       

                                            ave_volume = df.volume.rolling(window=20).mean()

                                            df['returns'] = round(df.close.pct_change(), 3)              

                                            
                                            df['volume_change'] = np.where((df.volume.shift(1) > 0), round(df.volume.pct_change(), 3), 0)
                                            df['rel_volume'] = round(df.volume/ave_volume, 3)
                                            df['high-low'] = round(df['high'] - df['low'], 3)
                                            df['high-close'] = round(abs(df['high'] - df['close']), 3)
                                            df['low-close'] = round(abs(df['low'] - df['close']), 3)         

                                            df.dropna(inplace=True)
                                            
                                        
                                        if datelen == 0:
                                            lens = (len(df))

                                            df.reset_index(inplace=True)

                                            print("SKIPPING")
                                            continue # creating new database hangs here for some reason

                                            df.to_sql(table, con=sqlEngine, if_exists='replace', index=False)
                                            print('{}/'.format(i+1) + str(total) + ' CREATED {} WITH {} ROWS: {}'.format(interval, lens, stock))
                         
                                        else:          
                                            latest = str(datetimes[datelen-1])

                                            print(latest)
                                        
                                            df = df[latest:]
                                            df = df[1:]

                                            lens = (len(df))

                                            df.reset_index(inplace=True)

                                            df.to_sql(table, con=sqlEngine, if_exists='append', index=False)
                                            print('{}/'.format(i+1) + str(total) + ' UPDATED {} WITH {} ROWS: {}'.format(interval, lens, stock))



                                    except Exception as errorA2:

                                        print("ERROR A2: " + str(errorA2))
                                        pass
                                    
                                    
                                    try:
                                        err_code = data['code']
                                    except:
                                        print("ERROR OCCURED WHILE PROCESSING " + str(stock) + ". INFO: " + str(errorA))
                                        error1 = str(stock) + str(interval) + ": UNREACHABLE"
                                        status_errors.append(error1)
                                        pass


                                    if err_code == 400:
                                        print(str(stock) + " DATA UNAVAILABLE")
                                        if stock not in status_unavailable:
                                            status_unavailable.append(stock)
                                        else:
                                            pass
                                    else:
                                        pass

                                    x = 1
                                                            

                            else:
                                print('{}/'.format(i+1) + str(total) + " TABLE ALREADY UP TO DATE: " + str(interval) + " " + str(stock))
                                x = 1


                        except Exception as errorB:             
                             
                            print("ERROR OCCURED WHILE PROCESSING " + str(stock) + ". INFO: " + str(errorB))

                            error2 = str(stock) + str(interval) + ": " + str(errorB)
                            status_errors.append(error2)
                            x = 1

                else:
                    counter_position = counter_position + 1

        else:
            counter_interval = counter_interval + 1

                    
except Exception as errorC:

    print("UNEXPECTED ERROR OCCURED. INFO: " + str(errorC))

    status_position = total_counter - 1

    status_endtime = time.time()
    status_time = status_endtime - status_starttime

    complete = False

    if status_position+1 == alltotal:
        complete = True
    else:
        complete = False

    status['complete'] = pd.Series(complete)
    status['time'] = pd.Series(status_time)
    status['progress'] = pd.Series(str(status_position+1) + " / " + str(alltotal))

    errors['ticker'] = status_unavailable
    
    errors.to_sql('errors', con=sqlEngine2, if_exists='replace', index=False)
    status.to_sql('report', con=sqlEngine2, if_exists='replace', index=False)


status_position = total_counter - 1

status_endtime = time.time()
status_time = round(((status_endtime - status_starttime)/60), 2)

progress = str(status_position + 1) + '/' + str(total)

if status_position+1 == alltotal:
    complete = 'SUCCESS'
else:
    complete = 'FAILED'


errors['ticker'] = status_unavailable


errors.to_sql('errors', con=sqlEngine2, if_exists='replace', index=False)

print("RUNTIME: ", round(((time.time() - start)/60), 2), " MINUTES")







            
