import warnings
warnings.filterwarnings('ignore')

import requests
import pandas as pd
# from nsepython import *
# print(indices)
import pymysql
import os
try:
    import datetime
except ImportError:
    os.system('python -m pip install datetime')
import numpy
import math
try:
    import time
except ImportError:
    os.system('python -m pip install time')
from datetime import datetime, timedelta
import time
mydb = pymysql.connect(
    host="localhost",
    user="root",
    password="M@bile7204"
    )
mycursor = mydb.cursor()

class OptionChain:
    def __init__(self,symbol,timeout=5) -> None:
        self.__url="https://www.nseindia.com/api/option-chain-equities?symbol={}".format(symbol)
        self.__session = requests.sessions.Session()
        self.__session.headers =  {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0", "Accept":"*/*","Accept-Language":"en-us,en;q=0.5"}
        self.__timeout = timeout
        self.__session.get("https://www.nseindia.com/option-chain", timeout=self.__timeout)
    def fetch_data(self, expiry_date=None, starting_strike_price=None, number_of_rows=2):
        try:
            data = self.__session.get(url=self.__url, timeout=self.__timeout)
            data = data.json()
            return data
            print(data["records"].keys())
            df = pd.json_normalize(data['records']['data'])
            if expiry_date != None:
                df = df[(df.expiryDate == expiry_date)]
            if starting_strike_price != None:
                df = df[(df.strikePrice >= starting_strike_price)][:number_of_rows]
            return df
        except Exception as ex:
            print('Error: {}'.format(ex))
            self._session.get("https://www.nseindia.com/option-chain", timeout=self.__timeout)
        return []
    
def oi_chain_builder( symbol,payload,expiry="latest",oi_mode="full"):
    global oi_data
    # payload = fetch_data(symbol)

    if(oi_mode=='compact'):
        col_names = ['CALLS_OI','CALLS_Chng_in_OI','CALLS_Volume','CALLS_IV','CALLS_LTP','CALLS_Net_Chng','StrikePrice','PUTS_OI','PUTS_Chng_in_OI','PUTS_Volume','PUTS_IV','PUTS_LTP','PUTS_Net_Chng']
    if(oi_mode=='full'):
        col_names = ['CALLS_Chart','CALLS_OI','CALLS_Chng_in_OI','CALLS_Volume','CALLS_IV','CALLS_LTP','CALLS_Net_Chng','CALLS_Bid_Qty','CALLS_Bid_Price','CALLS_Ask_Price','CALLS_Ask_Qty','StrikePrice','PUTS_Bid_Qty','PUTS_Bid_Price','PUTS_Ask_Price','PUTS_Ask_Qty','PUTS_Net_Chng','PUTS_LTP','PUTS_IV','PUTS_Volume','PUTS_Chng_in_OI','PUTS_OI','PUTS_Chart']
    oi_data = pd.DataFrame(columns = col_names)
    

    #oi_row = {'CALLS_OI':0, 'CALLS_Chng in OI':0, 'CALLS_Volume':0, 'CALLS_IV':0, 'CALLS_LTP':0, 'CALLS_Net Chng':0, 'Strike Price':0, 'PUTS_OI':0, 'PUTS_Chng in OI':0, 'PUTS_Volume':0, 'PUTS_IV':0, 'PUTS_LTP':0, 'PUTS_Net Chng':0}
    oi_row = {'CALLS_OI':0, 'CALLS_Chng_in_OI':0, 'CALLS_Volume':0, 'CALLS_IV':0, 'CALLS_LTP':0, 'CALLS_Net_Chng':0, 'CALLS_Bid_Qty':0,'CALLS_Bid_Price':0,'CALLS_Ask_Price':0,'CALLS_Ask_Qty':0,'StrikePrice':0, 'PUTS_OI':0, 'PUTS_Chng_in_OI':0, 'PUTS_Volume':0, 'PUTS_IV':0, 'PUTS_LTP':0, 'PUTS_Net_Chng':0,'PUTS_Bid_Qty':0,'PUTS_Bid_Price':0,'PUTS_Ask_Price':0,'PUTS_Ask_Qty':0}
    if(expiry=="latest"):
        expiry = payload['records']['expiryDates'][0]
    else:
        expiry = payload['records']['expiryDates'][expiry]
    m=0
    for m in range(len(payload['records']['data'])):
        if(payload['records']['data'][m]['expiryDate']==expiry):
            if(1>0):
                try:
                    oi_row['CALLS_OI']=payload['records']['data'][m]['CE']['openInterest']
                    oi_row['CALLS_Chng_in_OI']=payload['records']['data'][m]['CE']['changeinOpenInterest']
                    oi_row['CALLS_Volume']=payload['records']['data'][m]['CE']['totalTradedVolume']
                    oi_row['CALLS_IV']=payload['records']['data'][m]['CE']['impliedVolatility']
                    oi_row['CALLS_LTP']=payload['records']['data'][m]['CE']['lastPrice']
                    oi_row['CALLS_Net_Chng']=payload['records']['data'][m]['CE']['change']
                    if(oi_mode=='full'):
                        oi_row['CALLS_Bid_Qty']=payload['records']['data'][m]['CE']['bidQty']
                        oi_row['CALLS_Bid_Price']=payload['records']['data'][m]['CE']['bidprice']
                        oi_row['CALLS_Ask_Price']=payload['records']['data'][m]['CE']['askPrice']
                        oi_row['CALLS_Ask_Qty']=payload['records']['data'][m]['CE']['askQty']
                except KeyError:
                    oi_row['CALLS_OI'], oi_row['CALLS_Chng_in_OI'], oi_row['CALLS_Volume'], oi_row['CALLS_IV'], oi_row['CALLS_LTP'],oi_row['CALLS_Net_Chng']=0,0,0,0,0,0
                    if(oi_mode=='full'):
                        oi_row['CALLS_Bid_Qty'],oi_row['CALLS_Bid_Price'],oi_row['CALLS_Ask_Price'],oi_row['CALLS_Ask_Qty']=0,0,0,0
                    pass

                oi_row['StrikePrice']=payload['records']['data'][m]['strikePrice']

                try:
                    oi_row['PUTS_OI']=payload['records']['data'][m]['PE']['openInterest']
                    oi_row['PUTS_Chng_in_OI']=payload['records']['data'][m]['PE']['changeinOpenInterest']
                    oi_row['PUTS_Volume']=payload['records']['data'][m]['PE']['totalTradedVolume']
                    oi_row['PUTS_IV']=payload['records']['data'][m]['PE']['impliedVolatility']
                    oi_row['PUTS_LTP']=payload['records']['data'][m]['PE']['lastPrice']
                    oi_row['PUTS_Net_Chng']=payload['records']['data'][m]['PE']['change']
                    if(oi_mode=='full'):
                        oi_row['PUTS_Bid_Qty']=payload['records']['data'][m]['PE']['bidQty']
                        oi_row['PUTS_Bid_Price']=payload['records']['data'][m]['PE']['bidprice']
                        oi_row['PUTS_Ask_Price']=payload['records']['data'][m]['PE']['askPrice']
                        oi_row['PUTS_Ask_Qty']=payload['records']['data'][m]['PE']['askQty']
                except KeyError:
                    oi_row['PUTS_OI'], oi_row['PUTS_Chng_in_OI'], oi_row['PUTS_Volume'], oi_row['PUTS_IV'], oi_row['PUTS_LTP'],oi_row['PUTS_Net_Chng']=0,0,0,0,0,0
                    if(oi_mode=='full'):
                        oi_row['PUTS_Bid_Qty'],oi_row['PUTS_Bid_Price'],oi_row['PUTS_Ask_Price'],oi_row['PUTS_Ask_Qty']=0,0,0,0
            else:
                print("fail")

            if(oi_mode=='full'):
                oi_row['CALLS_Chart'],oi_row['PUTS_Chart']=0,0
            #oi_data = oi_data.append(oi_row, ignore_index=True)
            #oi_data = pd.concat([oi_data, oi_row], ignore_index=True)
            oi_data = pd.concat([oi_data, pd.DataFrame([oi_row])], ignore_index=True)



    oi_data['time_stamp']=payload['records']['timestamp']
    oi_data['time_stamp'] = pd.to_datetime(oi_data['time_stamp'], format="%d-%b-%Y %H:%M:%S")
    oi_data[symbol.lower()]=payload['records']['underlyingValue']
    oi_data["ceall"]=oi_data["CALLS_OI"].sum()
    oi_data["peall"]=oi_data["PUTS_OI"].sum()
    if oi_data["PUTS_OI"].sum()>0 and oi_data["CALLS_OI"].sum()>0:
        oi_data["pcrall"]=oi_data["PUTS_OI"].sum()/oi_data["CALLS_OI"].sum()
    else:
        oi_data["pcrall"] = 0
    oi_data["pcr"]=0
    for i in range(len(oi_data)):
        if oi_data["PUTS_OI"].iloc[i]>0 and oi_data["CALLS_OI"].iloc[i]>0:
            oi_data["pcr"].iloc[i] = oi_data["PUTS_OI"].iloc[i]/oi_data["CALLS_OI"].iloc[i]
        else:
            oi_data["pcr"].iloc[i]=0
            
    from datetime import datetime
    
    return oi_data,float(payload['records']['underlyingValue']),datetime.strptime(payload['records']['timestamp'], '%d-%b-%Y %H:%M:%S')

def dftosql(df,databasename="nifty",tablename="expiryhoge"):
#     create database
    global create_tablesql
    createDatabase = "CREATE DATABASE if NOT EXISTS " + databasename
    mycursor.execute(createDatabase)
    mydb.commit()
    # create table
    create_tablesql = "CREATE TABLE IF NOT EXISTS "+databasename+"."+tablename+" ("
    for i in range(0,len(df.columns)):
        typ = type(df[df.columns[i]][math.ceil(len(df)/2)])
        if typ ==int:
            temp ="INT NOT NULL DEFAULT '0'"
        elif typ == numpy.float64:
            temp = "Float NOT NULL DEFAULT '0'"
        elif typ == pd._libs.tslibs.timestamps.Timestamp:
            temp = "datetime NOT NULL"
        elif typ == numpy.int64:
            temp = "INT NOT NULL DEFAULT '0'"
        elif typ == float:
            temp = "Float NOT NULL DEFAULT '0'"
        else:
            print(typ)
            break
        create_tablesql = create_tablesql+"`"+df.columns[i]+"` "+temp+", "
    create_tablesql = create_tablesql[:-2]
    create_tablesql = create_tablesql+",PRIMARY KEY (`time_stamp`,`strikeprice`))  ENGINE=MYISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
    mycursor.execute(create_tablesql)
    mydb.commit()
    # create insertsql 
    insertsql = "INSERT INTO  "+databasename+"."+tablename+" ("
    insertsql
    for i in range(0,len(df.columns)):
        insertsql = insertsql+df.columns[i]+","
    insertsql=insertsql[:-1]

    insertsql = insertsql+") VALUES ("
    for i in range(0,len(df.columns)):
        insertsql = insertsql+"%s,"
    insertsql=insertsql[:-1]

    insertsql = insertsql+") ON DUPLICATE KEY UPDATE  "
    for i in range(0,len(df.columns)):
        insertsql = insertsql+df.columns[i]+"=%s,"
    insertsql = insertsql[:-1]
    tuples = [tuple(x) for x in df.to_numpy()]
    # data save
    for data in tuples:
#         print("mycursor.execute(insertsql,data+data")
#         print(insertsql,data+data)
        mycursor.execute(insertsql,data+data)

if __name__ == '__main__':
    print("START")
    index_list = ["ACC","BANKNIFTY","FINNIFTY","MIDCPNIFTY"]
    b =0
    a = True
    while True:
        print("START WHILE LOOP:  ",b)
        b = b+1
        x = datetime.now()
        a = int(x.strftime("%H%M"))<1540 and int(x.strftime("%H%M"))>812 and x.strftime("%w")!="6" and x.strftime("%w")!="0" 
        if a==False:
            start = datetime.now() 
            end = start+timedelta(days = 1)
            end = datetime.strptime(end.strftime("%Y-%m-%d")+" 08:30:31", "%Y-%m-%d %H:%M:%S") 

            difference = end - start 

            seconds = round(difference.total_seconds())
            if b >5:
                print("sleep to "+end.strftime("%Y-%m-%d")+" 08:30:31  "+str(seconds)+" sec")
                time.sleep(seconds)
        import time
        for index in index_list:
            try:
                databasename = "nse"+index+"may24"
                obj = OptionChain(index)
                data = obj.fetch_data()
                for date in range(len(data["records"]["expiryDates"])):
                    tablename = data["records"]["expiryDates"][date].replace("-","")
                    df,ltp,Time = oi_chain_builder(index,data,date)
                    if len(df)<10:
                        continue
                    dftosql(df,databasename,tablename)
                    print(databasename+"."+tablename,Time)
            except:       
                    print("fail:-"+str(b)+index)
                    time.sleep(.5)
        if index == "MIDCPNIFTY":
            import time
            print("sleep 30.5 sec")
            # time.sleep(30.5)
        