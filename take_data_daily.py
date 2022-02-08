import pandas as pd
import yfinance as yf
import sqlalchemy
import yfinance as yf
import datetime as dt
import ccxt
import os
import time
import schedule

def job():
    
    exchange=ccxt.currencycom()
    markets= exchange.load_markets()    

    symbols1=pd.read_csv('csymbols.csv',header=None)
    symbols=symbols1.iloc[:,0].to_list()
    symbols

    data = []
    index = 1
    start = time.perf_counter()
    for ticker in symbols[:10]:
        print(index,ticker,end="\r")
        index += 1
        data2= exchange.fetch_ohlcv(ticker, timeframe='1d',limit=251)
        header = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        dfc = pd.DataFrame(data2, columns=header)
        dfc['Date'] = pd.to_datetime(dfc['Date'],unit='ms')
        data.append(dfc)
    end = time.perf_counter()
    print(end - start) 
    
    bdata= []
    index2 = 1
    bsymbols1=pd.read_csv('bsymbols.csv',header=None)
    bsymbols=bsymbols1.iloc[:,0].to_list()
    for bticker in bsymbols:
        print(index2,bticker,end="\r")
        index2 += 1
        df=yf.download(bticker,period="1y")
        df2=df.drop('Adj Close', 1)
        df3=df2.reset_index()
        df4=df3.round(2)
        bdata.append(df4)
    
    fullname=symbols1.iloc[:,1].to_list()
    engine=sqlalchemy.create_engine('sqlite:///günlük.db')

    start = time.perf_counter()
    for frame,symbol in zip(data,fullname):
        frame.to_sql(symbol,engine, if_exists='replace')
    end = time.perf_counter()
    print(end - start) 

    for bframe,bsymbol in zip(bdata,bsymbols):
        bframe.to_sql(bsymbol,engine, if_exists='replace') 


#schedule.every(10).seconds.do(job)
schedule.every(5).minutes.do(job)
#schedule.every().hour.do(job)
#schedule.every().day.at("08:00").do(job)
#schedule.every(5).to(10).minutes.do(job)
#schedule.every().monday.do(job)
# schedule.every().monday.at("09:15").do(job)
# schedule.every().tuesday.at("09:15").do(job)
# schedule.every().wednesday.at("09:15").do(job)
# schedule.every().thursday.at("09:15").do(job)
# schedule.every().friday.at("09:15").do(job)
#schedule.every().minute.at(":17").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
