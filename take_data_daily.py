import streamlit as st
import pandas as pd
import sqlalchemy
import ta
import numpy as np
import yfinance as yf
import sqlalchemy
import ccxt



st.title('Screener')
@st.cache(suppress_st_warning=True)
def getdata():
    exchange=ccxt.currencycom()
    markets= exchange.load_markets()    
    symbols1=pd.read_csv('csymbols.csv',header=None)
    symbols=symbols1.iloc[:,0].to_list()


    index = 1
    fullnames=symbols1.iloc[:,1].to_list()
    engine=sqlalchemy.create_engine('sqlite:///günlük.db')
    #enginew=sqlalchemy.create_engine('sqlite:///haftalik.db')
    for ticker,fullname in zip(symbols[:10],fullnames[:10]):
        index += 1
        try:
            data2 = exchange.fetch_ohlcv(ticker, timeframe='1d',limit=55) #since=exchange.parse8601('2022-02-13T00:00:00Z'))
            #data3= exchange.fetch_ohlcv(ticker, timeframe='1w',limit=55)
            st.write(ticker)
        except Exception as e:
            print(e)
        else:
            header = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            dfc = pd.DataFrame(data2, columns=header)
            dfc['Date'] = pd.to_datetime(dfc['Date'],unit='ms')
            dfc.to_sql(fullname,engine, if_exists='replace')
            #dfc2 = pd.DataFrame(data3, columns=header)
            #dfc2['Date'] = pd.to_datetime(dfc2['Date'],unit='ms')
            #dfc2.to_sql(fullname,enginew, if_exists='replace')

    index2 = 1
    bsymbols1=pd.read_csv('bsymbols.csv',header=None)
    bsymbols=bsymbols1.iloc[:,0].to_list()
    for bticker in bsymbols[:10]:
        print(index2,bticker,end="\r")
        index2 += 1
        df=yf.download(bticker,period="3mo")
        df2=df.drop('Adj Close', 1)
        df3=df2.reset_index()
        df4=df3.round(2)
        df4.to_sql(bticker,engine, if_exists='replace')
        #dfw=yf.download(bticker,period="55wk",interval = "1wk")
        #df2w=dfw.drop('Adj Close', 1)
        #df3w=df2w.reset_index()
        #df4w=df3w.round(2)
        #df4w.to_sql(bticker,enginew, if_exists='replace')

st.button('Get Data',on_click=getdata())


engine=sqlalchemy.create_engine('sqlite:///günlük.db')

names = pd.read_sql('SELECT name FROM sqlite_master WHERE type="table"',engine)
names = names.name.to_list()


framelist=[]
for name in names[:10]:
    framelist.append(pd.read_sql(f'SELECT Date,Close,High,Low FROM "{name}"',engine))

def MACDdecision(df):
    df['MACD_diff']= ta.trend.macd_diff(df.Close)
    df.loc[(df.MACD_diff>0)& (df.MACD_diff.shift(1)<0),'Decision MACD']='Buy'

def EMA_decision(df):
    df['EMA'] = ta.trend.ema_indicator(df.Close,window=50)
    df.loc[(df.Close>df['EMA']), 'Decision EMA'] = 'Buy'

def ADX_decision(df):
    df['ADX']= ta.trend.adx(df.High, df.Low, df.Close)
    df.loc[(df.ADX>=18),'Decision ADX']='Buy'

np.seterr(divide='ignore', invalid='ignore')
for name,frame in zip(names,framelist):  
    if len(frame)>30:
            MACDdecision(frame)
            EMA_decision(frame)
            print(name)
            print(frame)

option = st.sidebar.selectbox("Which Indicator?", ('MACD', 'EMA'))
st.header(option)

if option == 'MACD':
    sira=0
    for name, frame in zip(names,framelist):
        try:   
            if  len(frame)>30 and frame['Decision MACD'].iloc[-1]=='Buy' \
            and frame['Decision EMA'].iloc[-1]=='Buy':
                sira +=1
                print(str(sira)+" Buying Signal MACD/EMA200 for "+ name) 
        except:
            print(name)
            st.write(name)

if option == 'EMA':
    sira=0
    for name, frame in zip(names,framelist):
        try:   
            if  len(frame)>30 and frame['Decision EMA'].iloc[-1]=='Buy':
                sira +=1
                st.write(str(sira)+" Buying Signal MACD/EMA200 for "+ name) 
        except Exception as e:
            print(name,e)
            st.write(name)
