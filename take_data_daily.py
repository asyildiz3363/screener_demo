import streamlit as st
import pandas as pd
import sqlalchemy
import ta
import numpy as np
import yfinance as yf
import sqlalchemy
import ccxt
import time
import altair as alt
import pandas_ta as pa

start = time.perf_counter() 
st.title('Screener')
@st.cache(suppress_st_warning=True,ttl=24*60*60*1000)
def getdata():
    exchange=ccxt.currencycom()
    markets= exchange.load_markets()    
    symbols1=pd.read_csv('csymbols.csv',header=None)
    symbols=symbols1.iloc[:,0].to_list()
    index = 0
    fullnames=symbols1.iloc[:,1].to_list()
    engine=sqlalchemy.create_engine('sqlite:///günlük.db')
    #enginew=sqlalchemy.create_engine('sqlite:///haftalik.db')
    with st.empty():
        for ticker,fullname in zip(symbols,fullnames):
            index += 1
            try:
                data2 = exchange.fetch_ohlcv(ticker, timeframe='1d',limit=205) #since=exchange.parse8601('2022-02-13T00:00:00Z'))
                #data3= exchange.fetch_ohlcv(ticker, timeframe='1w',limit=55)
                st.write(f"⏳ {index,ticker} downloaded")
            #st.write(index,ticker,end="\r")     
            except Exception as e:
                print(e)
            else:
                header = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
                dfc = pd.DataFrame(data2, columns=header)
                dfc['Date'] = pd.to_datetime(dfc['Date'],unit='ms')
                dfc['Date'] = dfc['Date'].dt.strftime('%d-%m-%Y')
                dfc.to_sql(fullname,engine, if_exists='replace')
                #dfc2 = pd.DataFrame(data3, columns=header)
                #dfc2['Date'] = pd.to_datetime(dfc2['Date'],unit='ms')
                #dfc2.to_sql(fullname,enginew, if_exists='replace')

        index += 1
        bsymbols1=pd.read_csv('bsymbols.csv',header=None)
        bsymbols=bsymbols1.iloc[:,0].to_list()
        for bticker in bsymbols:
            #print(index,bticker,end="\r")
            st.write(f"⏳ {index,bticker} downloaded")
            index += 1
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
        now=pd.Timestamp.now().strftime("%d-%m-%Y, %H:%M")
        st.write('Last downloaded', index,ticker,now)
        return(index,ticker,now)
lastindex=getdata()
st.write('Last downloaded', lastindex)
end = time.perf_counter() 
st.write(end - start)

def MACDdecision(df):
    df['MACD_diff']= ta.trend.macd_diff(df.Close)
    df['MACD']= ta.trend.macd(df.Close)
    df.loc[(df.MACD_diff>0) & (df.MACD_diff.shift(1)<0),'Decision MACD']='Buy'
    df.loc[(df.MACD_diff<0) & (df.MACD_diff.shift(1)>0),'Decision MACD']='Sell'

def EMA_decision(df):
    df['EMA50'] = ta.trend.ema_indicator(df.Close,window=50)
    df['EMA200'] = ta.trend.ema_indicator(df.Close,window=200)
    df.loc[(df.Close>df['EMA200']), 'Decision EMA200'] = 'Buy'
    df.loc[(df.Close<df['EMA200']), 'Decision EMA200'] = 'Sell'
    df.loc[(df.Close>df.EMA50)& (df.Close.shift(1)<df.EMA50.shift(1)), 'Decision EMA50_cross'] = 'Buy'
    df.loc[(df.Close<df.EMA50)& (df.Close.shift(1)>df.EMA50.shift(1)), 'Decision EMA50_cross'] = 'Sell'
    df.loc[(df.Close>df.EMA200)& (df.Close.shift(1)<df.EMA200.shift(1)), 'Decision EMA200_cross'] = 'Buy'
    df.loc[(df.Close<df.EMA200)& (df.Close.shift(1)>df.EMA200.shift(1)), 'Decision EMA200_cross'] = 'Sell'

def ADX_decision(df):
    df['ADX']= ta.trend.adx(df.High, df.Low, df.Close)
    df.loc[(df.ADX>df.ADX.shift(1)) & (df.ADX>=18),'Decision ADX']='Buy'

def Supertrend(df):
    df['sup']=pa.supertrend(high=df['High'],low=df['Low'],close=df['Close'],length=10,multiplier=1)['SUPERTd_10_1.0']
    df.loc[(df.sup==1)&(df.sup.shift(1)==-1), 'Decision Super'] = 'Buy'
    df.loc[(df.sup==-1)&(df.sup.shift(1)==1), 'Decision Super'] = 'Sell'

@st.cache(allow_output_mutation=True)
def connect_engine(url):
    engine=sqlalchemy.create_engine(url) 
    return engine
start = time.perf_counter()
@st.cache(hash_funcs={sqlalchemy.engine.base.Engine:id},suppress_st_warning=True)
def get_framelist(engine):
    names= pd.read_sql('SELECT name FROM sqlite_master WHERE type="table"',engine)   
    names = names.name.to_list()
    framelist=[]
    for name in names:
        framelist.append(pd.read_sql(f'SELECT Date,Close,High,Low FROM "{name}"',engine))   
    np.seterr(divide='ignore', invalid='ignore')
    sira=0
    for name, frame in zip(names,framelist): 
        if len(frame)>30:
            MACDdecision(frame)
            EMA_decision(frame)
            ADX_decision(frame)
            Supertrend(frame)
            print(name)
            print(frame)
            sira +=1
            print(sira)
            st.write(sira)
    return framelist        
end = time.perf_counter() 
st.write(end - start)
def get_names(engine):
    st.write('isimler alınıyor')
    names= pd.read_sql('SELECT name FROM sqlite_master WHERE type="table"',engine)
    names = names.name.to_list()
    return names
connection_url='sqlite:///günlük.db'
engine= connect_engine(connection_url) 
framelist1=get_framelist(engine) 
names1=get_names(engine)
 
option = st.sidebar.selectbox("Which Indicator?", ('EMA', 'MACD','SUPERTREND'))
min_value=18
adx_value= st.sidebar.number_input('ADX Value',min_value)
st.header(option)
if option == 'EMA':
    sira=0
    for name, frame in zip(names1,framelist1):   
        try:
            if len(frame)>30 and frame['Decision EMA200'].iloc[-1]=='Buy' \
            and frame['ADX'].iloc[-1]>=adx_value and frame['MACD_diff'].iloc[-1]>0 and frame['MACD'].iloc[-1]<0 \
            and frame['Decision EMA50_cross'].iloc[-1]=='Buy': 
                sira +=1
                st.write(str(sira)+" Buying EMA50 for "+ name)
                #st.write(frame)
                st.line_chart(frame[['Close', 'EMA50']])
            elif len(frame)>30 and frame['Decision EMA200'].iloc[-1]=='Buy' \
            and frame['ADX'].iloc[-1]>=adx_value and frame['MACD_diff'].iloc[-1]>0 and frame['MACD'].iloc[-1]<0 \
            and frame['Decision EMA200_cross'].iloc[-1]=='Buy': 
                sira +=1
                st.write(str(sira)+" Buying EMA200 for "+ name)
            elif len(frame)>30 and frame['Decision EMA200'].iloc[-1]=='Sell' \
            and frame['ADX'].iloc[-1]>=adx_value and frame['MACD_diff'].iloc[-1]<0 and frame['MACD'].iloc[-1]>0 \
            and frame['Decision EMA50_cross'].iloc[-1]=='Sell' :    
                sira +=1
                st.write(str(sira)+" Selling EMA50 for "+ name)
            elif len(frame)>30 and frame['Decision EMA200'].iloc[-1]=='Sell' \
            and frame['ADX'].iloc[-1]>=adx_value and frame['MACD_diff'].iloc[-1]<0 and frame['MACD'].iloc[-1]>0 \
            and frame['Decision EMA200_cross'].iloc[-1]=='Sell':    
                sira +=1
                st.write(str(sira)+" Selling EMA200 for "+ name)
        except Exception as e:
            st.write(name,e)
if option == 'MACD':
    sira=0
    for name, frame in zip(names1,framelist1):
        try:   
            if  len(frame)>30 and frame['Decision MACD'].iloc[-1]=='Buy' and frame['MACD'].iloc[-1]<0 \
            and frame['Decision EMA200'].iloc[-1]=='Buy' and frame['ADX'].iloc[-1]>=adx_value:
                sira +=1
                st.write(str(sira)+" Buying Signal MACD/EMA200 for "+ name)
            elif len(frame)>30 and frame['Decision MACD'].iloc[-1]=='Sell' and frame['MACD'].iloc[-1]>0 \
            and frame['Decision EMA200'].iloc[-1]=='Sell' and frame['ADX'].iloc[-1]>=adx_value :
                sira +=1
                st.write(str(sira)+" Selling Signal MACD/EMA200 for "+ name)
        except Exception as e:
            st.write(name,e)
if option == 'SUPERTREND':
    sira=0
    for name, frame in zip(names1,framelist1):   
        try:
            if len(frame)>30 and frame['Decision Super'].iloc[-1]=='Buy' and frame['Decision EMA200'].iloc[-1]=='Buy' \
            and frame['ADX'].iloc[-1]>=adx_value and frame['MACD_diff'].iloc[-1]>0 and frame['MACD'].iloc[-1]<0 :
                sira +=1
                st.write(str(sira)+" Buy Supertrend for "+ name)
            elif len(frame)>30 and frame['Decision Super'].iloc[-1]=='Sell' and frame['Decision EMA200'].iloc[-1]=='Sell' \
            and frame['ADX'].iloc[-1]>=adx_value and frame['MACD_diff'].iloc[-1]<0 and frame['MACD'].iloc[-1]>0:
                sira +=1
                st.write(str(sira)+" Sell Supertrend for "+ name)
        except Exception as e:
            st.write(name,e)  
