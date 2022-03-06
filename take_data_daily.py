import streamlit as st
import pandas as pd
import ccxt
import time
from streamlit_autorefresh import st_autorefresh

start = time.perf_counter() 
st.title('Screener')
st_autorefresh(interval=1 * 60 * 1000, key="dataframerefresh")
def getdata():
    exchange=ccxt.currencycom()
    markets= exchange.load_markets()    
    data2 = exchange.fetch_ohlcv('ETH/USDT', timeframe='1m',limit=24) #since=exchange.parse8601('2022-02-13T00:00:00Z'))
    header = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    dfc = pd.DataFrame(data2, columns=header)
    dfc['Date'] = pd.to_datetime(dfc['Date'],unit='ms')
    dfc['Date'] = dfc['Date'].dt.strftime('%d-%m-%Y %H:%M')
    dfc = dfc.set_index('Date')
    return dfc
getdata()
end = time.perf_counter() 
st.write(end - start)
dfc=getdata()
st.line_chart(dfc[['Close']])             
