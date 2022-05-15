import pandas as pd
import numpy as np
import ccxt
import datetime
import time

from tqdm import tqdm
from math import ceil
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import coint
import statsmodels.api as sm


####future price panel###
def get_future_panel(binance_futures,tickers):
    btc_ohlcv  = binance_futures.fetch_ohlcv("BTC/USDT", timeframe='1m')

    df = pd.DataFrame(btc_ohlcv, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
    df['datetime'] = pd.to_datetime(df['datetime'],unit='ms')
    df.set_index('datetime', inplace=True)

    future_panel_minute=pd.DataFrame(index=df.index,columns=tickers)
    for ticker in tqdm(tickers):
        ohlcv=binance_futures.fetch_ohlcv(ticker, timeframe='1m')
        df1 = pd.DataFrame(ohlcv, columns=['datetime', 'open', 'high', 'low', 'close','volume'])
        df1['datetime'] = pd.to_datetime(df1['datetime'],unit='ms')
        df1.set_index('datetime', inplace=True)
        df1=df1['close'].copy()
        if len(df1)==500:
            future_panel_minute[ticker]=df1.values
        else:
            future_panel_minute[ticker]=np.nan
    return future_panel_minute

####coin price panel###

def get_coin_panel(binance,tickers):
    btc_ohlcv = binance.fetch_ohlcv("BTC/USDT", timeframe='1h')

    df2 = pd.DataFrame(btc_ohlcv, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
    df2['datetime'] = pd.to_datetime(df2['datetime'],unit='ms')
    df2.set_index('datetime', inplace=True)
    

    coin_panel_minute=pd.DataFrame(index=df2.index,columns=tickers)
    for ticker in tqdm(tickers):
        ohlcv=binance.fetch_ohlcv(ticker, timeframe='1h')
        df1 = pd.DataFrame(ohlcv, columns=['datetime', 'open', 'high', 'low', 'close','volume'])
        df1['datetime'] = pd.to_datetime(df1['datetime'],unit='ms')
        df1.set_index('datetime', inplace=True)
        df1=df1['close'].copy()
        if len(df1)==500:
            coin_panel_minute[ticker]=df1.values
        else:
            coin_panel_minute[ticker]=np.nan
    return coin_panel_minute


###coint test###
def E_Gtest(y,x):
    return coint(y,x,maxlag=12)[0]

###get beta###
def get_beta(y,x):
    results=sm.OLS(y,x).fit()
    beta=results.params[0]
    return beta

def adf_test(x,cutoff=0.01):
    pvalue=adfuller(x)[1]
    if pvalue<cutoff:
        print('stationary')
    else:
        print('no')

####get error terms###
def get_spread(y,x):
    results=sm.OLS(y,x).fit()
    spread=results.resid
    spread=pd.Series(spread,name='spread')
    return spread


def find_distance(a,b):
    dist=np.linalg.norm(a-b)
    return dist
    
def mm_scaler(df):
    for i in df.columns:
        df[i]=(df[i]-np.min(df[i]))/(np.max(df[i])-np.min(df[i]))
    return df


def get_futures_price(binance_futures,ticker):
    futures_price = binance_futures.fetch_ticker(ticker)
    return futures_price['last']

def get_spot_price(binance,ticker):
    spot_price = binance.fetch_ticker(ticker)
    return spot_price['last']
####determine coin and future amount###
def coin_amount(binance,ticker,beta):
    return int(ceil(20.0/float(get_spot_price(binance,ticker))*beta))

def future_amount(binance_futures,ticker):
    return int(ceil(20.0/float(get_futures_price(binance_futures,ticker))))



####get funding rate###
def get_funding_rate(binance_futures,ticker):
    fund = binance_futures.fetch_funding_rate(symbol=ticker)

    return fund['interestRate']



####excution function####
def spot_long(ticker,amount,binance):
    order=binance.create_market_buy_order(
        symbol=ticker,
        amount=amount)
    return order



def spot_long_close(ticker,amount,binance):
    order=binance.create_market_sell_order(
        symbol=ticker,
        amount=amount)
    return order


def future_close_position(ticker,amount,binance_futures):
    order=binance_futures.create_market_buy_order(
        symbol=ticker,amount=amount,

    )
    return order

def futures_short(ticker,amount,binance_futures):
    binance_futures.set_leverage(2,symbol=ticker,params={"marginMode":"isolated"})
    #binance_futures.set_margin_mode(marginType='isolated', symbol = ticker, params={})
    order=binance_futures.create_market_sell_order(
        symbol=ticker,
        amount=amount)
    return order

####################

##계좌잔고 함수
def balance(binance,a='USDT'):
    balance=binance.fetch_blance()
    return balance[a]

def f_balance(binance,a='USDT'):
    balance = binance.fetch_balance(params={"type": "future"})
    return balance[a]



