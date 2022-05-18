import pandas as pd
import numpy as np
import ccxt
import datetime
import time

from tqdm import tqdm
from pprint import pprint
from math import ceil
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import coint
import statsmodels.api as sm
from trading.utils import *
binance_futures= ccxt.binance(config={
    'apiKey': 'apiKey', 
    'secret': 'secret',
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future'
    }
})




binance = ccxt.binance(config={
    'apiKey': 'apiKey',
    'secret': 'secret',
    'enableRateLimit': True,
})

"""
pair trading을 해야하므로 우리는 각 코인 및 선물의 가격 데이터를 가져온 후 공적분 검증을 해봐야한다. 

"""

#1 각 시장의 ticker를 가져온다
tickers=get_tickers(binance=binance,binance_futures=binance_futures)
#2 아제 데이터 셋을 만들어 보자
#2-1 선물
print('선물 dataframe\n')
future_panel_minute=get_future_panel(binance_futures=binance_futures,tickers=tickers)
future_panel_minute=future_panel_minute.apply(lambda x : x.fillna(method='ffill'))

# 맨 윗줄 날리기~
#future_panel_minute = future_panel_minute.iloc[2:]

# 2-2 현물

    
print('현물 dataframe\n')
coin_panel_minute=get_coin_panel(binance=binance,tickers=tickers)
coin_panel_minute=coin_panel_minute.apply(lambda x : x.fillna(method='ffill'))

# 맨 윗줄 날리기~
#coin_panel_minute = coin_panel_minute.iloc[2:]

###########################################trading!#####################


funding_target=0
funding=dict()

for ticker in tickers:
    funding[ticker]=get_funding_rate(binance_futures,ticker=ticker)

flag=1
while True:
    # 맨 윗줄 날리기~
    coin_panel_minute = coin_panel_minute.iloc[2:]
    future_panel_minute = future_panel_minute.iloc[2:]
    now=datetime.datetime.now()
    if (now.hour==9) or (now.hour==13) or (now.hour==17):
        if flag==1:
            for ticker in tickers:
                funding[ticker]=get_funding_rate(binance_futures,ticker=ticker)
            flag=0
    if (now.hour==8) or (now.hour==12) or (now.hour==16):
        flag=1 
    #####공적분 검정######
    #coin_scaled,future_scaled=mm_scaler(coin_panel_minute),mm_scaler(future_panel_minute)
    



  ###진입
    #buy_tickers=[]
    coin_pair=dict()
    s=2
    future_pair=dict()
    beta_dict=dict()
    print('매수를 시작합니다.\n')
    for ticker in tqdm(tickers):
        balance = binance.fetch_balance()
        balance_futures=binance_futures.fetch_balance()
        if (balance['USDT']['free']>30) and (balance_futures['USDT']['free']>30):
            if E_Gtest(coin_panel_minute[ticker],future_panel_minute[ticker])<-2.58:
                beta=get_beta(coin_panel_minute[ticker].values,future_panel_minute[ticker].values)
                spread=get_spread(coin_panel_minute[ticker].values,future_panel_minute[ticker].values)
                threshold1=spread.std()*s+spread.mean()
                threshold2=spread.std()*-s+spread.mean()
                beta_dict[ticker]=beta

                if (get_futures_price(binance_futures=binance_futures,ticker=ticker)-get_spot_price(binance=binance,ticker=ticker)*beta_dict[ticker]>threshold1) and (funding[ticker]>funding_target):
                    if (balance['USDT']['free']>get_futures_price(binance_futures=binance_futures,ticker=ticker)) and (balance_futures['USDT']['free']>get_spot_price(binance=binance,ticker=ticker)):
                        try:
                            c_amount=coin_amount(ticker=ticker,binance=binance,beta=beta)
                            order_spot=spot_long(binance=binance,ticker=ticker,amount=c_amount)
                            pprint(order_spot)
                            f_amount=future_amount(binance_futures=binance_futures,ticker=ticker)
                            short=futures_short(binance_futures=binance_futures,ticker=ticker,amount=f_amount)
                            pprint(short)
                            beta_dict[ticker]=beta
                            if ticker not in coin_pair.keys():
                                coin_pair[ticker]=c_amount
                                future_pair[ticker]=f_amount
                            else:
                                coin_pair[ticker]+=c_amount
                                future_pair[ticker]+=f_amount
                            print(f'매수 티커: {ticker}')
                        except Exception as e:
                            print(f'매수 티커: {ticker} ' + str(e))
                            #buy_tickers.append(ticker)

                        
                    time.sleep(1)
    
    
    ###청산
    buy_tickers=list(coin_pair.keys())
    if len(buy_tickers)!=0:
        print('청산 조건에 맞는지 검증합니다\n')
        for ticker in tqdm(buy_tickers):
            if get_futures_price(binance_futures=binance_futures,ticker=ticker)-get_spot_price(binance=binance,ticker=ticker)*beta_dict[ticker]<=0 or funding[ticker]<=0:
                try:
                    close_short=future_close_position(binance_futures=binance_futures,ticker=ticker,amount=future_pair[ticker])
                    pprint(close_short)
                    close_spot=spot_long_close(binance=binance,ticker=ticker,amount=coin_pair[ticker])
                    pprint(close_spot)
                    del buy_tickers[buy_tickers.index(ticker)]
                    coin_pair.pop(ticker)
                    future_pair.pop(ticker)
                    beta.pop(ticker)
                    print(f'청산 티커: {ticker}')
                except Exception as e:
                    print(e, ticker)

    coin_panel_minute=get_coin_panel(binance=binance,tickers=tickers)
    future_panel_minute=get_future_panel(binance_futures=binance_futures,tickers=tickers)
    tickers=get_tickers(binance=binance,binance_futures=binance_futures)
    time.sleep(300)