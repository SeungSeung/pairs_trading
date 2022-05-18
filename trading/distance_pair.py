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
from utils import *
from pprint import pprint
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

tickers=get_tickers(binance=binance,binance_futures=binance_futures)


print('선물 dataframe\n')
future_panel_minute=get_future_panel(binance_futures=binance_futures,tickers=tickers)
future_panel_minute=future_panel_minute.apply(lambda x : x.fillna(method='ffill'))


# 맨 윗줄 날리기~
#future_panel_minute = future_panel_minute.iloc[2:]

print('현물 dataframe\n')
coin_panel_minute=get_coin_panel(binance=binance,tickers=tickers)
coin_panel_minute=coin_panel_minute.apply(lambda x : x.fillna(method='ffill'))

# 맨 윗줄 날리기~
#coin_panel_minute = coin_panel_minute.iloc[2:]
max_account=100000
min_account=100
s=2
funding_target=0
funding=dict()
dist_dict=dict()
future_pair=dict()
coin_pair=dict()
for ticker in tickers:
    funding[ticker]=get_funding_rate(binance_futures,ticker=ticker)
flag=1
while True:
    time.sleep(20)
    balance = binance.fetch_balance()
    balance_futures=binance_futures.fetch_balance()
    if (balance['USDT']['total']+balance_futures['USDT']['total']<max_account) or (balance['USDT']['total']+balance_futures['USDT']['total']<min_account):
        break
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
        if flag==0:
            flag=1 
    
    ###각각 가격을 scaling한다###
    coin_scaled,future_scaled=mm_scaler(coin_panel_minute),mm_scaler(future_panel_minute)
    for tikcer in tickers:
        dist_dict[ticker]=find_distance(coin_scaled[ticker],future_scaled[ticker])
    key_list=list(dist_dict.keys())
    key_list=sorted(key_list)[0:9]
    for dist in tqdm(key_list):
        if (balance['USDT']['free']>30) and (balance_futures['USDT']['free']>30):
            ticker=dist_dict[dist]
            spread=future_panel_minute[ticker]-coin_panel_minute[ticker]
            threshold1=spread.std()*s+spread.mean()
            if (get_futures_price(binance_futures=binance_futures,ticker=ticker)-get_spot_price(binance=binance,ticker=ticker)>threshold1) and (funding[ticker]>funding_target):
                if (balance['USDT']['free']>get_spot_price(binance_futures=binance_futures,ticker=ticker)) and (balance_futures['USDT']['free']>get_futures_price(binance=binance,ticker=ticker)):
                    try:
                        print('-'*70)
                        print(f'포지션 진입 ticker:{ticker}')
                        c_amount=coin_amount(ticker=ticker,binance=binance,beta=1)
                        order_spot=spot_long(binance=binance,ticker=ticker,amount=c_amount)
                        pprint(order_spot)
                        f_amount=future_amount(binance_futures=binance_futures,ticker=ticker)
                        short=futures_short(binance_futures=binance_futures,ticker=ticker,amount=f_amount)
                        pprint(short)
                        print('-'*70)
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
    time.sleep(30)
    buy_tickers=list(coin_pair.keys())
    if len(buy_tickers)!=0:
        print('청산 조건에 맞는지 검증합니다\n')
        for ticker in tqdm(buy_tickers):
            if get_futures_price(binance_futures=binance_futures,ticker=ticker)-get_spot_price(binance=binance,ticker=ticker)<=0 or funding[ticker]<=0:
                try:
                    print(f'청산 티커: {ticker}')
                    close_short=future_close_position(binance_futures=binance_futures,ticker=ticker,amount=future_pair[ticker])
                    pprint(close_short)
                    close_spot=spot_long_close(binance=binance,ticker=ticker,amount=coin_pair[ticker])
                    pprint(close_spot)
                    print('-'*70)
                    del buy_tickers[buy_tickers.index(ticker)]
                    coin_pair.pop(ticker)
                    future_pair.pop(ticker)
                except Exception as e:
                    print(e, ticker)
    time.sleep(150)
    tickers=get_tickers(binance=binance,binance_futures=binance_futures)
    coin_panel_minute=get_coin_panel(binance=binance,tickers=tickers)
    future_panel_minute=get_future_panel(binance_futures=binance_futures,tickers=tickers)
    time.sleep(250)

####계좌 잔고에 따른 조건이 만족되었을 때 거래를 종료하고 모든 포지션을 청산한다#######
print('-'*70)
print('자동매매 종료\n')
print('포지션 전부 청산 시작\n')
print('*'*70)
for ticker in tqdm(buy_tickers):
    try:
        close_short=future_close_position(binance_futures=binance_futures,ticker=ticker,amount=future_pair[ticker])
        pprint(close_short)
        close_spot=spot_long_close(binance=binance,ticker=ticker,amount=coin_pair[ticker])
        pprint(close_spot)
        print(f'청산 티커: {ticker}')
        time.sleep(1)
    except Exception as e:
        print(e, ticker)
print('*'*70)
f_total=balance_futures['USDT']['total']
c_total=balance['USDT']['total']
print(f'선물계좌 총액: {f_total}')
print(f'현물계좌 총액: {c_total}')



    

