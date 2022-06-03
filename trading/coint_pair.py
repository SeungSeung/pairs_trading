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
from arch.unitroot import DFGLS
import os


apiKey = os.environ['apiKey']
secret = os.environ['secret']

binance_futures= ccxt.binance(config={
    'apiKey': apiKey, 
    'secret': secret,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future'
    }
})




binance = ccxt.binance(config={
    'apiKey': apiKey,
    'secret': secret,
    'enableRateLimit': True,
})

"""
pair trading을 해야하므로 우리는 각 코인 및 선물의 가격 데이터를 가져온 후 공적분 검정을 해봐야한다. 

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

max_account=100000
min_account=100
funding_target=0
funding=dict()
velo_dict=dict()
coin_panel_minute = coin_panel_minute.iloc[2:]
future_panel_minute = future_panel_minute.iloc[2:]
for ticker in tickers:
    funding[ticker]=get_funding_rate(binance_futures,ticker=ticker)
    velo_dict[ticker]=get_velo(get_spread(future_panel_minute[ticker].values,coin_panel_minute[ticker].values))

flag=1
while True:
    velo_ticker=sorted(list(velo_dict.values()))
    balance = binance.fetch_balance()
    balance_futures=binance_futures.fetch_balance()
    if (balance['USDT']['total']+balance_futures['USDT']['total']>max_account) or (balance['USDT']['total']+balance_futures['USDT']['total']<min_account):
        break
    # 맨 윗줄 날리기~
    coin_panel_minute = coin_panel_minute.iloc[10:]
    future_panel_minute = future_panel_minute.iloc[10:]

    now=datetime.datetime.now()
    if (now.hour==9) or (now.hour==13) or (now.hour==17):
        if flag==1:
            for ticker in tickers:
                funding[ticker]=get_funding_rate(binance_futures,ticker=ticker)
            flag=0
    if (now.hour==8) or (now.hour==12) or (now.hour==16):
        if flag==0:
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
    time.sleep(20)
    for ticker in tqdm(tickers):
        if (balance['USDT']['free']>30) and (balance_futures['USDT']['free']>30):
            if E_Gtest(coin_panel_minute[ticker],future_panel_minute[ticker])<-2.58:
                beta=get_beta(coin_panel_minute[ticker].values,future_panel_minute[ticker].values)
                spread=get_spread(coin_panel_minute[ticker].values,future_panel_minute[ticker].values)
                ###속도를 구해보자#####
                velo=get_velo(spread)
                threshold1=spread.std()*s+spread.mean()
                threshold2=spread.std()*-s+spread.mean()
                beta_dict[ticker]=beta

                if (get_futures_price(binance_futures=binance_futures,ticker=ticker)-get_spot_price(binance=binance,ticker=ticker)*beta_dict[ticker]>threshold1) and (funding[ticker]>funding_target):
                    if (balance['USDT']['free']>get_spot_price(binance=binance,ticker=ticker)) and (balance_futures['USDT']['free']>get_futures_price(binance_futures=binance_futures,ticker=ticker)):
                        try:
                            print('-'*120)
                            print(f'포지션 진입 ticker:{ticker}')
                            c_amount=coin_amount(ticker=ticker,binance=binance,beta=beta)
                            order_spot=spot_long(binance=binance,ticker=ticker,amount=c_amount)
                            pprint(order_spot)
                            print('-'*120)
                            f_amount=future_amount(binance_futures=binance_futures,ticker=ticker,velo_ticker=velo_ticker,velo_dict=velo_dict)
                            flev=leverage(velo_ticker=velo_ticker,velo_dict=velo_dict,ticker=ticker)
                            short=futures_short(binance_futures=binance_futures,ticker=ticker,amount=f_amount,lev=flev)
                            pprint(short)
                            print('-'*120)
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
    
    time.sleep(30)
    ###청산
    buy_tickers=list(coin_pair.keys())
    if len(buy_tickers)!=0:
        print('청산 조건에 맞는지 검증합니다\n')
        for ticker in tqdm(buy_tickers):
            if get_futures_price(binance_futures=binance_futures,ticker=ticker)-get_spot_price(binance=binance,ticker=ticker)*beta_dict[ticker]<=0 or funding[ticker]<=0:
                try:
                    print(f'청산 티커: {ticker}')
                    close_short=future_close_position(binance_futures=binance_futures,ticker=ticker,amount=future_pair[ticker])
                    pprint(close_short)
                    print('-'*120)
                    close_spot=spot_long_close(binance=binance,ticker=ticker,amount=coin_pair[ticker])
                    pprint(close_spot)
                    print('-'*120)
                    del buy_tickers[buy_tickers.index(ticker)]
                    coin_pair.pop(ticker)
                    future_pair.pop(ticker)
                    beta.pop(ticker)
                except Exception as e:
                    print(e, ticker)
    time.sleep(100)
    tickers=get_tickers(binance=binance,binance_futures=binance_futures)
    coin_panel_minute=get_coin_panel(binance=binance,tickers=tickers)
    future_panel_minute=get_future_panel(binance_futures=binance_futures,tickers=tickers)
    velo_dict=dict()
    for ticker in tickers:
        velo_dict[ticker]=get_velo(get_spread(future_panel_minute[ticker].values,coin_panel_minute[ticker].values))
    coin_panel_minute = coin_panel_minute.iloc[2:]
    future_panel_minute = future_panel_minute.iloc[2:]
    time.sleep(200)

####계좌 잔고에 따른 조건이 만족되었을 때 거래를 종료하고 모든 포지션을 청산한다#######
print('-'*120)
print('자동매매 종료\n')
print('포지션 전부 청산 시작\n')
print('*'*120)
for ticker in tqdm(buy_tickers):
    try:
        print(f'청산 티커: {ticker}')
        close_short=future_close_position(binance_futures=binance_futures,ticker=ticker,amount=future_pair[ticker])
        pprint(close_short)
        close_spot=spot_long_close(binance=binance,ticker=ticker,amount=coin_pair[ticker])
        pprint(close_spot)
        time.sleep(1)
    except Exception as e:
        print(e, ticker)
print('*'*120)
f_total=balance_futures['USDT']['total']
c_total=balance['USDT']['total']
print(f'선물계좌 총액: {f_total}')
print(f'현물계좌 총액: {c_total}')
