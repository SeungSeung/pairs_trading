# pairs_trading

## pairs trading project에서 구현한 code 및 백테스팅을 정리하는 repo입니다.

### 1. Backtesting
backtesting은 각각 distance approach와 cointegration approach를 해봄.

### 2. trading bot

    1) 현선물 공통된 ticker를 이용하여 거래 -> 각각 엥겔-그레인저 검정을 통하여 pair selection

    2) pair의 spread가 과거 데이터의 평균 +2*표준편차 만큼 벌어졌을 때 포지션 진입
    과거 데이터를 이용하여 구한 Beta를 이용하여 현재 spread가 역전 되었을 때 청산

    3) 수렴 속도 개념 도입. 
        a)수렴속도(v)=1/반감기 -> 반감기는 spread가 절반으로 줄어드는데 걸리는 시간. 
        
        b) spread가 공적분검정을 기각하였으므로 I(0) 시계열 이므로 간단하게 AR(1) 모형을 이용하여 반감기 공식을 적용.

        
<pre>
<code>
    -np.log(2)/v
    ##여기서 v는 ar(1) 모형의 계수
</code>
</pre>

        c) 수렴 속도가 빠를 수록 상대적으로 더 큰 레버리지, 더 많이 진입


    4)병렬 처리
        a)대략 139개의 티커를 이용하여 pair selection을 하는 과정을 ray를 이용하여 병렬로 처리
<pre>
<code>
@ray.remote
def pair_selection(ticker,y=future_panel_minute,x=coin_panel_minute):
    y,x=y[ticker].values,x[ticker].values
    if coint(y,x,maxlag=12)[0]<-2.58:
        results=sm.OLS(y,x).fit()
        beta=results.params[0]
        spread=results.resid
        spread=pd.Series(spread,name='spread')
        velo=-np.log(2)/DFGLS(spread).regression.params['Level.L1']
        threshold=spread.std()*2+spread.mean()
        return (ticker,beta,spread,velo,threshold)
</code>
</pre>

    5)zero passing
        a) 공적분 검정을 통과한 spread는 white noise 이므로 zero passing(즉 spread가 0을 단위 시간동안 얼마나 통과하는지) 개념을 도입
        
        b) zero passing이 클 수록 단위시간당 그만큼 많이 회귀한다는 것이므로 수익을 볼 가능성이 큼 -> 비교적 큰 레버리지, 큰 포지션



