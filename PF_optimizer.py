

# Author  : 유상균
# Contact : sglyu0414@gmail.com
# Date    : 2022-10-13(목)

# !pip install pyportfolioopt
# !pip install yfinance



# Required Modules
import pandas   as pd
import numpy    as np
import yfinance as yf

from pypfopt.expected_returns    import ema_historical_return
from pypfopt.risk_models         import exp_cov
from pypfopt.efficient_frontier  import EfficientFrontier
from pypfopt.discrete_allocation import DiscreteAllocation, get_latest_prices
from pypfopt.efficient_frontier  import EfficientCVaR
from pypfopt                     import HRPOpt





# yfinance를 통해 종목을 받아오는 단계
ticker = ['003230.ks','033780.ks','017670.ks','003550.ks','010950.ks','078930.ks']
# ticker = ['TSLA','T','KO','XOM','AAPL'] 해외
df = yf.download(ticker, start = '2021-01-01', end='2022-09-30')[['Close']]
df = df.dropna()
df.columns = ['삼양식품', 'KT&G','SK텔레콤','LG유플러스','S오일','GS']
# df.columns = ['테슬라', 'AT&T','코카콜라','엑손모빌','애플'] 해외


# mu = 과거 수익률 평균
# S = 공분산

mu = ema_historical_return(df, compounding=True, log_returns=False)
S = exp_cov(df)


#### 아래 방법으로는 마이너스 수익률이 예상되는 포트폴리오가 들어올 경우 오류 남

# from pypfopt.expected_returns import mean_historical_return
# from pypfopt.risk_models import CovarianceShrinkage
# mu = mean_historical_return(df,compounding=True,log_returns=False)
# S = CovarianceShrinkage(df).ledoit_wolf()


# 샤프지수 기반 효율적 프론티어를 이용한 위험 회피 및 종목별 매수 주식 수
ef = EfficientFrontier(mu, S)
weights = ef.max_sharpe()

cleaned_weights = ef.clean_weights()
print(dict(cleaned_weights))

ef.portfolio_performance(verbose=True)


latest_prices = get_latest_prices(df)

da = DiscreteAllocation(weights, latest_prices, total_portfolio_value=10000000) # 샤프지수 max기준 비중, 전일 종가, 투자할 금액

allocation, leftover = da.greedy_portfolio()
print("종목별 매수할 주식 수:", allocation)
print("남은 시드머니: {:.2f}원".format(leftover))


# 자산 간 상관관계를 이용한 위험 회피 및 종목별 매수 주식 수
returns = df.pct_change().dropna()
hrp = HRPOpt(returns)
hrp_weights = hrp.optimize()
hrp.portfolio_performance(verbose=True)
print(dict(hrp_weights))

da_hrp = DiscreteAllocation(hrp_weights, latest_prices, total_portfolio_value=10000000)

allocation, leftover = da_hrp.greedy_portfolio()
print("종목별 매수할 주식 수 (HRP):", allocation)
print("남은 시드머니 (HRP): {:.2f}원".format(leftover))


# 자산 별 최악의 시나리오 가정을 이용한 위험 회피
# 수익률이 안 좋은 순서대로 n개를 구해서 낸 평균 = Conditional Value ar Risk
S = df.cov()
ef_cvar = EfficientCVaR(mu, S)
cvar_weights = ef_cvar.min_cvar()
ef_cvar.portfolio_performance(verbose=True)

cleaned_weights = ef_cvar.clean_weights()
print(dict(cleaned_weights))

da_cvar = DiscreteAllocation(cvar_weights, latest_prices, total_portfolio_value=10000000)

allocation, leftover = da_cvar.greedy_portfolio()
print("종목별 매수할 주식 수 (CVAR):", allocation)
print("남은 시드머니 (CVAR): {:.2f}원".format(leftover))



