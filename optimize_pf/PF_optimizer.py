
# Author  : 유상균
# Contact : sglyu0414@gmail.com
# Date    : 2022-10-20(목)

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
from pypfopt.objective_functions import sharpe_ratio, portfolio_return, portfolio_variance
from sklearn.preprocessing       import minmax_scale


# 종가 데이터 변환 
def convert_to_df():
    """
    데이터베이스로부터 받아온 종목과 각 종목의 날짜별 종가를 데이터프레임 형식으로 반화한다.
    [Parameters]
    [Returns]
    DataFrame : 각 종목이름을 칼럼명으로, 날짜를 인덱스로 갖는 종목별 종가
    """
    pass
    return stocks_close


# 회원 포트폴리오 데이터프레임으로 변환
def convert_to_my_pf():
    """
    데이터베이스로부터 받아온 회원이 보유한 종목수와 평균단가를 이용하여
    종목별 비중의 퍼센트값과 종목별 종가, 시드머니 총액을 반환한다.
    [Parameters]
    [Returns]
    list      : 종목별 비중
    DataFrame : 각 종목이름을 칼럼명으로, 날짜를 인덱스로 갖는 종목별 종가
    int       : 시드머니
    """
    pass
    return weights, stocks_close, seed_money
    

# 회원 포트폴리오 평가
def evaluate_my_pf(weights, stocks_close):
    """
    회원의 포트폴리오 구성종목별 비중과 각 종목의 종가를 입력받아 
    기존 포트폴리오의 연간 수익률과 샤프지수 기반 포트폴리오의 점수를 반환한다.
    [Parameters]
    weights            (list) : 각 종목별 비중
    stocks_close  (DataFrame) : 각 종목이름을 칼럼명으로, 날짜를 인덱스로 갖는 종목별 종가
    [Returns]
    float64 : 포트폴리오의 수익률
    int     : 포트폴리오의 점수
    """
    stocks_return = ema_historical_return(stocks_close, compounding=True, log_returns=True)
    pf_cov = exp_cov(stocks_close, log_returns=True)
    pf_var = portfolio_variance(weights, pf_cov)
    pf_sharpe = sharpe_ratio(weights, stocks_return, pf_cov, negative=False)
    pf_ef = EfficientFrontier(stocks_return, pf_cov)
    pf_weights_mod = pf_ef.max_sharpe()
    pf_cleaned_weights = pf_ef.clean_weights()
    max_sharpe = pf_ef.portfolio_performance()[2]
    pre_score = np.array([0, pf_sharpe.value, max_sharpe])
    scaled_pre_score = minmax_scale(pre_score)
    
    pf_return = portfolio_return(weights, stocks_return, negative=False)
    score = round(scaled_pre_score[1] * 100)
    
    return pf_return, score


# 포트폴리오 최적화
def get_best_pf(stocks_close, seed_money):
    """
    입력받은 주식 종목들간 비중 최적화를 통해 가장 높은 수익률을 가진 포트폴리오와
    가장 위험이 적은 포트폴리오의 비중을 딕셔너리 형식으로 반환한다.
    [Parameters]
    stocks_close  (DataFrame) : 각 종목이름을 칼럼명으로, 날짜를 인덱스로 갖는 종목별 종가
    [Returns]
    dict : 포트폴리오의 수익률
    int     : 포트폴리오의 점수
    """
    stocks_return = ema_historical_return(stocks_close, compounding=True, log_returns=True)
    pf_cov = exp_cov(stocks_close, log_returns=True)
    stocks_return_pct = stocks_close.pct_change().dropna()
    
    # 샤프지수 기반 
    ef = EfficientFrontier(stocks_return, pf_cov)
    ef_weights = ef.max_sharpe()
    ef_cleaned_weights = ef.clean_weights()
    ef_return = round(ef.portfolio_performance()[0] * 100, 2)
    ef_vol = round(ef.portfolio_performance()[1] * 100, 2)
    ef_sharpe = round(ef.portfolio_performance()[2] * 100, 2)
    
    
    # 위험회피 기반
    hrp = HRPOpt(stocks_return_pct)
    hrp_weights = hrp.optimize()
    hrp_return = round(hrp.portfolio_performance()[0] * 100, 2)
    hrp_vol = round(hrp.portfolio_performance()[1] * 100, 2)
    hrp_sharpe = round(hrp.portfolio_performance()[2] * 100, 2)
    
    
    # CVaR 기반 변동성 장에서는 꼬리위험이 지나치게 크게 나오기 때문에 부적절
#     cvar_cov = stock_close.cov()
#     ef_cvar = EfficientCVaR(stocks_return, cvar_cov)
#     cvar_weights = ef_cvar.min_cvar()
#     ef_cvar.portfolio_performance
    
    # 샤프지수 전략에 따라 매수할 주식 수와 남는 돈
    latest_prices = get_latest_prices(stocks_close)
    ef_da = DiscreteAllocation(ef_weights, latest_prices, total_portfolio_value = seed_money)
    ef_allocation, ef_leftover = ef_da.greedy_portfolio()
    
    # 위험회피 전략에 따라 매수할 주식 수와 남는 돈
#     hrp_da = DiscreteAllocation(hrp_weights, latest_prices, total_portfolio_value= seed_money)
#     hrp_allocation, hrp_leftover = hrp_da.greedy_portfolio()

    return ef_allocation, ef_leftover/100


# 포트폴리오에 편입할 종목 추천
def recommend_stocks(allocation, stocks_close, stocks_close_all):
    """
    최적화된 포트폴리오 비중과 종목들의 종가를 받아 개별 종목간 상관계수를 기반으로 5개의 종목 추천
    [Parameters]
    allocation            (dict) : 종목이름 : 주식수 로 이루어진 딕셔너리
    stocks_close     (DataFrame) : 각 종목이름을 칼럼명으로, 날짜를 인덱스로 갖는 종목별 종가
    stocks_close_all (DataFrame) : 모든 종목이름을 칼럼명으로, 날짜를 인덱스로 갖는 종목별 종가
    [Returns]
    list : 추천하는 다섯개의 종목
    """
    stocks = list(allocation.keys())
    weights = list(allocation.values())
    stocks_name = list(stocks_close.columns)
    stocks_close['pf_total_price'] = 0
    for idx, stock_name in enumerate(stocks):
        stocks_close['pf_total_price'] += stocks_close[columns] * weights[idx]
        
    pf_close = stocks_close['pf_total_price'].reset_index().dropna()
    stocks_close_all = stocks_close_all.reset_index().dropna()
    
    pf_cor = pd.concat([pf_close, stocks_close_all], axis =1)
    recommend = pf_cor.corr()[['pf_total_price']].sort_values(by='pf_total_price')
    rec_stocks = list(recommend.index[0:5])
    
    return rec_stocks

