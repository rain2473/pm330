import pandas as pd
import numpy    as np

from plotly.offline import plot
import plotly.express as px
from pypfopt.expected_returns    import ema_historical_return
from pypfopt.risk_models         import exp_cov
from pypfopt.efficient_frontier  import EfficientFrontier
from pypfopt.discrete_allocation import DiscreteAllocation, get_latest_prices
from pypfopt.efficient_frontier  import EfficientCVaR
from pypfopt                     import HRPOpt
from pypfopt.objective_functions import sharpe_ratio, portfolio_return, portfolio_variance
from sklearn.preprocessing       import minmax_scale

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data import postgres_handler as pg
from data import data_manipulator as dm
from data import conn_config as con

pgdb = pg.PostgresHandler(user=con.ID_SG, password=con.PW_SG)

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
    stocks_return = ema_historical_return(stocks_close, compounding=True, log_returns=True) # 최근 종가에 더 많은 시간 가중치를 부여해 계산한 수익률
    pf_cov = exp_cov(stocks_close, log_returns=True)                                        # 상관계수
    pf_var = portfolio_variance(weights, pf_cov)                                            # 포트폴리오의 변동률
    pf_sharpe = sharpe_ratio(weights, stocks_return, pf_cov, negative=False)                # 포트폴리오의 샤프지수
    pf_ef = EfficientFrontier(stocks_return, pf_cov)                                        # 포트폴리오의 효율적 프론티어
    pf_weights_mod = pf_ef.max_sharpe()                                                     # 효율적 프론티어 상 가장 높은 샤프지수를 가지는 포트폴리오의 주식 별 비중
    pf_cleaned_weights = pf_ef.clean_weights()                                              
    max_sharpe = pf_ef.portfolio_performance()[2]                                           # 샤프지수가 가장 높은 포트폴리오의 샤프지수
    pre_score = np.array([0, pf_sharpe.value, max_sharpe])                                  # 회원 포트폴리오의 점수를 계산하기 위한 정규화용 배열
    scaled_pre_score = minmax_scale(pre_score)                                              # 회원 포트폴리오의 점수를 0~ max sharpe 기반으로 정규화
    
    pf_return = portfolio_return(weights, stocks_return, negative=False)                    # 회원 포트폴리오의 수익률
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
    int  : 포트폴리오의 점수
    """
    stocks_return = ema_historical_return(stocks_close, compounding=True, log_returns=True) # 최근 종가에 더 많은 시간 가중치를 부여해 계산한 수익률
    pf_cov = exp_cov(stocks_close, log_returns=True)                                        # 상관계수
    stocks_return_pct = stocks_close.pct_change().dropna()                                  # 종목들의 퍼센트 수익률
    
    # 샤프지수 기반 
    ef = EfficientFrontier(stocks_return, pf_cov)                                           # 효율적 프론티어
    ef_weights = ef.max_sharpe()                                                            # 효율적 프론티어 상 가장 높은 샤프지수를 가지는 포트폴리오의 주식 별 비중
    ef_cleaned_weights = ef.clean_weights()
    ef_return = round(ef.portfolio_performance()[0] * 100, 2)                               # 샤프지수가 가장 높은 포트폴리오의 수익률
    ef_vol = round(ef.portfolio_performance()[1] * 100, 2)                                  # 샤프지수가 가장 높은 포트폴리오의 변동성
    ef_sharpe = round(ef.portfolio_performance()[2] * 100, 2)                               # 샤프지수가 가장 높은 포트폴리오의 샤프지수
    
    
    # 위험회피 기반
    # hrp = HRPOpt(stocks_return_pct)
    # hrp_weights = hrp.optimize()
    # hrp_return = round(hrp.portfolio_performance()[0] * 100, 2)
    # hrp_vol = round(hrp.portfolio_performance()[1] * 100, 2)
    # hrp_sharpe = round(hrp.portfolio_performance()[2] * 100, 2)
    
    
    # CVaR 기반 변동성 장에서는 꼬리위험이 지나치게 크게 나오기 때문에 부적절
#     cvar_cov = stock_close.cov()
#     ef_cvar = EfficientCVaR(stocks_return, cvar_cov)
#     cvar_weights = ef_cvar.min_cvar()
#     ef_cvar.portfolio_performance
    
    # 샤프지수 전략에 따라 매수할 주식 수와 남는 돈
    latest_prices = get_latest_prices(stocks_close)                                           # 계산에 사용할 종목들의 전일 종가
    ef_da = DiscreteAllocation(ef_weights, latest_prices, total_portfolio_value = seed_money) # 전일 종가 기준으로 현재 시드머니로 매수할 수 있는 종목들의 수를 포트폴리오 비중에 따라 배분
    ef_allocation, ef_leftover = ef_da.greedy_portfolio()                                     # {종목이름: 종가수 ,...}로 이루어진 딕셔너리와 시드머니에서 종목을 매수한 후 남은 금액 계산
    
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
    list      : 추천하는 다섯개의 종목명(str)
    DataFrame : 다섯개 종목에 대한 종가
    """
    stocks = list(allocation.keys())                                               # 최적 포트폴리오에 포함될 종목들의 이름
    weights = list(allocation.values())                                            # 최적 포트폴리오에 포함되어 매수한 종목들의 주식 수
    stocks_close['pf_total_price'] = 0                                             # 최적 포트폴리오의 가격 변동을 할당하기 위한 칼럼 0으로 초기화 및 추가
    for idx, stock_name in enumerate(stocks):                                      # 종목들의 종가에 비중을 곱하여 합한 것을 포트폴리오의 종가로 설정
        stocks_close['pf_total_price'] += stocks_close[stock_name] * weights[idx]
        
    pf_close = stocks_close['pf_total_price'].reset_index().dropna()               # 포트폴리오의 종가만 따로 할당
    stocks_close_all = stocks_close_all.reset_index().dropna()                     # 포트폴리오에 편입할 종목을 찾아내기 위해 모든 주식들의 종가데이터를 정의
    
    pf_cor = pd.concat([pf_close, stocks_close_all], axis =1)                      # 포트폴리오의 종가와 다른 모든 종목들의 종가를 병합
    recommend = pf_cor.corr()[['pf_total_price']].sort_values(by='pf_total_price') # 병합된 데이터에서 포트폴리오 종가에 대한 상관계수를 계산하고 상관계수별로 정렬
    rec_stocks = list(recommend.index[0:5])                                        # 상관계수가 가장 낮은 5개의 종목을 rec_stock 변수에 할당
    cor_close = pf_cor[rec_stocks]

    return rec_stocks, cor_close

def get_drift(stocks_close):
    """
    단일 종목의 종가로 이루어진 시계열 데이터를 입력받아 
    해당 종목의 주가 모멘텀을 반환한다.
    양수이면 상승 모멘텀, 음수이면 하락 모멘텀을 나타낸다.
    [Parameters]
    stocks_close  (DataFrame) : 단일 종목의 이름을 칼럼명으로, 날짜를 인덱스로 갖는 종목별 종가
    [Returns]
    dict : {'종목':'모멘텀'} 형식의 dict
    """
    log_returns = np.log(1+stocks_close.pct_change()) # 단일 종목의 로그수익률 계산
    U = log_returns.mean()                            # 로그 수익률의 평균
    var = log_returns.var()                           # 로그 수익률의 분산
    
    drift = U - 0.5*var                               # drift를 구하는 수식을 통한 모멘텀 계산
    
    momentum = dict()                                 # 문자열을 저장하기 위한 딕셔너리
    
    for idx, name in enumerate(dict(drift)):          # 계산된 drift가 양수면 상승, 음수면 하락을 각 종목에 대해 저장
        if drift[idx] >0:
            momentum[name] = '상승 모멘텀'
        else:
            momentum[name] = '하락 모멘텀'
    
    return momentum


def get_member_stock(m_id):
    """
    회원의 아이디를 입력받아 해당 회원의 포트폴리오 편입 종목을 isin으로 반환한다.
    [Parameters]
    m_id   (str) : 회원의 아이디
    [Returns]
    list : 회원이 보유한 종목 isin code로 이루어진 리스트
    """
    member = pgdb.get_portfolio_by_member_id(member_id = m_id)
    temp_list = []
    for i in range(len(member)):
        temp_list.append(member[i]['isin_code'])
    weight_list = []
    for i in range(len(member)):
        weight_list.append(member[i]['quantity'])

    return temp_list, weight_list

def create_member_info(member_isin_list, member_weight, start_date ='20211026', end_date ='20221025'):
    close_df = pd.DataFrame()
    for idx, code in enumerate(member_isin_list):
        test_close = pgdb.get_close_price(isin_code = code, start_date=start_date, end_date = end_date)
        df_test = pd.DataFrame(test_close)
        df_test_mod = df_test.rename(columns={'close_price':df_test['isin_code'][0]})
        index_date = pd.to_datetime(df_test_mod['base_date'].unique())
        df_test_mod = df_test_mod.set_index(index_date,drop=True)
        df_test_mod.drop(columns=['base_date','isin_code'], inplace=True)
        close_df = pd.concat([close_df,df_test_mod], axis=1)
    close_df = close_df.astype('float')

    
    latest_price = close_df.iloc[-1, :]
    pf_value = latest_price * member_weight
    seed_money = pf_value.sum()
    
    weights = []
    for idx, quantity in enumerate(member_weight):
        weights.append(round(quantity/sum(member_weight),2))
    
    return close_df, seed_money, weights

def create_rec_close(member_isin_list, start_date = '20211026', end_date='20221025'):
    close_df = pd.DataFrame()
    for idx, code in enumerate(member_isin_list):
        test_close = pgdb.get_close_price(isin_code = code, start_date=start_date, end_date = end_date)
        df_test = pd.DataFrame(test_close)
        df_test_mod = df_test.rename(columns={'close_price': code})
        index_date = pd.to_datetime(df_test_mod['base_date'].unique())
        df_test_mod = df_test_mod.set_index(index_date,drop=True)
        df_test_mod.drop(columns=['base_date','isin_code'], inplace=True)
        close_df = pd.concat([close_df,df_test_mod], axis=1)
    close_df = close_df.astype('float')
    
    return close_df

def return_momentum_list(m_id):
    # member = pgdb.get_portfolio_by_member_id(member_id = m_id)
    stock_isin, stock_weight = get_member_stock(m_id)
    stock_close, seed_money, weights = create_member_info(stock_isin, stock_weight)
    allo, left = get_best_pf(stock_close, seed_money)
    stock_allocation = []
    for key, value in allo.items():
        stock_allocation.append({'subject': pgdb.get_item_name_by_isin_code(isin_code= key), 'value': value})
        
    stocks_csv = os.path.join(os.path.dirname(__file__), 'all_stocks_close.csv')
    all_close = pd.read_csv(stocks_csv)
    rec = recommend_stocks(allo, stock_close, all_close)
    rec_stock_close = create_rec_close(rec[0])
    momentum = get_drift(rec_stock_close)
    recommend_list = rec[0]

    return recommend_list, momentum, stock_allocation


def return_score_rec(m_id):
    # member = pgdb.get_portfolio_by_member_id(member_id = m_id)
    stock_isin, stock_weight = get_member_stock(m_id)
    stock_close, seed_money, weights = create_member_info(stock_isin, stock_weight, '20211025', '20221023')
    m_sharpe, score = evaluate_my_pf(weights, stock_close)
    allo, left = get_best_pf(stock_close, seed_money)
    
    return score

def create_plot(m_id, start_date = '20211025', end_date='20221026'):
    stock_isin, stock_weight = get_member_stock(m_id)
    stock_close, seed_money, weights = create_member_info(stock_isin, stock_weight, start_date= start_date, end_date = end_date)
 
    m_weights_pct = []
    for i in weights:
        m_weights_pct.append(i/sum(weights))
    
    m_pf = stock_close.copy()
    m_pf['original_pf'] = 0
    for idx, columns in enumerate(stock_isin):
        m_pf['original_pf'] += stock_close[columns] * m_weights_pct[idx]
    m_pf = m_pf.drop(columns=stock_close.columns)
    m_pf['최적화_전'] = (m_pf - m_pf.mean())/m_pf.std()
 
 
    best_weight, left = get_best_pf(stock_close, seed_money)
    best_weight_list = list(best_weight.items())
    b_weight = []
    b_stocks = []
    for i in best_weight_list:
        b_weight.append(i[1])
        b_stocks.append(i[0])
    b_weight_pct = []
    for i in b_weight:
        b_weight_pct.append(i/sum(b_weight))
    b_df = stock_close.copy()[b_stocks]
    b_pf = stock_close.copy()[b_stocks]
    b_pf['optimized_pf'] = 0
    for idx, columns in enumerate(b_stocks):
        b_pf['optimized_pf'] += b_df[columns] * b_weight[idx]
    b_pf = b_pf.drop(columns=b_stocks)
    b_pf['최적화_후'] = (b_pf - b_pf.mean())/b_pf.std()

    b_pf = b_pf.reset_index(names='date')
    m_pf = m_pf.reset_index(names='date')
    pf_data = pd.merge(b_pf,m_pf, how="inner")
 
    graphs = px.line(pf_data,
                  x='date', y=['최적화_후','최적화_전'], custom_data=['optimized_pf','original_pf'],
                  color_discrete_sequence=['#ff0000','#0000ff'],
                 )
    graphs.update_traces(
        hovertemplate="<br>".join([
            "날짜: %{x}",
        ])
    )
    graphs.update_layout(
        paper_bgcolor = "#d9d9d9",
        plot_bgcolor = '#d9d9d9',
        height = 360,
        width = 650,
        margin={"r":15,"t":15,"l":15,"b":25},
    )

    # Getting HTML needed to render the plot.
    plot_div = plot( graphs, output_type='div')
    return plot_div
