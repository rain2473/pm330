#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from numpy.random import random, uniform, dirichlet, choice
from numpy.linalg import inv
from scipy.optimize import minimize
import pandas_datareader.data as web
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import seaborn as sns
get_ipython().system('pip install yfinance')
import yfinance as yf
import plotly.graph_objects as go


# In[2]:


ticker = ['SPY','AGG','TIP']
df = yf.download(ticker, start = '2018-01-01', end='2022-09-01')[['Close']]
df = df.dropna()
df.columns = ['S&P', '종합채권','채권ETF']
df = np.log(df).pct_change()
df = df.dropna()


# In[3]:


stocks = df.columns
n_obs, n_assets = df.shape
pf_num = 100000 # 시뮬레이션 돌릴 갯수

x0 = uniform(0, 1, n_assets) # 0~1사이 숫자 종목 수(n_assets)만큼 리턴
x0 /= np.sum(np.abs(x0)) # 초기값으로 랜덤 비중 부여
# x0 = np.array([비중들]) 으로 수동으로 줄 수 있음

# 연단위 환산 'A'
periods_per_year = round(df.resample('A').size().mean())
# 전체 기간을 1년 단위로 환산

# 무위험이자율: 미국 10년 국고채
treasury_10yr = yf.download('^TNX', start= '2018-01-01', end= '2022-09-01')[['Close']]
rf_rate = treasury_10yr.mean()

# 평균 수익률 및 cov-var matrix
mean_returns = df.mean()
cov_matrix = df.cov()

# precision matrix
pre_matrix = pd.DataFrame(inv(cov_matrix), index=stocks, columns=stocks)


# In[4]:


def pf_sim(mean_ret, cov, rf_rate=rf_rate, short=True):
    alpha = np.full(shape=n_assets, fill_value= .05)
    weights = dirichlet(alpha = alpha, size = pf_num) # 디리클레 분포 = 합이 1이 되는 연속 확률분포
    
    if short:
        weights *= choice([-1,1], size = weights.shape) # 숏을 고려할 경우의 무작위 가중치 설정
    
    returns = weights @ mean_ret.values + 1 # 비중 벡터와 평균 수익률 매트릭스의 벡터 곱
    returns = returns ** periods_per_year - 1 # 전체 평균 하루짜리로
    std = (weights @ df.T).std(1) # 행 하나씩(weight 하나씩) std 계산
    std *= np.sqrt(periods_per_year) # 연율화
    sharpe = (returns - rf_rate.to_numpy()) / std
    
    return pd.DataFrame({'연 표준편차': std,
                         '연 수익률' : returns,
                         '샤프지수' : sharpe}), weights


# In[5]:


sim_perf, sim_weight = pf_sim(mean_returns, cov_matrix, short=False)
sim_max_sharpe = sim_perf.iloc[:,2].idxmax()
sim_perf.iloc[sim_max_sharpe]


# In[6]:


sim_perf


# In[7]:


sim_perf.plot(x='연 표준편차', y='연 수익률', kind='scatter', figsize=(10, 6));
plt.xlabel('Expected Volatility')
plt.ylabel('Expected Return')


# In[8]:


def pf_std(wt, rt=None, cov=None):
    return np.sqrt(wt @ cov @ wt * periods_per_year)

def pf_returns(wt, rt=None, cov=None):
    return (wt @ rt + 1) ** periods_per_year -1

def pf_performance(wt, rt, cov):
    r = pf_returns(wt, rt=rt)
    sd = pf_std(wt, cov=cov)
    return r, sd

def neg_sharpe_ratio(weights, mean_ret, cov):
    r, sd = pf_performance(weights, mean_ret, cov)
    return -(r - rf_rate) / sd


# In[9]:


weight_constraints = {'type':'eq',
                     'fun':lambda x: np.sum(np.abs(x)) -1}

def max_sharpe_ratio(mean_ret, cov, short=False):
    return minimize(fun = neg_sharpe_ratio,
                    x0= x0,
                    args=(mean_ret, cov),
                    method='SLSQP',
                    bounds=((-1 if short else 0, 1), ) * n_assets,
                    constraints = weight_constraints,
                    options= {'tol':1e-10, 'maxiter':1e4})


# In[10]:


max_sharpe_pf = max_sharpe_ratio(mean_returns, cov_matrix, short=False)
max_sharpe_perf = pf_performance(max_sharpe_pf.x, mean_returns, cov_matrix)


# In[11]:


max_sharpe_perf


# In[12]:


r, sd = max_sharpe_perf
pd.Series({'수익률':r, '표준편차':sd, '샤프지수':(r-rf_rate/sd)})


# In[13]:


def min_vol(mean_ret, cov, short=False):
    bounds = ((-1 if short else 0, 1), ) * n_assets
    
    return minimize(fun=pf_std,
                    x0=x0,
                    args = (mean_ret, cov),
                    method='SLSQP',
                    bounds= bounds,
                    constraints = weight_constraints,
                    options= {'tol':1e-10, 'maxiter':1e4})


# In[14]:


min_vol_pf = min_vol(mean_returns, cov_matrix, short=False)
min_vol_perf = pf_performance(min_vol_pf.x, mean_returns, cov_matrix)


# In[15]:


def min_vol_target(mean_ret, cov, target, short=False):
    
    def ret_(wt):
        return pf_returns(wt, mean_ret)
    
    constraints = [{'type':'eq',
                    'fun':lambda x: ret_(x) - target},
                  weight_constraints]
    bounds = ((-1 if short else 0, 1), ) * n_assets
    return minimize(fun=pf_std,
                    x0=x0,
                    args = (mean_ret, cov),
                    method='SLSQP',
                    bounds= bounds,
                    constraints = constraints,
                    options= {'tol':1e-10, 'maxiter':1e4})


# In[16]:


def efficient_frontier(mean_ret, cov, ret_range, short=False):
    return [min_vol_target(mean_ret, cov, ret) for ret in ret_range]


# In[17]:


ret_range = np.linspace(sim_perf.iloc[:, 1].min(), sim_perf.iloc[:, 1].max(), 50)
eff_pf = efficient_frontier(mean_returns, cov_matrix, ret_range, short=True)
eff_pf = pd.Series(dict(zip([p['fun'] for p in eff_pf], ret_range)))


# In[18]:


eff_pf_df = pd.DataFrame(eff_pf, columns=['연 수익률']).reset_index(drop=False, names='연 표준편차')


# In[19]:


eff_pf_df.plot(x='연 표준편차', y='연 수익률', kind='scatter', figsize=(10, 6))
plt.xlabel('Expected Volatility')
plt.ylabel('Expected Return')
plt.title('Optimized Portfolios')


# In[ ]:




