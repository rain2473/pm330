from . import PF_optimizer as pf
import pandas as pd
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data import postgres_handler as pg
from data import data_manipulator as dm
from data import conn_config as con


pgdb = pg.PostgresHandler(user=con.ID_SG, password=con.PW_SG)

def return_score_rec(m_id):
    # member = pgdb.get_portfolio_by_member_id(member_id = m_id)
    stock_isin, stock_weight = pf.get_member_stock(m_id)
    stock_close, seed_money, weights = pf.create_member_info(stock_isin, stock_weight, '20211025', '20221023')
    m_sharpe, score = pf.evaluate_my_pf(weights, stock_close)
    allo, left = pf.get_best_pf(stock_close, seed_money)
    stocks_csv = os.path.join(os.path.dirname(__file__), 'all_stocks_close.csv')
    all_close = pd.read_csv(stocks_csv)
    rec = pf.recommend_stocks(allo, stock_close, all_close)
    # rec_stock_close = pf.create_rec_close(rec[0],'20211025','20221023')
    # momentum = pf.get_drift(rec_stock_close)
    # recommend_list = rec[0]

    return score
