import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data import postgres_handler as pg
from data import data_manipulator as dm
from data import conn_config as con
import pandas as pd

# 디비 연결
pgdb = pg.PostgresHandler(user=con.ID_IJ, password=con.PW_IJ)

def show_news(isin_code):
    result_dict = []
    data_list = pgdb.get_news_by_isin_code(isin_code)
    for i in range(len(data_list)):
        headline = data_list[i]['headline']
        sentiment = data_list[i]['sentiment']
        determinant =""
        if sentiment > 0:
            determinant = "호재"
        elif sentiment < 0:
            determinant = "악재"
        else:
            determinant = "중립"
        sentiment = round(abs(sentiment) * 100)
        stock_name = pgdb.get_item_name_by_isin_code(isin_code)
        result_dict.append({"headline":headline, "determinant":determinant, "sentiment":sentiment})
    # headline, tf, sentiment
    return stock_name, result_dict

def total_setiment(isin_code):
    data_list = pgdb.get_news_by_isin_code(isin_code)
    total = 0
    for data in data_list:
        total += data['sentiment']

    total /= len(data_list)

    return total
