import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data import postgres_handler as pg
from data import data_manipulator as dm
from data import conn_config as con
import pandas as pd
import math

# 디비 연결
pgdb = pg.PostgresHandler(user=con.ID_IJ, password=con.PW_IJ)

def show_news(isin_code):
    result_dict = []
    data_list = pgdb.get_news_by_isin_code(isin_code)
    stock_name = pgdb.get_item_name_by_isin_code(isin_code)
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
        sentiment = abs(math.floor(sentiment * 100))
        result_dict.append({"headline":headline, "determinant":determinant, "sentiment":sentiment})

    return stock_name, result_dict

def total_setiment(id):
    result_dict = []
    mem_data = pgdb.get_portfolio_by_member_id(member_id=id)
    for dic in mem_data:
        data_list = pgdb.get_news_by_isin_code(dic['isin_code'])
        total = 0
        for data in data_list:
            total += data['sentiment']

        if len(data_list) != 0:
            total /= len(data_list)

        determinant =""
        if total > 0:
            determinant = "호재"
        elif total < 0:
            determinant = "악재"
        else:
            determinant = "중립"
        total = abs(math.floor(total * 100))
        stock_name = pgdb.get_item_name_by_isin_code(dic['isin_code'])
        short_isin = pgdb.get_short_isin_code(dic['isin_code'])
        result_dict.append({"stock_name":stock_name, "short_isin":short_isin, "determinant":determinant, "total":total})

    return result_dict

def total_recommend(isin_list):
    result_dict = []
    for isin in isin_list:
        data_list = pgdb.get_news_by_isin_code(isin)
        total = 0
        for data in data_list:
            total += data['sentiment']
        
        if len(data_list) != 0:
            total /= len(data_list)

        determinant =""
        if total > 0:
            determinant = "호재"
        elif total < 0:
            determinant = "악재"
        else:
            determinant = "중립"
        total = abs(math.floor(total * 100))
        stock_name = pgdb.get_item_name_by_isin_code(isin)
        short_isin = pgdb.get_short_isin_code(isin)
        result_dict.append({"stock_name":stock_name, "short_isin":short_isin, "determinant":determinant, "total":total})

    return result_dict