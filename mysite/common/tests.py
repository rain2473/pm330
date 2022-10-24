
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data import postgres_handler as pg
from data import conn_config as con
from data import data_manipulator as dm
import pandas as pd

pgdb = pg.PostgresHandler(user=con.ID_YK, password=con.PW_YK)
# username = 'test_user_28'
# raw_password = "28288dmdlrn"
# email = "28uigoo@naver.com"
# pgdb.set_new_member(member_id=username, member_pw=raw_password, member_email=email)
print(pgdb.get_all_data(table='member_info'))

# 데이터 조회
# print( pd.DataFrame(pgdb.get_all_data(table="basic_stock_info")))
# print( pd.DataFrame(pgdb.get_all_data(table="news_info")))

# # 종가 조회
# samsung = pgdb.get_close_price(
#     isin_code=pgdb.get_isin_code('000020'),
#     start_date= '20221001',
#     end_date= dm.YESTERDAY
# )
# print(samsung)