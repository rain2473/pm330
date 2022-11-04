
# Author  : 이병헌
# Contact : lww7438@gmail.com
# Date    : 2022-11-04(금)



# Required Modules
import psycopg2    # Command to install: "pip install psycopg2-binary"

from . import schema
from . import conn_config      as config
from . import api_handler      as api
from . import data_manipulator as dm



# Class Declaration
class PostgresCore():

    # * * *    Low-Level Methods (SQL Handlers)    * * *
    def __init__(self, user:str, password:str, host=config.POSTGRES_HOST, port=config.POSTGRES_PORT, db_name=config.PORTGRES_DB_NAME):

        self._client = psycopg2.connect(
            host     = host,
            port     = port,
            dbname   = db_name,
            user     = user,
            password = password            
        )

        self.conn_user = user
        self.cursor = self._client.cursor()

    def __del__(self):

        self._client.close()
        self.cursor.close()

    def __execute(self, query:str, args={}):

        self.cursor.execute(query,args)
        row = self.cursor.fetchall()
        return row

    def __commit(self):

        self.cursor.commit()

    def insert_item(self, table:str=None, columns:list=None, data:dict=None):
        """
        단일 Row를 삽입한다.
        (SQL: INSERT INTO 구문을 수행한다.)

        [Parameters]
        table   (str)  : 데이터를 조회할 테이블 이름 (default: None)
        columns (list) : 해당 테이블의 속성
        data    (dict) : 삽입할 데이터 ({<colume>:<value>}) (default: None)

        [Returns]
        True  : 데이터 삽입이 성공적으로 수행된 경우
        False : 데이터 삽입 과정에서 오류가 발생한 경우
        """

        # Processing for invalid arguments
        if (table not in schema.LIST_TABLE_NAME) or (table is None):
            print(f"[ERROR] Invalid Table Name: {table} does not exist")
            return False

        if (data is None) or (len(data) == 0):
            print(f"[ERROR] Empty Data Insertion: data is empty")
            return False

        # Setting columns and values for query
        str_values = ""

        for column in columns:
            
            if data[column] in schema.NULL_TYPES:
                str_values += "None" + ", "
            elif schema.get_type_by_column_name(table, column) in schema.STR_TYPES:
                str_values += "'" + str(data[column]) + "', "
            else:
                str_values += str(data[column]) + ", "

        # Elimination last comma
        str_values = str_values[:-2]

        # Make columns a string
        str_columns = ', '.join(columns)

        sql = f""" INSERT INTO {table} ({str_columns}) VALUES ({str_values}) ;"""

        try:
            print("Executed SQL: ", sql)
            self.cursor.execute(sql)
            self._client.commit()
            return True

        except Exception as err_msg :
            print(f"[ERROR] insert_item Error: {err_msg}")
            return False

    def insert_items(self, table:str=None, columns:list=None, data:list=None):
        """
        다수의 Row를 삽입한다.
        (SQL: INSERT INTO 구문을 수행한다.)

        [Parameters]
        table   (str)  : 데이터를 조회할 테이블 이름 (default: None)
        columns (list) : 해당 테이블의 속성
        data    (list) : 삽입할 데이터 ([{<colume>:<value>}, ...]) (default: None)

        [Returns]
        True  : 데이터 삽입이 성공적으로 수행된 경우
        False : 데이터 삽입 과정에서 오류가 발생한 경우
        """

        # Processing for invalid arguments
        if (table not in schema.LIST_TABLE_NAME) or (table is None):
            print(f"[ERROR] Invalid Table Name: {table} does not exist")
            return False

        if (data is None) or (len(data) == 0):
            print(f"[ERROR] Empty Data Insertion: data is empty")
            return False

        # Setting columns and values for query
        value_list = []
        
        for row in data:
            values = "("
            for column in columns:

                if row[column] in schema.NULL_TYPES:
                    values += "null" + ", "
                elif schema.get_type_by_column_name(table, column) in schema.STR_TYPES:
                    values += "'" + str(row[column]) + "', "
                else:
                    values += str(row[column]) + ", "
                    
            values = values[:-2]
            values += ")"
            value_list.append(values)

        # Make columns a string
        str_columns = ', '.join(columns)

        sql = f""" INSERT INTO {table} ({str_columns}) VALUES """
        for str_value in value_list:
            sql += str_value + ', '

        sql = sql[:-2]
        sql += ';'

        try:
            print("Executed SQL: ", sql)
            self.cursor.execute(sql)
            self._client.commit()
            return True

        except Exception as err_msg :
            print(f"[ERROR] insert_items Error: {err_msg}")
            return False
    
    def find_item(self, table:str=None, columns='ALL', condition:str=None, order_by:str=None, asc:bool=True):
        """
        테이블에서 조건에 부합하는 데이터를 반환한다.
        (SQL: SELECT 구문을 수행한다.)

        [Parameters]
        table     (str)        : 데이터를 조회할 테이블 이름 (default: None)
        columns   (list | str) : 반환할 데이터의 속성 (default: ALL; 모든 속성값)
        condition (list)       : 조회 조건 (WHERE Clause in SQL) (default: None)
        order_by  (str)        : 정렬의 기준이 될 속성 (default: None)
        asc       (bool)       : 오름차순 정렬 여부 (default: True)

        [Returns]
        str   : 데이터 조회가 성공적으로 수행된 경우, 조회 결과
        False : 데이터 조회 과정에서 오류가 발생한 경우
        """

        # Processing for invalid arguments
        if (table not in schema.LIST_TABLE_NAME) or (table is None):
            print(f"[ERROR] Invalid Table Name: {table} does not exist")
            return False

        # Setting SQL for query

        # SELECT Cluase in SQL
        if columns == 'ALL':
            str_columns = "*"
        elif type(list()) == type(columns):
            str_columns = ", ".join(columns)
            str_columns = "(" + str_columns + ")"
        elif type(str()) == type(columns):
            str_columns = columns
        
        # WHERE Cluase in SQL
        if condition is None:
            sql = f""" SELECT {str_columns} FROM {table} """
        else:
            sql = f""" SELECT {str_columns} FROM {table} WHERE {condition} """

        # ORDER BY Cluase in SQL
        if order_by is not None:
            sql += f""" ORDER BY {order_by} """

            if asc:
                sql += f""" ASC """
            else:
                sql += f""" DESC """

        sql += """ ; """

        try:
            print("Executed SQL: ", sql)
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            return result

        except Exception as err_msg:
            print(f"[ERROR] find_item Error: {err_msg}")
            return False

    def update_item(self, table:str=None, column:str=None, value=None, condition:str=None):
        """
        테이블에서 조건에 부합하는 데이터를 수정한다.
        (SQL: UPDATE 구문을 수행한다.)

        [Parameters]
        table     (str) : 데이터를 수정할 테이블 이름 (default: None)
        column    (str) : 수정 대상 속성 (default: None)
        value           : 수정할 값 (default: None)
        condition (str) : 수정 조건 (WHERE Clause in SQL) (default: None)

        [Returns]
        True  : 데이터 수정이 성공적으로 수행된 경우
        False : 데이터 수정 과정에서 오류가 발생한 경우
        """

        # Processing for invalid arguments
        if (table not in schema.LIST_TABLE_NAME) or (table is None):
            print(f"[ERROR] Invalid Table Name: {table} does not exist")
            return False

        # Setting SQL for query
        if schema.get_type_by_column_name(table, column) in schema.STR_TYPES:
            value = "'" + value + "'"
        
        sql = f""" UPDATE {table} SET {column}={value} WHERE {condition} ; """

        try :
            print("Executed SQL: ", sql)
            self.cursor.execute(sql)
            self._client.commit()
            return True

        except Exception as err_msg:
            print(f"[ERROR] update_item Error: {err_msg}")
            return False

    def delete_item(self, table:str=None, condition:str=None):
        """
        테이블에서 조건에 부합하는 데이터를 수정한다.
        condition의 값을 'ALL'로 지정할 경우, table의 모든 데이터를 삭제한다.
        (SQL: DELETE 구문을 수행한다.)

        [Parameters]
        table     (str) : 데이터를 수정할 테이블 이름 (default: None)
        condition (str) : 삭제 조건 (WHERE Clause in SQL) (default: None)

        [Returns]
        True  : 데이터 삭제가 성공적으로 수행된 경우
        False : 데이터 삭제 과정에서 오류가 발생한 경우
        """

        # Processing for invalid arguments
        if (table not in schema.LIST_TABLE_NAME) or (table is None):
            print(f"[ERROR] Invalid Table Name: {table} does not exist")
            return False
        
        if condition is None:
            print(f"[ERROR] Invalid condition: {condition} does not allowed")
            return False

        if condition == 'ALL':
            sql = f" DELETE FROM {table} ;"
        else:
            sql = f" DELETE FROM {table} WHERE {condition} ;"

        try :
            self.cursor.execute(sql)
            self._client.commit()
            return True

        except Exception as err_msg:
            print(f"[ERROR] delete_item Error: {err_msg}")
            return False

    def build_info_stock(self):
        """
        데이터베이스의 info_stock 테이블에 KOSPI, KOSDAQ, KONEX에 상장된 전 종목에 대한 기본정보들을 저장한다.
        ※ 본 메서드는 DBA만 수행할 수 있다.

        [Parameters]
        -
        
        [Returns]
        True  : info_stock 테이블 빌드에 성공한 경우
        False : DBA 이외에 다른 사용자가 이 함수를 호출한 경우 혹은 에러가 발생한 경우
        """

        # DBA Only can run initial building function
        if(self.conn_user != config.ID_DBA):
            print("Only DBA can run the build function")
            return False

        try:           
            # Clear Table
            self.delete_item(table='info_stock', condition='ALL')
            
            # Listing All Stock
            krx_listed_info = api.get_krx_listed_info(serviceKey=config.API_KEY_OPEN_DATA_PORTAL)
            krx_listed_info = dm.filter_params(data_list=krx_listed_info, params=['isinCd', 'mrktCtg', 'itmsNm', 'crno', 'corpNm', 'shotnIsin'])
            basic_stock_info = []

            # Remove Duplicates in krx_listed_info
            for i in range(len(krx_listed_info)):
                if krx_listed_info[i] not in krx_listed_info[i+1:]:
                    basic_stock_info.append(krx_listed_info[i])

            for krx_stock in basic_stock_info:
                item_basi_info = api.get_item_basi_info(serviceKey=config.API_KEY_OPEN_DATA_PORTAL, crno=krx_stock['crno'])

                if item_basi_info is not None:
                    item_basi_info = dm.filter_params(data_list=item_basi_info, params=['isinCd', 'stckParPrc', 'issuStckCnt', 'lstgDt'])
                    
                    for key, value in item_basi_info[0].items():
                        krx_stock[key] = value       

                # Column Renaming
                krx_stock['isin_code'] = krx_stock.pop('isinCd', None)
                krx_stock['market_category'] = krx_stock.pop('mrktCtg', None)
                krx_stock['item_name'] = krx_stock.pop('itmsNm', None)
                krx_stock['corp_number'] = krx_stock.pop('crno', None)
                krx_stock['corp_name'] = krx_stock.pop('corpNm', None)
                krx_stock['short_isin_code'] = krx_stock.pop('shotnIsin', None)
                krx_stock['face_value'] = krx_stock.pop('stckParPrc', None)
                krx_stock['issue_cnt'] = krx_stock.pop('issuStckCnt', None)
                krx_stock['listing_date'] = krx_stock.pop('lstgDt', None)
                krx_stock['industry'] = None # 산업군(섹터)에 대한 데이터 소스를 찾지 못함

            # Query Execution
            self.insert_items(table='info_stock', columns=['isin_code', 'short_isin_code', 'market_category', 'item_name', 'corp_name', 'corp_number', 'listing_date', 'issue_cnt', 'industry', 'face_value'], data=basic_stock_info)
            return True

        except Exception as err_msg:
            print(f"[ERROR] build_info_stock: {err_msg}")
            return False

    def build_prices(self, market_category:str='ALL'):
        """
        데이터베이스의 price_info 테이블(price_kospi, price_kosdaq, price_konex)에
        KOSPI, KOSDAQ, KONEX에 상장된 전 종목에 대한 주가정보들을 저장한다.
        ※ 본 메서드는 DBA만 수행할 수 있다.
        
        [Parameters]
        market_category (str) : 주가정보를 가져올 종목들의 상장 시장 (default='ALL') ('KOSPI':코스피 | 'KOSDAQ':코스닥 | 'KONEX':코넥스)
        
        [Returns]
        True  : price_kospi, price_kosdaq, price_konex 테이블 빌드에 성공한 경우
        False : DBA 이외에 다른 사용자가 이 함수를 호출한 경우 혹은 에러가 발생한 경우
        """
        
        # DBA Only can run initial building function
        if(self.conn_user != config.ID_DBA):
            print("Only DBA can run the build function")
            return False

        # Processing for invalid arguments
        if market_category not in schema.MARKET_CATEGORIES and market_category != 'ALL':
            print(f"[ERROR] Invalid Market_Category: {market_category} does not allowed")
            return False

        try:
            # Clear Table
            if market_category == 'KOSPI' or market_category == 'ALL':
                self.delete_item(table='price_kospi')
            if market_category == 'KOSDAQ' or market_category == 'ALL':
                self.delete_item(table='price_kosdaq')
            if market_category == 'KONEX' or market_category == 'ALL':
                self.delete_item(table='price_konex')

            # Listing Stocks
            if market_category == 'KOSPI' or market_category == 'ALL':
                condition = f"market_category = CAST('{market_category}' AS {schema.TYPE_info_stock['market_category']})"
                str_kospi_list = self.find_item(table='info_stock', columns=['isin_code', 'market_category'], condition=condition)
            if market_category == 'KOSDAQ' or market_category == 'ALL':
                condition = f"market_category = CAST('{market_category}' AS {schema.TYPE_info_stock['market_category']})"
                str_kosdaq_list = self.find_item(table='info_stock', columns=['isin_code', 'short_isin_code'], condition=condition)
            if market_category == 'KONEX' or market_category == 'ALL':
                condition = f"market_category = CAST('{market_category}' AS {schema.TYPE_info_stock['market_category']})"
                str_konex_list = self.find_item(table='info_stock', columns=['isin_code', 'short_isin_code'], condition=condition)

            # Parsing
            if market_category == 'KOSPI' or market_category == 'ALL':
                list_kospi = list()
                for row in str_kospi_list:
                    data = dict()
                    data['isin_code'] = row[0][1:-1].split(',')[0]
                    data['short_isin_code'] = row[0][1:-1].split(',')[1]
                    list_kospi.append(data)
            
            if market_category == 'KOSDAQ' or market_category == 'ALL':
                list_kosdaq = list()
                for row in str_kosdaq_list:
                    data = dict()
                    data['isin_code'] = row[0][1:-1].split(',')[0]
                    data['short_isin_code'] = row[0][1:-1].split(',')[1]
                    list_kosdaq.append(data)

            if market_category == 'KONEX' or market_category == 'ALL':
                list_konex = list()
                for row in str_konex_list:
                    data = dict()
                    data['isin_code'] = row[0][1:-1].split(',')[0]
                    data['short_isin_code'] = row[0][1:-1].split(',')[1]
                    list_konex.append(data)

            # Collecting prices
            if market_category == 'KOSPI' or market_category == 'ALL':
                for kospi_stock in list_kospi:

                    list_ohlcv = api.get_market_ohlcv_by_date(short_isin_code=kospi_stock['short_isin_code'])

                    for ohlcv in list_ohlcv:
                        ohlcv['isin_code'] = kospi_stock['isin_code']
                        ohlcv.pop('short_isin_code')

                    self.insert_items(table='price_kospi', columns=['base_date', 'isin_code', 'market_price', 'close_price', 'high_price', 'low_price', 'fluctuation', 'fluctuation_rate', 'volume'], data=list_ohlcv)

            if market_category == 'KOSDAQ' or market_category == 'ALL':
                for kosdaq_stock in list_kosdaq:

                    list_ohlcv = api.get_market_ohlcv_by_date(short_isin_code=kosdaq_stock['short_isin_code'])

                    for ohlcv in list_ohlcv:
                        ohlcv['isin_code'] = kosdaq_stock['isin_code']
                        ohlcv.pop('short_isin_code')

                    self.insert_items(table='price_kosdaq', columns=['base_date', 'isin_code', 'market_price', 'close_price', 'high_price', 'low_price', 'fluctuation', 'fluctuation_rate', 'volume'], data=list_ohlcv)

            if market_category == 'KONEX' or market_category == 'ALL':
                for konex_stock in list_konex:

                    list_ohlcv = api.get_market_ohlcv_by_date(short_isin_code=konex_stock['short_isin_code'])

                    for ohlcv in list_ohlcv:
                        ohlcv['isin_code'] = konex_stock['isin_code']
                        ohlcv.pop('short_isin_code')

                    self.insert_items(table='price_konex', columns=['base_date', 'isin_code', 'market_price', 'close_price', 'high_price', 'low_price', 'fluctuation', 'fluctuation_rate', 'volume'], data=list_ohlcv)

            return True
        
        except Exception as err_msg:
            print(f"[ERROR] build_prices Error: {err_msg}")
            return False

    def build_financial_info(self):
        """
        데이터베이스의 financial_info 테이블에 KOSPI, KOSDAQ, KONEX에 상장된 전 종목에 대한 재무정보들을 저장한다.
        ※ 본 메서드는 DBA만 수행할 수 있다.
        
        [Parameters]
        -
        
        [Returns]
        -
        """

        # DBA Only can run initial building function
        if(self.conn_user != config.ID_DBA):
            print("Only DBA can run the build function")
            return False

        try:
            pass # This logic not implemented yet

        except Exception as err_msg:
            print(f"[ERROR] build_financial_info Error: {err_msg}")

    # def build_news_info(self, market_category:str='ALL'):
    #     """
    #     데이터베이스의 news_info 테이블에 KOSPI, KOSDAQ, KONEX에 상장된 전 종목에 대한 주가정보들을 저장한다.
    #     ※ 본 메서드는 DBA만 수행할 수 있다.
        
    #     [Parameters]
    #     market_category (str) : 주가정보를 가져올 종목들의 상장 시장 (default='ALL') ('KOSPI':코스피 | 'KOSDAQ':코스닥 | 'KONEX':코넥스)
        
    #     [Returns]
    #     False : 오류가 발생한 경우
    #     """

    #     # Only authorized developer can run initial building function
    #     if(self.conn_user not in [config.ID_DBA, config.ID_IJ]):
    #         print("Only authorized developer can run the build function")
    #         return False

    #     try:
    #         # Clear Table
    #         self.delete_item(table='news_info')

    #         # Listing Stocks
    #         if market_category == 'ALL':
    #             rows = self.find_item(table='basic_stock_info', columns=['isin_code', 'market_category'])
    #         elif market_category in MARKET_CATEGORIES:
    #             condition = f"market_category = CAST('{market_category}' AS {TYPE_basic_stock_info['market_category']})"
    #             rows = self.find_item(table='basic_stock_info', columns=['isin_code', 'short_isin_code'], condition=condition)
    #         else:
    #             print(f"[ERROR] Invalid parameter: market_category: {market_category}")
    #             return False

    #         # Parsing
    #         krx_listed_info = list()
    #         for row in rows:
    #             data = dict()
    #             data['isin_code'] = row[0][1:-1].split(',')[0]
    #             data['short_isin_code'] = row[0][1:-1].split(',')[1]
    #             krx_listed_info.append(data)

    #         for kr_stock in krx_listed_info:

    #             raw_news = news.get_data(isin_code=kr_stock['isin_code'], short_isin_code=kr_stock['short_isin_code'])

    #             if len(raw_news) == 0:
    #                 continue

    #             list_news = list()
    #             for article in raw_news:
    #                 data = dict()
    #                 data['isin_code'] = article[0]
    #                 data['write_code'] = article[1]
    #                 data['headline'] = article[2]
    #                 data['sentiment'] = float(article[3])
    #                 list_news.append(data)

    #             self.insert_items(table='news_info', columns=['isin_code', 'write_date', 'headline', 'sentiment'], data=list_news)
        
    #     except Exception as err_msg:
    #         print(f"[ERROR] build_news_info Error: {err_msg}")

    def build_world_index_info(self):
        """
        데이터베이스의 world_index_info 테이블에 세계 주요 주가 지수에 대한 정보들을 저장한다.
        ※ 본 메서드는 DBA만 수행할 수 있다.
        
        [Parameters]
        -
        
        [Returns]
        -
        """

        WORLD_INDEX_TICKERS = [

            # Primary Indices below:
            {'ticker':'^GSPC',     'nation':'US',          'index_name':'S&P 500',                      'latitude':39.1, 'longtitude':-118.3},
            {'ticker':'^DJI',      'nation':'US',          'index_name':'Dow Jones Industrial Average', 'latitude':38.7, 'longtitude':-94.2},
            {'ticker':'^IXIC',     'nation':'US',          'index_name':'NASDAQ Composite',             'latitude':42.5, 'longtitude':-74.1},
            {'ticker':'^VIX',      'nation':'US',          'index_name':'Vix',                          'latitude':44.1, 'longtitude':-84.2},
            {'ticker':'^STOXX50E', 'nation':'Europe',      'index_name':'ESTX 50 PR.EUR',               'latitude':41.9, 'longtitude':13.5},
            {'ticker':'\^FTSE',    'nation':'UK',          'index_name':'FTSE 100',                     'latitude':53.5, 'longtitude':-2.1},
            {'ticker':'^GDAXI',    'nation':'Germany',     'index_name':'DAX PERFORMANCE-INDEX',        'latitude':52.8, 'longtitude':10.45},
            {'ticker':'^FCHI',     'nation':'France',      'index_name':'CAC 40',                       'latitude':48.2, 'longtitude':4.3},
            {'ticker':'^N100',     'nation':'France',      'index_name':'Euronext 100 Index',           'latitude':46.4, 'longtitude':2.1},
            {'ticker':'000001.SS', 'nation':'China',       'index_name':'SSE Composite Index',          'latitude':29.1, 'longtitude':119.2},
            {'ticker':'399001.SZ', 'nation':'China',       'index_name':'Shenzhen Index',               'latitude':37.1, 'longtitude':116.5},
            {'ticker':'^N225',     'nation':'Japan',       'index_name':'Nikkei 225',                   'latitude':36.4, 'longtitude':138.24},
            {'ticker':'^KS11',     'nation':'Korea',       'index_name':'KOSPI Composite Index',        'latitude':37.5, 'longtitude':128.00},
            {'ticker':'^HSI',      'nation':'Taiwan',      'index_name':'HANG SENG INDEX',              'latitude':22.1, 'longtitude':120.1},
            {'ticker':'^TWII',     'nation':'Taiwan',      'index_name':'TSEC weighted index',          'latitude':24.4, 'longtitude':121.3},

            # Secondary Indices below:
            {'ticker':'^BUK100P',  'nation':'UK',          'index_name':'Cboe UK 100',                  'latitude':None, 'longtitude':None},
            {'ticker':'^NYA',      'nation':'US',          'index_name':'NYSE COMPOSITE (DJ)',          'latitude':None, 'longtitude':None},
            {'ticker':'^XAX',      'nation':'US',          'index_name':'NYSE AMEX COMPOSITE INDEX',    'latitude':None, 'longtitude':None},
            {'ticker':'^RUT',      'nation':'US',          'index_name':'Russell 2000',                 'latitude':None, 'longtitude':None},
            {'ticker':'^BFX',      'nation':'Belgium',     'index_name':'BEL 20',                       'latitude':None, 'longtitude':None},
            {'ticker':'IMOEX.ME',  'nation':'Russia',      'index_name':'MOEX Russia Index',            'latitude':None, 'longtitude':None},
            {'ticker':'\^STI',     'nation':'Singapore',   'index_name':'STI Index',                    'latitude':None, 'longtitude':None},
            {'ticker':'^AXJO',     'nation':'Australia',   'index_name':'S&P/ASX 200',                  'latitude':None, 'longtitude':None},
            {'ticker':'^AORD',     'nation':'Australia',   'index_name':'ALL ORDINARIES',               'latitude':None, 'longtitude':None},
            {'ticker':'^BSESN',    'nation':'India',       'index_name':'S&P BSE SENSEX',               'latitude':None, 'longtitude':None},
            {'ticker':'^JKSE',     'nation':'Indonesia',   'index_name':'Jakarta Composite Index',      'latitude':None, 'longtitude':None},
            {'ticker':'\^KLSE',    'nation':'Malaysia',    'index_name':'FTSE Bursa Malaysia KLCI',     'latitude':None, 'longtitude':None},
            {'ticker':'^NZ50',     'nation':'New Zealand', 'index_name':'S&P/NZX 50 INDEX GROSS',       'latitude':None, 'longtitude':None},
            {'ticker':'^GSPTSE',   'nation':'Canada',      'index_name':'S&P/TSX Composite index',      'latitude':None, 'longtitude':None},
            {'ticker':'^BVSP',     'nation':'Brazil',      'index_name':'IBOVESPA',                     'latitude':None, 'longtitude':None},
            {'ticker':'^MXX',      'nation':'Mexico',      'index_name':'IPC MEXICO',                   'latitude':None, 'longtitude':None},
            {'ticker':'^IPSA',     'nation':'Chile',       'index_name':'S&P/CLX IPSA',                 'latitude':None, 'longtitude':None},
            {'ticker':'^MERV',     'nation':'Argentina',   'index_name':'MERVAL',                       'latitude':None, 'longtitude':None},
            {'ticker':'^TA125.TA', 'nation':'Israel',      'index_name':'TA-125',                       'latitude':None, 'longtitude':None},
            {'ticker':'^CASE30',   'nation':'Egypt',       'index_name':'EGX 30 Price Return Index',    'latitude':None, 'longtitude':None},
            {'ticker':'^JN0U.JO',  'nation':'Republic of South Africa', 'index_name':'Top 40 USD Net TRI Index', 'latitude':None, 'longtitude':None},
        ]

        # DBA Only can run initial building function
        if(self.conn_user != config.ID_DBA):
            print("Only DBA can run the build function")
            return False

        try:
            # Clear Table
            self.delete_item(table='world_index_info')
            self.insert_items(table='world_index_info', columns=['ticker', 'nation', 'index_name'], data=WORLD_INDEX_TICKERS)

        except Exception as err_msg:
            print(f"[ERROR] build_world_index_info Error: {err_msg}")

    def build_world_index_price(self):
        """
        데이터베이스의 world_index_price 테이블에 세계 주요 주가 지수에 대한 가격 정보들을 저장한다.
        ※ 본 메서드는 DBA만 수행할 수 있다.
        
        [Parameters]
        -
        
        [Returns]
        -
        """

        # DBA Only can run initial building function
        if(self.conn_user != config.ID_DBA):
            print("Only DBA can run the build function")
            return False

        try:
            # Clear Table
            self.delete_item(table='world_index_price')

            # Get information of world indices
            raw_data = self.get_all_data(table='world_index_info')

            # Parsing
            world_indices = list()
            for row in raw_data:
                data = dict()
                data['ticker'] = row[0]
                data['naion'] = row[1]
                data['index_name'] = row[2]
                world_indices.append(data)

            # Collect data of prices of indices
            for world_index in world_indices:
                prices = api.get_world_index(ticker=world_index['ticker'], startDt='20220101', endDt=dm.YESTERDAY)

                self.insert_items(table='world_index_price', columns=['ticker', 'base_date', 'market_price', 'close_price', 'adj_close_price', 'high_price', 'low_price', 'fluctuation', 'fluctuation_rate', 'volume'], data=prices)

        except Exception as err_msg:
            print(f"[ERROR] build_world_index_info Error: {err_msg}")