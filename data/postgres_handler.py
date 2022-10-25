# Author  : 이병헌
# Contact : lww7438@gmail.com
# Date    : 2022-10-25(화)



# Required Modules
import psycopg2    # Command to install: "pip install psycopg2-binary"

# from . import conn_config      as config
# from . import api_handler      as api
# from . import data_manipulator as dm
import conn_config      as config
import api_handler      as api
import data_manipulator as dm



# Configurations (Database Schema)
TYPE_basic_stock_info = {
    'isin_code'       : 'varchar',
    'short_isin_code' : 'varchar',
    'market_category' : 'varchar',
    'item_name'       : 'varchar',
    'corp_name'       : 'varchar',
    'corp_number'     : 'varchar',
    'listing_date'    : 'date',
    'issue_cnt'       : 'bigint',
    'industry'        : 'varchar',
    'face_value'      : 'integer',
}

TYPE_financial_info = {
    'isin_code' : 'varchar',
    'bps'       : 'integer',
    'per'       : 'double precision',
    'pbr'       : 'double precision',
    'eps'       : 'integer',
    'div'       : 'double precision',
    'dps'       : 'integer',
}

TYPE_member_info = {
    'member_id'    : 'varchar',
    'member_pw'    : 'varchar',
    'member_email' : 'varchar',
}

TYPE_news_info = {
    'isin_code'  : 'varchar',
    'write_date' : 'date',
    'headline'   : 'varchar',
    'sentiment'  : 'double precision',
    'news_id'    : 'bigint', # Serial8 Type
}

TYPE_price_info = {
    'base_date'        : 'date',
    'isin_code'        : 'varchar',
    'market_price'     : 'integer',
    'close_price'      : 'integer',
    'high_price'       : 'integer',
    'low_price'        : 'integer',
    'fluctuation'      : 'integer',
    'fluctuation_rate' : 'double precision',
    'volume'           : 'bigint',
}

TYPE_portfolio_transaction = {
    'member_id'        : 'varchar',
    'isin_code'        : 'varchar',
    'quantity'         : 'integer',
    'break_even_price' : 'double precision',
}

TYPE_world_index_info = {
    'ticker' : 'varchar',
    'nation' : 'varchar',
    'index_name' : 'varchar',
}

TYPE_world_index_price = {
    'ticker'           : 'varchar',
    'base_date'        : 'date',
    'market_price'     : 'double precision',
    'close_price'      : 'double precision',
    'adj_close_price'  : 'double precision',
    'high_price'       : 'double precision',
    'low_price'        : 'double precision',
    'fluctuation'      : 'double precision',
    'fluctuation_rate' : 'double precision',
    'volume'           : 'bigint',
}

LIST_TABLE_NAME = [
    'basic_stock_info',
    'member_info',
    'price_info',
    'news_info',
    'financial_info',
    'portfolio_transaction',
    'world_index_info',
    'world_index_price',
]

SCHEMA = {
    'basic_stock_info'      : TYPE_basic_stock_info,
    'member_info'           : TYPE_member_info,
    'price_info'            : TYPE_price_info,
    'news_info'             : TYPE_news_info,
    'financial_info'        : TYPE_financial_info,
    'portfolio_transaction' : TYPE_portfolio_transaction,
    'world_index_info'      : TYPE_world_index_info,
    'world_index_price'     : TYPE_world_index_price,
}

STR_TYPES  = ['varchar', 'text', 'char', 'date']
NULL_TYPES = [None, 'None', 'none', 'NULL', 'null', 'nullptr', '']



# Functions
def get_type_by_column_name(table:str=None, column:str=None):
    
    try:
        return SCHEMA[table][column]

    except Exception as err_msg:
        print(f"[ERROR] Schema Error: {err_msg}")


# Class Declaration
class PostgresHandler():

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

    def insert_item(self, schema:str='postgres', table:str=None, columns:list=None, data:dict=None):

        if (table not in LIST_TABLE_NAME) or (table is None):
            raise f"[ERROR] Invalid Table Name: {table} does not exist"

        if data is None:
            raise f"[ERROR] Empty Data Insertion: data is empty"

        values = ""

        for column in columns:
            
            if data[column] in NULL_TYPES:
                values += "None" + ", "
            elif get_type_by_column_name(table, column) in STR_TYPES:
                values += "'" + str(data[column]) + "', "
            else:
                values += str(data[column]) + ", "

        values = values[:-2]

        sql = f""" INSERT INTO {table} ({', '.join(columns)}) VALUES ({values}) ;"""

        print("SQL: ", sql)

        try:
            self.cursor.execute(sql)
            self._client.commit()

        except Exception as err_msg :
            print(f"[ERROR] Insert Error: {err_msg}") 

    def insert_items(self, table:str=None, columns:list=None, data:list=None):

        if (table not in LIST_TABLE_NAME) or (table is None):
            raise f"[ERROR] Invalid Table Name: {table} does not exist"

        if data is None:
            raise f"[ERROR] Empty Data Insertion: data is empty"

        value_list = []
        
        for row in data:
            values = "("
            for column in columns:

                if row[column] in NULL_TYPES:
                    values += "null" + ", "
                elif get_type_by_column_name(table, column) in STR_TYPES:
                    values += "'" + str(row[column]) + "', "
                else:
                    values += str(row[column]) + ", "
                    
            values = values[:-2]
            values += ")"
            value_list.append(values)

        sql = f""" INSERT INTO {table} ({', '.join(columns)}) VALUES """
        for value in value_list:
            sql += value + ', '

        sql = sql[:-2]
        sql += ';'

        print("SQL: ", sql)

        try:
            self.cursor.execute(sql)
            self._client.commit()

        except Exception as err_msg :
            print(f"[ERROR] Insert Error: {err_msg}") 
    
    def find_item(self, table:str=None, columns='ALL', condition:str=None, order_by:str=None, asc:bool=True):

        if (table not in LIST_TABLE_NAME) or (table is None):
            raise f"[ERROR] Invalid Table Name: {table} does not exist"

        if columns == 'ALL':
            columns = "*"
        elif type(list()) == type(columns):
            columns = ", ".join(columns)
            columns = "(" + columns + ")"
        elif type(str()) == type(columns):
            pass
            
        if condition is None:
            sql = f""" SELECT {columns} FROM {table} """
        else:
            sql = f""" SELECT {columns} FROM {table} WHERE {condition} """

        if order_by is not None:
            sql += f""" ORDER BY {order_by} """

            if asc:
                sql += f"""ASC"""
            else:
                sql += f"""DESC"""

        sql += ';'
        print("SQL: ", sql)

        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()

        except Exception as err_msg:
            result = (f"[ERROR] Select Error: {err_msg}")
        
        return result

    def update_item(self, table:str=None, column:str=None, value=None, condition:str=None):

        if (table not in LIST_TABLE_NAME) or (table is None):
            raise f"[ERROR] Invalid Table Name: {table} does not exist"

        if get_type_by_column_name(table, column) in STR_TYPES:
            value = "'" + value + "'"
        
        sql = f" UPDATE {table} SET {column}={value} WHERE {condition} ;"

        try :
            self.cursor.execute(sql)
            self._client.commit()

        except Exception as err_msg:
            print(f"[ERROR] Update Error: {err_msg}")

    def delete_item(self, table:str=None, condition:str=None):
        
        if (table not in LIST_TABLE_NAME) or (table is None):
            raise f"[ERROR] Invalid Table Name: {table} does not exist"

        if condition is None:
            sql = f" DELETE FROM {table} ;"
        else:
            sql = f" DELETE FROM {table} WHERE {condition} ;"

        try :
            self.cursor.execute(sql)
            self._client.commit()

        except Exception as err_msg:
            print(f"[ERROR] Delete Error: {err_msg}")

    # * * *    High-Level Methods    * * *
    def build_basic_stock_info(self):
        """
        데이터베이스의 basic_stock_info 테이블에 KOSPI, KOSDAQ, KONEX에 상장된 전 종목에 대한 기본정보들을 저장한다.
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
            self.delete_item(table='basic_stock_info')
            
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
            self.insert_items(table='basic_stock_info', columns=['isin_code', 'short_isin_code', 'market_category', 'item_name', 'corp_name', 'corp_number', 'listing_date', 'issue_cnt', 'industry', 'face_value'], data=basic_stock_info)

        except Exception as err_msg:
            print(f"[ERROR] build_basic_stock_info: {err_msg}")

    def build_price_info(self):
        """
        데이터베이스의 price_info 테이블에 KOSPI, KOSDAQ, KONEX에 상장된 전 종목에 대한 주가정보들을 저장한다.
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
            self.delete_item(table='price_info')

            # Listing All Stock
            krx_listed_info = api.get_krx_listed_info(serviceKey=config.API_KEY_OPEN_DATA_PORTAL)

            for kr_stock in krx_listed_info:

                list_ohlcv = api.get_market_ohlcv_by_date(short_isin_code=kr_stock['shotnIsin'])

                for ohlcv in list_ohlcv:
                    ohlcv['isin_code'] = kr_stock['isinCd']
                    ohlcv.pop('short_isin_code')

                self.insert_items(table='price_info', columns=['base_date', 'isin_code', 'market_price', 'close_price', 'high_price', 'low_price', 'fluctuation', 'fluctuation_rate', 'volume'], data=list_ohlcv)
        
        except Exception as err_msg:
            print(f"[ERROR] build_price_info Error: {err_msg}")

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
                        {'ticker':'^GSPC',     'nation':'US',          'index_name':'S&P 500'},
                        {'ticker':'^DJI',      'nation':'US',          'index_name':'Dow Jones Industrial Average'},
                        {'ticker':'^IXIC',     'nation':'US',          'index_name':'NASDAQ Composite'},
                        {'ticker':'^NYA',      'nation':'US',          'index_name':'NYSE COMPOSITE (DJ)'},
                        {'ticker':'^XAX',      'nation':'US',          'index_name':'NYSE AMEX COMPOSITE INDEX'},
                        {'ticker':'^BUK100P',  'nation':'UK',          'index_name':'Cboe UK 100'},
                        {'ticker':'^RUT',      'nation':'US',          'index_name':'Russell 2000'},
                        {'ticker':'^VIX',      'nation':'US',          'index_name':'Vix'},
                        {'ticker':'\^FTSE',    'nation':'UK',          'index_name':'FTSE 100'},
                        {'ticker':'^GDAXI',    'nation':'Germany',     'index_name':'DAX PERFORMANCE-INDEX'},
                        {'ticker':'^FCHI',     'nation':'France',      'index_name':'CAC 40'},
                        {'ticker':'^STOXX50E', 'nation':'Europe',      'index_name':'ESTX 50 PR.EUR'},
                        {'ticker':'^N100',     'nation':'France',      'index_name':'Euronext 100 Index'},
                        {'ticker':'^BFX',      'nation':'Belgium',     'index_name':'BEL 20'},
                        {'ticker':'IMOEX.ME',  'nation':'Russia',      'index_name':'MOEX Russia Index'},
                        {'ticker':'^N225',     'nation':'Japan',       'index_name':'Nikkei 225'},
                        {'ticker':'^HSI',      'nation':'Taiwan',      'index_name':'HANG SENG INDEX'},
                        {'ticker':'000001.SS', 'nation':'China',       'index_name':'SSE Composite Index'},
                        {'ticker':'399001.SZ', 'nation':'China',       'index_name':'Shenzhen Index'},
                        {'ticker':'\^STI',     'nation':'Singapore',   'index_name':'STI Index'},
                        {'ticker':'^AXJO',     'nation':'Australia',   'index_name':'S&P/ASX 200'},
                        {'ticker':'^AORD',     'nation':'Australia',   'index_name':'ALL ORDINARIES'},
                        {'ticker':'^BSESN',    'nation':'India',       'index_name':'S&P BSE SENSEX'},
                        {'ticker':'^JKSE',     'nation':'Indonesia',   'index_name':'Jakarta Composite Index'},
                        {'ticker':'\^KLSE',    'nation':'Malaysia',    'index_name':'FTSE Bursa Malaysia KLCI'},
                        {'ticker':'^NZ50',     'nation':'New Zealand', 'index_name':'S&P/NZX 50 INDEX GROSS'},
                        {'ticker':'^KS11',     'nation':'Korea',       'index_name':'KOSPI Composite Index'},
                        {'ticker':'^TWII',     'nation':'Taiwan',      'index_name':'TSEC weighted index'},
                        {'ticker':'^GSPTSE',   'nation':'Canada',      'index_name':'S&P/TSX Composite index'},
                        {'ticker':'^BVSP',     'nation':'Brazil',      'index_name':'IBOVESPA'},
                        {'ticker':'^MXX',      'nation':'Mexico',      'index_name':'IPC MEXICO'},
                        {'ticker':'^IPSA',     'nation':'Chile',       'index_name':'S&P/CLX IPSA'},
                        {'ticker':'^MERV',     'nation':'Argentina',   'index_name':'MERVAL'},
                        {'ticker':'^TA125.TA', 'nation':'Israel',      'index_name':'TA-125'},
                        {'ticker':'^CASE30',   'nation':'Egypt',       'index_name':'EGX 30 Price Return Index'},
                        {'ticker':'^JN0U.JO',  'nation':'Republic of South Africa', 'index_name':'Top 40 USD Net TRI Index'},
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

        # try:
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

        # except Exception as err_msg:
        #     print(f"[ERROR] build_world_index_info Error: {err_msg}")

    def get_all_data(self, table:str=None):
        """
        테이블의 모든 데이터를 조회한다.

        [Parameters]
        table  (str) : 데이터를 조회할 테이블 이름 (default: None)

        [Returns]
        list : 조회 결과 (list of dict)
        """
        
        try:
            # Query
            return self.find_item(table=table)
        
        except Exception as err_msg:
            print(f"[ERROR] get_all_data Error: {err_msg}")

    def get_isin_code(self, short_isin_code:str):
        """
        ISIN Code (축약형)에 해당하는 ISIN Code를 반환한다.

        [Parameters]
        short_isin_code (str) : 국제 증권 식별 번호 (축약형, 6자리)

        [Returns]
        str  : 국제 증권 식별 번호 (12자리)
        """

        try:
            # Query
            result = self.find_item(table='basic_stock_info', columns='isin_code', condition=f"short_isin_code = CAST('{short_isin_code}' AS varchar)")

            return result[0][0]

        except Exception as err_msg:
            print(f"[ERROR] get_isin_code Error: {err_msg}")

    def get_isin_code_by_item_name(self, item_name:str):
        """
        종목명에 해당하는 ISIN Code와 종목명을 담은 리스트를 반환한다.

        [Parameters]
        item_name (str) : 종목명

        [Returns]
        list : ISIN Code와 종목명을 담은 리스트 (list of dict)
            isin_code : 국제 증권 식별 번호 (12자리)
            item_name : 종목명
        """

        try:
            # Query
            result = self.find_item(table='basic_stock_info', columns=['isin_code', 'item_name'], condition=f"item_name LIKE CAST('%{item_name}%' AS {TYPE_basic_stock_info['item_name']})")

            # Parsing
            rows = list()
            for row in result:
                raw_string = row[0][1:-1]
                data = dict()
                data['isin_code'] = raw_string.split(sep=',')[0]
                data['item_name'] = raw_string.split(sep=',')[1]
                rows.append(data)

            return rows

        except Exception as err_msg:
            print(f"[ERROR] get_isin_code_by_item_name Error: {err_msg}")

    def get_short_isin_code(self, isin_code:str):
        """
        ISIN Code에 해당하는 ISIN Code (축약형)를 반환한다.

        [Parameters]
        isin_code (str) : 국제 증권 식별 번호 (12자리)

        [Returns]
        str  : 국제 증권 식별 번호 (축약형, 6자리)
        """

        try:
            # Query
            result = self.find_item(table='basic_stock_info', columns='short_isin_code', condition=f"isin_code = CAST('{isin_code}' AS varchar)")

            return result[0][0]

        except Exception as err_msg:
            print(f"[ERROR] get_short_isin_code Error: {err_msg}")

    def get_item_name_by_isin_code(self, isin_code:str):
        """
        ISIN Code에 해당하는 종목명을 반환한다.

        [Parameters]
        isin_code (str) : 국제 증권 식별 번호 (12자리)

        [Returns]
        str  : 종목명
        """

        try:
            # Query
            result = self.find_item(table='basic_stock_info', columns='item_name', condition=f"isin_code = CAST('{isin_code}' AS {TYPE_basic_stock_info['isin_code']})")

            return result[0][0]

        except Exception as err_msg:
            print(f"[ERROR] get_item_name_by_isin_code Error: {err_msg}")

    def get_close_price(self, isin_code:str, start_date:str='20000101', end_date:str=dm.YESTERDAY):
        """
        ISIN Code에 해당하는 종목의 종가를 조회한다.

        [Parameters]
        isin_code  (str) : 국제 증권 식별 번호 (12자리)
        start_date (str) : 조회를 시작할 날짜 (Format: YYYYMMDD) (Default: '20000101')
        end_date   (str) : 조회를 종료할 날짜 (Format: YYYYMMDD) (Default: 전일 (KST 기준))
        
        [Returns]
        list  : 기준일자:종가 Pair들이 저장된 리스트 (list of dict) ex) {'base_date': <기준일자>, 'isin_code':<국제증권식별번호>, 'close_price': <종가>}
        """

        try:
            # Query
            result = self.find_item(
                table='price_info',
                columns=['base_date', 'isin_code', 'close_price'],
                condition=f"isin_code = CAST('{isin_code}' AS varchar) AND CAST('{start_date}' AS date) <= base_date AND base_date <= CAST('{end_date}' AS date)",
                order_by='base_date',
                asc=True
            )

            # Parsing
            rows = list()
            for row in result:
                raw_string = row[0][1:-1]
                data = dict()
                data['base_date'] = raw_string.split(sep=',')[0]
                data['isin_code'] = raw_string.split(sep=',')[1]
                data['close_price'] = int(raw_string.split(sep=',')[2])
                rows.append(data)

            return rows

        except Exception as err_msg:
            print(f"[ERROR] get_close_price Error: {err_msg}")

    def get_close_price_for_days(self, days:int=365):
        """
        금일 기준 days일 이전까지의 전 종목의 종가를 반환한다.
        기준일자를 기준으로 오름차순으로 정렬하여 반환한다.

        [Parameters]
        days (int) : 종가를 조회할 기간 (일) (Default: 365일)
        
        [Returns]
        list : 기준일자, ISIN Code, 종가들이 저장된 리스트 (list of dict)
            base_date   : 기준일자
            isin_code   : 국제 증권 식별 번호 (12자리)
            close_price : 종가
        """

        try:
            # Parameter Setting
            start_date = dm.datetime.strftime(dm.datetime.now(dm.timezone('Asia/Seoul')) - dm.timedelta(days)  , "%Y%m%d")
            end_date   = dm.YESTERDAY
        
            # Query
            result = self.find_item(
                table='price_info',
                columns=['base_date', 'isin_code', 'close_price'],
                condition=f"CAST('{start_date}' AS date) <= base_date AND base_date <= CAST('{end_date}' AS date)",
                order_by='base_date',
                asc=True
            )

            # Parsing
            rows = list()
            for row in result:
                raw_string = row[0][1:-1]
                data = dict()
                data['base_date'] = raw_string.split(sep=',')[0]
                data['isin_code'] = raw_string.split(sep=',')[1]
                data['close_price'] = int(raw_string.split(sep=',')[2])
                rows.append(data)

            return rows

        except Exception as err_msg:
            print(f"[ERROR] get_close_price_for_days Error: {err_msg}")

    def set_news(self, isin_code:str, write_date:str, headline:str, sentiment:float):
        """
        새로운 뉴스 기사를 데이터베이스에 저장한다.
        해당 종목(short_isin_code)에 연관된 뉴스 기사가 50개를 초과할 경우,
        가장 오래된 뉴스 기사를 제거하고 삽입한다.

        [Parameters]
        isin_code  (str)   : 국제 증권 식별 번호 (12자리)
        write_date (str)   : 뉴스 기사 작성 일자 (Format: YYYYMMDD)
        headline   (str)   : 뉴스 헤드라인
        sentiment  (float) : 뉴스 감정도
        
        [Returns]
        -
        """
        
        try:
            # Parameter Setting
            data = {
                'isin_code'  : isin_code,
                'write_date' : write_date,
                'headline'   : headline,
                'sentiment'  : sentiment
            }

            self.insert_item(table='news_info', columns=['isin_code', 'write_date', 'headline', 'sentiment'], data=data)
            # return self.find_item(table='news_info', columns='news_id', condition=f"isin_code = CAST('{isin_code}' AS {TYPE_news_info['isin_code']}) AND write_date = CAST('{write_date}' AS {TYPE_news_info['write_date']}) AND headline = CAST('{headline}' AS {TYPE_news_info['headline']})")[0][0]

        except Exception as err_msg:
            print(f"[ERROR] set_news Error: {err_msg}")

    def set_multiple_news(self, news_list:list):
        """
        새로운 다수의 뉴스 기사를 데이터베이스에 저장한다.
        해당 종목(short_isin_code)에 연관된 뉴스 기사가 50개를 초과할 경우,
        가장 오래된 뉴스 기사를 제거하고 삽입한다.

        [Parameters]
        news_list (list) : 저장할 다수의 뉴스들의 정보가 담긴 2차원 리스트 ex) [[<isin_code>, <write_date>, <headline>, <sentiment>], ...]
            isin_code  (str)   : 국제 증권 식별 번호 (12자리)
            write_date (str)   : 뉴스 기사 작성 일자 (Format: YYYYMMDD)
            headline   (str)   : 뉴스 헤드라인
            sentiment  (float) : 뉴스 감정도
        
        [Returns]
        -
        """
        
        try:
            # Parameter Setting
            multiple_news = list()
            last_news = dict()
            for news in news_list:
                data = dict()
                data['isin_code'] = news[0]
                data['write_date'] = news[1]
                data['headline'] = news[2]
                data['sentiment'] = news[3]
                multiple_news.append(data)

                last_news['isin_code'] = news[0]
                last_news['write_date'] = news[1]
                last_news['headline'] = news[2]
                last_news['sentiment'] = news[3]

            # Query
            self.insert_items(table='news_info', columns=['isin_code', 'write_date', 'headline', 'sentiment'], data=multiple_news)

        except Exception as err_msg:
            print(f"[ERROR] set_multiple_news Error: {err_msg}")

    def set_new_member(self, member_id:str, member_pw:str, member_email:str):
        """
        신규 회원의 정보를 입력받아 데이터베이스에 저장한다.

        [Parameters]
        member_id    (str) : 신규회원 ID
        member_pw    (str) : 신규회원 비밀번호
        member_email (str) : 신규회원 이메일
        
        [Returns]
        True  : 데이터베이스에 중복된 ID가 존재하지 않아 저장에 성공한 경우

        False : 데이터베이스에 중복된 ID가 존재하여, 저장에 실패한 경우
        """

        try:
            # Duplicates Check
            if len(self.find_item(table='member_info', condition=f"member_id = CAST('{member_id}' AS {TYPE_member_info['member_id']})")) > 0: # 해당 아이디가 이미 존재함
                return False
            
            # Parameter Setting
            columns = list(TYPE_member_info.keys())
            data = [member_id, member_pw, member_email]
            signup_data = dict()
            
            for column, value in zip(columns, data):
                signup_data[column] = value

            # Query
            self.insert_item(table='member_info', columns=columns, data=signup_data)

        except Exception as err_msg:
            print(f"[ERROR] set_new_member Error: {err_msg}")
            return False

    def get_news_by_isin_code(self, isin_code:str):
        """
        해당 종목의 뉴스 데이터를 불러온다.

        [Parameters]
        isin_code (str) : 국제 증권 식별 번호 (12자리)
        
        [Returns]
        list : 해당 주식 종목에 연관된 뉴스 데이터들이 담긴 리스트 (list of dict)
            isin_code  (str)   : 국제 증권 식별 번호 (12자리)
            write_date (str)   : 뉴스 기사 작성 일자 (Format: YYYYMMDD)
            headline   (str)   : 뉴스 헤드라인
            sentiment  (float) : 뉴스 감정도
        """

        try:
            # Query
            result = self.find_item(
                table='news_info',
                columns='ALL',
                condition=f"isin_code = CAST('{isin_code}' AS {TYPE_news_info['isin_code']})",
                order_by='write_date',
                asc=False
            )

            # Parsing
            output = list()

            for news in result:
                data = dict()
                data['isin_code'] = news[0]
                data['write_date'] = news[1].strftime('%Y-%m-%d')
                data['headline'] = news[2]
                data['sentiment'] = float(news[3])
                output.append(data)

            return output

        except Exception as err_msg:
            print(f"[ERROR] get_news_by_isin_code Error: {err_msg}")
            return False

    def add_transaction(self, member_id:str, isin_code:str, break_even_price:float, quantity:int):
        """
        해당 회원의 보유 주식 정보를 입력한다.

        [Parameters]
        member_id        (str)   : 회원 ID
        isin_code        (str)   : 국제 증권 식별 번호 (12자리)
        break_even_price (float) : 평균 매수가
        quantity         (int)   : 보유량

        [Returns]
        True  : 해당 회원(member_id)이 해당 주식 종목(isin_code)에 대한 보유 정보를
                갖고 있지 않은 상태에서 새롭게 입력하는 경우

        False : 해당 회원(member_id)이 해당 주식 종목(isin_code)에 대한 보유 정보를
                이미 기입력해놓은 경우
                (이 경우, update_transaction() 메서드를 이용하여 '수정'해야 한다.)
        """

        try:
            # Duplicates Check 
            duplicates = self.find_item(
                table='portfolio_transaction',
                condition=f"member_id = CAST('{member_id}' AS {TYPE_portfolio_transaction['member_id']}) AND isin_code = CAST('{isin_code}' AS {TYPE_portfolio_transaction['isin_code']})"
            )

            if len(duplicates) > 0:
                print(f"[WARNING] This transaction already exist in database: member_id: {member_id}, isin_code: {isin_code}")
                return False

            # Parameter Setting
            columns = list(TYPE_portfolio_transaction.keys())

            tr = dict()
            tr['member_id'] = member_id
            tr['isin_code'] = isin_code
            tr['break_even_price'] = break_even_price
            tr['quantity'] = quantity

            # Query
            self.insert_item(table='portfolio_transaction', columns=columns, data=tr)

        except Exception as err_msg:
            print(f"[ERROR] add_transaction Error: {err_msg}")
            return False

    def update_transaction(self, member_id:str, isin_code:str, break_even_price:float, quantity:int):
        """
        해당 회원의 보유 주식 정보를 수정한다.

        [Parameters]
        member_id        (str)   : 회원 ID
        isin_code        (str)   : 국제 증권 식별 번호 (12자리)
        break_even_price (float) : 수정할 평균 매수가
        quantity         (int)   : 수정할 보유량

        [Returns]
        True  : 해당 회원(member_id)이 해당 주식 종목(isin_code)에 대한 보유 정보를
                갖고 있는 상태인 경우

        False : 해당 회원(member_id)이 해당 주식 종목(isin_code)에 대한 보유 정보를
                갖고 있지 않은 경우
        """
        
        try:
            # Duplicates Check 
            duplicates = self.find_item(
                table='portfolio_transaction',
                condition=f"member_id = CAST('{member_id}' AS {TYPE_portfolio_transaction['member_id']}) AND isin_code = CAST('{isin_code}' AS {TYPE_portfolio_transaction['isin_code']})"
            )

            if len(duplicates) == 0:
                print(f"[WARNING] There is no this transaction: member_id: {member_id}, isin_code: {isin_code}")
                return False
            elif len(duplicates) > 1:
                print(f"[ERROR] Anomaly detected for this transaction: member_id: {member_id}, isin_code: {isin_code}")
                return False

            # Parameter Setting
            condition = f"member_id = CAST('{member_id}' AS {TYPE_portfolio_transaction['member_id']}) AND isin_code = CAST('{isin_code}' AS {TYPE_portfolio_transaction['isin_code']})"
            
            # Query
            self.update_item(table='portfolio_transaction', column='break_even_price', value=break_even_price, condition=condition)
            self.update_item(table='portfolio_transaction', column='quantity', value=quantity, condition=condition)
            return True

        except Exception as err_msg:
            print(f"[ERROR] update_transaction Error: {err_msg}")
            return False

    def remove_transaction(self, member_id:str, isin_code:str):
        """
        해당 회원의 보유 주식 정보를 제거한다.

        [Parameters]
        member_id (str) : 회원 ID
        isin_code (str) : 국제 증권 식별 번호 (12자리)

        [Returns]
        True  : 해당 회원(member_id)이 해당 주식 종목(isin_code)에 대한 보유 정보를
                갖고 있는 상태인 경우

        False : 해당 회원(member_id)이 해당 주식 종목(isin_code)에 대한 보유 정보를
                갖고 있지 않은 경우
        """
        
        try:
            # Existence Check 
            existence = self.find_item(
                table='portfolio_transaction',
                condition=f"member_id = CAST('{member_id}' AS {TYPE_portfolio_transaction['member_id']}) AND isin_code = CAST('{isin_code}' AS {TYPE_portfolio_transaction['isin_code']})"
            )

            if len(existence) == 0:
                print(f"[WARNING] There is no this transaction: member_id: {member_id}, isin_code: {isin_code}")
                return False
            elif len(existence) > 1:
                print(f"[ERROR] Anomaly detected for this transaction: member_id: {member_id}, isin_code: {isin_code}")
                return False

            # Parameter Setting
            condition = f"member_id = CAST('{member_id}' AS {TYPE_portfolio_transaction['member_id']}) AND isin_code = CAST('{isin_code}' AS {TYPE_portfolio_transaction['isin_code']})"

            # Query
            self.delete_item(table='portfolio_transaction', condition=condition)
            return True            

        except Exception as err_msg:
            print(f"[ERROR] remove_transaction Error: {err_msg}")
            return False
    
    def get_portfolio_by_member_id(self, member_id):
        """
        해당 회원의 포트폴리오 구성 정보를 반환한다.

        [Parameters]
        member_id (str) : 회원 ID

        [Returns]
        list : 해당 회원의 포트폴리오 구성 요소를 담은 리스트 (list of dict)
            member_id        (str)   : 회원 ID
            isin_code        (str)   : 국제 증권 식별 번호 (12자리)
            break_even_price (float) : 수정할 평균 매수가
            quantity         (int)   : 수정할 보유량

        None  : 해당 회원의 포트폴리오가 비어있는 경우

        False : 오류가 발생한 경우
        """

        try:
            # Existence Check 
            existence = self.find_item(
                table='portfolio_transaction',
                condition=f"member_id = CAST('{member_id}' AS {TYPE_portfolio_transaction['member_id']})"
            )

            if len(existence) == 0:
                print(f"[WARNING] There is no any transaction this member: member_id: {member_id}")
                return None

            # Parameter Setting
            condition = f"member_id = CAST('{member_id}' AS {TYPE_portfolio_transaction['member_id']})"

            # Query
            result = self.find_item(table='portfolio_transaction', columns='ALL', condition=condition)

            # Parsing
            output = list()

            for news in result:
                data = dict()
                data['member_id'] = news[0]
                data['isin_code'] = news[1]
                data['quantity'] = int(news[2])
                data['break_even_price'] = float(news[3])
                output.append(data)

            return output

        except Exception as err_msg:
            print(f"[ERROR] get_portfolio_by_member_id Error: {err_msg}")
            return False
    
    def get_fluctuation_rate_of_world_index_price(self):
        """
        가장 최근의 세계 지수들의 등락률을 반환한다.

        [Parameters]
        -
        
        [Returns]
        list  : 세계지수 정보와 등락률이 담긴 리스트 (list of dict)
            index_name       (str)   : 지수명
            base_date        (str)   : 기준 일자
            fluctuation_rate (float) : 등락률
        """

        try:
            # Parameter Setting
            condition = f"base_date = CAST('{dm.YESTERDAY}' AS {TYPE_world_index_price['base_date']})"

            # Query
            result = self.find_item(table='world_index_price', columns=['ticker', 'base_date', 'fluctuation_rate'], condition=condition)

            # Parsing
            rows = list()
            for row in result:
                raw_string = row[0][1:-1]
                data = dict()
                data['index_name'] = self.get_index_name_by_ticker(raw_string.split(sep=',')[0])
                data['base_date'] = raw_string.split(sep=',')[1]
                data['fluctuation_rate'] = float(raw_string.split(sep=',')[2])
                rows.append(data)

            return rows

        except Exception as err_msg:
            print(f"[ERROR] get_portfolio_by_member_id Error: {err_msg}")
            return False

    def get_index_name_by_ticker(self, ticker:str):
        """
        티커에 해당되는 지수명을 반환한다.

        [Parameters]
        ticker (str) : 티커
        
        [Returns]
        str : 지수명
        """

        try:
            # Parameter Setting
            condition = f"ticker = CAST('{ticker}' AS {TYPE_world_index_info['ticker']})"

            # Query
            return self.find_item(table='world_index_info', columns='index_name', condition=condition)[0][0]

        except Exception as err_msg:
            print(f"[ERROR] get_index_name_by_ticker Error: {err_msg}")
            return False        

pgdb = PostgresHandler(user='byeong_heon', password='kbitacademy')
print(pgdb.get_fluctuation_rate_of_world_index_price())