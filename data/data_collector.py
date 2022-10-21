
# Author  : 이병헌
# Contact : lww7438@gmail.com
# Date    : 2022-10-20(목)



# Required Modules
import api_handler      as api
import postgres_handler as pg
import data_manipulator as dm
import config



# Function Declaration
def build_basic_stock_info(id:str, pw:str):
    """
    데이터베이스의 basic_stock_info 테이블에 KOSPI, KOSDAQ, KONEX에 상장된 전 종목에 대한 기본정보들을 저장한다.

    [Parameters]
    -
    
    [Returns]
    -
    """

    if(not(id == config.ID_DBA and id == config.PW_DBA)):
        print("Only DBA can run the build function")
        return False

    try:
        # DB Construction
        pgdb = pg.PostgresHandler(user=id, password=pw)
        
        # Clear Table
        pgdb.delete_item(table='basic_stock_info')
        
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
        pgdb.insert_items(table='basic_stock_info', columns=['isin_code', 'short_isin_code', 'market_category', 'item_name', 'corp_name', 'corp_number', 'listing_date', 'issue_cnt', 'industry', 'face_value'], data=basic_stock_info)

    except Exception as err_msg:
        print(f"[ERROR] build_basic_stock_info: {err_msg}")

def build_price_info(id:str, pw:str):
    
    if(not(id == config.ID_DBA and id == config.PW_DBA)):
        print("Only DBA can run the build function")
        return False

    try:
        # DB Construction
        pgdb = pg.PostgresHandler(user=id, password=pw)
        
        # Listing All Stock
        krx_listed_info = api.get_krx_listed_info(serviceKey=config.API_KEY_OPEN_DATA_PORTAL, numOfRows=3)

        for kr_stock in krx_listed_info:

            list_ohlcv = api.get_market_ohlcv_by_date(short_isin_code=kr_stock['shotnIsin'])

            for ohlcv in list_ohlcv:
                ohlcv['isin_code'] = kr_stock['isinCd']
                ohlcv.pop('short_isin_code')

            pgdb.insert_items(table='price_info', columns=['base_date', 'isin_code', 'market_price', 'close_price', 'high_price', 'low_price', 'fluctuation', 'fluctuation_rate', 'volume'], data=list_ohlcv)
    
    except Exception as err_msg:
        print(f"[ERROR] build_price_info Error: {err_msg}")

def build_news_info(id:str, pw:str):

    if(not(id == config.ID_DBA and id == config.PW_DBA)):
        print("Only DBA can run the build function")
        return False

    try:
        # DB Construction
        pgdb = pg.PostgresHandler(user=id, password=pw)

    except Exception as err_msg:
        print(f"[ERROR] build_news_info Error: {err_msg}")

def build_financial_info(id:str, pw:str):

    if(not(id == config.ID_DBA and id == config.PW_DBA)):
        print("Only DBA can run the build function")
        return False

    try:
        # DB Construction
        pgdb = pg.PostgresHandler(user=id, password=pw)
        
    except Exception as err_msg:
        print(f"[ERROR] build_financial_info Error: {err_msg}")

def get_all_data(id:str, pw:str, table:str=None, column:str='ALL'):
    
    try:
        # DB Construction
        pgdb = pg.PostgresHandler(user=id, password=pw)
        
        return pgdb.find_item(table=table, column=column)
    
    except Exception as err_msg:
        print(f"[ERROR] get_all_data Error: {err_msg}")

def get_isin_code(id:str, pw:str, short_isin_code:str):

    try:
        # DB Construction
        pgdb = pg.PostgresHandler(user=id, password=pw)
    
        # Query
        result = pgdb.find_item(table='basic_stock_info', column='isin_code', condition=f"short_isin_code = CAST('{short_isin_code}' AS varchar)")

        return result[0][0]

    except Exception as err_msg:
        print(f"[ERROR] get_isin_code Error: {err_msg}")

def get_short_isin_code(id:str, pw:str, isin_code:str):

    try:
        # DB Construction
        pgdb = pg.PostgresHandler(host=config.POSTGRES_HOST, port=config.POSTGRES_PORT, user=config.POSTGRES_USER, password=config.POSTGRES_USER_PW)
    
        # Query
        result = pgdb.find_item(table='basic_stock_info', column='short_isin_code', condition=f"isin_code = CAST('{isin_code}' AS varchar)")

        return result[0][0]

    except Exception as err_msg:
        print(f"[ERROR] get_short_isin_code Error: {err_msg}")

def get_close_price(id:str, pw:str, isin_code:str, start_date:str='20000101', end_date:str=dm.YESTERDAY):
    """
    ISIN Code에 해당하는 종목의 종가를 조회한다.

    [Parameters]
    isin_code  (str) : 국제 증권 식별 번호 (13자리)
    start_date (str) : 조회를 시작할 날짜 (Format: YYYYMMDD) (Default: '20000101')
    end_date   (str) : 조회를 종료할 날짜 (Format: YYYYMMDD) (Default: 전일 (KST 기준))
    
    [Returns]
    list  : 기준일자:종가 Pair들이 저장된 리스트 (list of dict) ex) {'base_date': <기준일자>, 'isin_code':<국제증권식별번호>, 'close_price': <종가>}
    """

    try:
        # DB Construction
        pgdb = pg.PostgresHandler(host=config.POSTGRES_HOST, port=config.POSTGRES_PORT, user=config.POSTGRES_USER, password=config.POSTGRES_USER_PW)

        # Query
        result = pgdb.find_item(table='price_info', column=['base_date', 'isin_code', 'close_price'], condition=f"isin_code = CAST('{isin_code}' AS varchar) AND CAST('{start_date}' AS date) <= base_date AND base_date <= CAST('{end_date}' AS date)")

        # Parsing
        rows = []
        data = {}
        for row in result:
            raw_string = row[0][1:-1]
            data['base_date'] = raw_string.split(sep=',')[0]
            data['isin_code'] = raw_string.split(sep=',')[1]
            data['close_price'] = int(raw_string.split(sep=',')[2])
            rows.append(data)

        return rows

    except Exception as err_msg:
        print(f"[ERROR] get_close_price Error: {err_msg}")

def get_sentiment_by_ticker():
    pass

def get_news():
    pass

def get_unscoring_news():
    pass

def set_setiment_by_news_id():
    pass

def set_news():
    pass

def set_new_member(*new_member):
    """
    신규 회원의 정보를 입력받아 데이터베이스에 저장한다.

    [Parameters]
    *new_member : 신규가입할 회원의 정보
        member_id    (str) : 회원 ID
        member_pw    (str) : 회원 비밀번호
        member_email (str) : 회원 이메일
    
    [Returns]
    True  : 데이터베이스에 중복된 ID가 존재하지 않아 저장에 성공한 경우
    False : 데이터베이스에 중복된 ID가 존재하여, 저장에 실패한 경우
    """

    try:
        # DB Construction
        pgdb = pg.PostgresHandler(host=config.POSTGRES_HOST, port=config.POSTGRES_PORT, user=config.POSTGRES_USER, password=config.POSTGRES_USER_PW)
    
        # Parameter Setting
        columns = pg.TYPE_member_info.keys()
        signup_data = dict()

        for column, data in zip(columns, new_member):
            signup_data[column] = data

        # Query
        result = pgdb.insert_item(table='member_info', column=new_member, data=signup_data)

        if result is not None:
            return True

    except Exception as err_msg:
        print(f"[ERROR] set_new_member Error: {err_msg}")
        return False
        