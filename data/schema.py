# Author  : 이병헌
# Contact : lww7438@gmail.com
# Date    : 2022-11-04(금)



# Columns of Table: info_stock
TYPE_info_stock = {
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

# Columns of Table: info_financial
TYPE_info_financial = {
    'base_date' : 'date',
    'isin_code' : 'varchar',
    'bps'       : 'integer',
    'per'       : 'double precision',
    'pbr'       : 'double precision',
    'eps'       : 'integer',
    'div'       : 'double precision',
    'dps'       : 'integer',
}

# Columns of Table: info_member
TYPE_info_member = {
    'member_id'    : 'varchar',
    'member_pw'    : 'varchar',
    'member_email' : 'varchar',
}

# Columns of Table: info_news
TYPE_info_news = {
    'isin_code'  : 'varchar',
    'write_date' : 'date',
    'headline'   : 'varchar',
    'sentiment'  : 'double precision',
    'news_id'    : 'bigint', # Serial8 Type
}

# Columns of Table: transaction_portfolio
TYPE_transaction_portfolio = {
    'member_id'        : 'varchar',
    'isin_code'        : 'varchar',
    'quantity'         : 'integer',
    'break_even_price' : 'double precision',
}

# Columns of Table: price_konex
TYPE_price_konex = {
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

# Columns of Table: price_kosdaq
TYPE_price_kosdaq = {
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

# Columns of Table: price_kospi
TYPE_price_kospi = {
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

# Columns of Table: info_world_index
TYPE_info_world_index = {
    'ticker'     : 'varchar',
    'nation'     : 'varchar',
    'index_name' : 'varchar',
    'lat'        : 'double precision',
    'lon'        : 'double precision',
}

# Columns of Table: price_world_index
TYPE_price_world_index = {
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

# List of Table
LIST_TABLE_NAME = [
    'info_stock',
    'info_financial',
    'info_member',
    'info_news',
    'transaction_portfolio',
    'price_konex',
    'price_kosdaq',
    'price_kospi',
    'info_world_index',
    'price_world_index'
]

# Dictionaries of Database Tables
SCHEMA = {
    'info_stock'            : TYPE_info_stock,
    'info_financial'        : TYPE_info_financial,
    'info_member'           : TYPE_info_member,
    'info_news'             : TYPE_info_news,
    'transaction_portfolio' : TYPE_transaction_portfolio,
    'price_konex'           : TYPE_price_konex,
    'price_kosdaq'          : TYPE_price_kosdaq,
    'price_kospi'           : TYPE_price_kospi,
    'info_world_index'      : TYPE_info_world_index,
    'price_world_index'     : TYPE_price_world_index,
}

# Data Types
STR_TYPES  = ('varchar', 'text', 'char', 'date')
NULL_TYPES = (None, 'None', 'none', 'NULL', 'null', 'nullptr', '')

# Market Types
MARKET_CATEGORIES = ('KOSPI', 'KOSDAQ', 'KONEX')



# General Function
def get_type_by_column_name(table:str=None, column:str=None):
        """
        테이블의 속성의 데이터 타입을 반환한다.

        [Parameters]
        table  (str) : 속성이 속한 테이블 이름 (default: None)

        [Returns]
        str   : 해당 속성의 데이터 타입
        False : 해당 column이 존재하지 않는 경우
        """
    
        try:
            if SCHEMA.get(table, False) is not False:
                return SCHEMA[table].get(column, False)
            else:
                return False

        except Exception as err_msg:
            print(f"[ERROR] Schema Error: {err_msg}")
