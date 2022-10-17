
# Author  : 이병헌
# Contact : lww7438@gmail.com
# Date    : 2022-10-17(월)



# Required Modules
import psycopg2


# Connection Information
HOST     = 'pm330.c68tjqdjajqc.us-east-1.rds.amazonaws.com'
PORT     = 5432   # Default port number of postgres
USER     = 'postgres'
password = '12341234'



# Configurations
TYPE_basic_stock_info = {
    'isin_code'       : 'varchar',
    'short_isin_code' : 'varchar',
    'market_category' : 'varchar',
    'item_name'       : 'varchar',
    'corp_name'       : 'varchar',
    'corp_number'     : 'varchar',
    'corp_url'        : 'varchar',
    'list_date'       : 'date',
    'face_value'      : 'int',
    'issue_cnt'       : 'bigint',
    'industry'        : 'varchar',
}

TYPE_member_info = {
    'member_id'         : 'varchar',
    'member_pw'         : 'varchar',
    'member_name'       : 'varchar',
    'member_phone'      : 'varchar',
    'brokerage_firm'    : 'varchar',
    'brokerage_account' : 'varchar',
    'member_propensity' : 'varchar',
}

TYPE_price_info = {
    'base_date'        : 'date',
    'isin_code'        : 'varchar',
    'market_price'     : 'int',
    'close_price'      : 'int',
    'high_price'       : 'int',
    'low_price'        : 'int',
    'fluctuation'      : 'int',
    'fluctuation_rate' : 'float8',
    'cum_volume'       : 'bigint',
}

TYPE_news_info = {
    'isin_code'  : 'varchar',
    'write_date' : 'date',
    'headline'   : 'varchar',
    'sentiment'  : 'float8',
}

TYPE_financial_info = {
    'isin_code'  : 'varchar',
    'bps'        : 'date',
    'per'        : 'varchar',
    'pbr'        : 'float8',
    'eps'        : 'int',
    'div'        : 'float8',
    'dps'        : 'int',
}

LIST_TABLE_NAME = [
    'basic_stock_info',
    'member_info',
    'price_info',
    'news_info',
    'financial_info'
]

SCHEMA = {
    'basic_stock_info' : TYPE_basic_stock_info,
    'member_info'      : TYPE_member_info,
    'price_info'       : TYPE_price_info,
    'news_info'        : TYPE_news_info,
    'financial_info'   : TYPE_financial_info,
}

STR_TYPES = ['varchar', 'text', 'char', 'date']



# Functions
def get_type_by_column_name(table:str=None, column:str=None):
    
    try:
        return SCHEMA[table][column]

    except Exception as err_msg:
        print(f"[ERROR] Schema Error: {err_msg}")



# Class Declaration
class PostgresHandler():

    def __init__(self):

        self._client = psycopg2.connect(
            host     = HOST , 
            user     = USER,
            password = password,
            port     = PORT
        )

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

    def insert_item(self, schema:str='postgres', table:str=None, data:dict=None):

        if (table not in LIST_TABLE_NAME) or (table is None):
            raise f"[ERROR] Invalid Table Name: {table} does not exist"

        if data is None:
            raise f"[ERROR] Empty Data Insertion: data is empty"

        columns = ""
        values = ""

        for column, value in data.items():
            columns += column + ", "
            if get_type_by_column_name(table, column) in STR_TYPES:
                values += "'" + value + "', "
            else:
                values += value + ", "

        columns = columns[:-2]
        values = values[:-2]

        sql = f" INSERT INTO {schema}.{table} ({columns}) VALUES ({values}) ;"

        try:
            self.cursor.execute(sql)
            self._client.commit()

        except Exception as err_msg :
            print(f"[ERROR] Insert Error: {err_msg}") 
    
    def find_item(self, table:str=None, column='ALL', condition=None):

        if (table not in LIST_TABLE_NAME) or (table is None):
            raise f"[ERROR] Invalid Table Name: {table} does not exist"

        if column is None:
            raise f"[ERROR] Empty Data Insertion: data is empty"
        elif column == 'ALL':
            column = "*"
        else:
            column = ", ".join(column)

        if condition is None:
            sql = f" SELECT {column} FROM {table} ;"
        else:
            sql = f" SELECT {column} FROM {table} WHERE {condition} ;"

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

    def get_close_price(self, ticker, start_date, end_date):
        pass
