
# Author  : 이병헌
# Contact : lww7438@gmail.com
# Date    : 2022-10-17(월)



# Required Modules
from asyncio.windows_events import NULL
import psycopg2

from config import *



# Configurations
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
    'isin_code'  : 'varchar',
    'bps'        : 'integer',
    'per'        : 'double precision',
    'pbr'        : 'double precision',
    'eps'        : 'integer',
    'div'        : 'double precision',
    'dps'        : 'integer',
}

TYPE_member_info = {
    'member_id'         : 'varchar',
    'member_pw'         : 'varchar',
    'member_name'       : 'varchar',
    'member_email'      : 'varchar',
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

    def __init__(self, host, port, user, password):

        self._client = psycopg2.connect(
            host     = host ,
            port     = port ,
            user     = user ,
            password = password            
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
    
    def find_item(self, table:str=None, column='ALL', condition:str=None):

        if (table not in LIST_TABLE_NAME) or (table is None):
            raise f"[ERROR] Invalid Table Name: {table} does not exist"

        if column == 'ALL':
            column = "*"
        elif type(list()) == type(column):
            column = ", ".join(column)
            column = "(" + column + ")"
        elif type(str()) == type(column):
            pass
            
        if condition is None:
            sql = f""" SELECT {column} FROM {table} ;"""
        else:
            sql = f""" SELECT {column} FROM {table} WHERE {condition} ;"""

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
