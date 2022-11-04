
# Author  : 이병헌
# Contact : lww7438@gmail.com
# Date    : 2022-11-04(금)



# Required Modules
from . import postgres_core    as core
from . import conn_config      as config
from . import data_manipulator as dm
from . import schema
# from . import set_news         as news    # Works only on Windows Environment



# Class Declaration
class PostgresHandler(core.PostgresCore):

    # * * *    Constructor    * * *
    def __init__(self, user:str, password:str):

        self._client = core.PostgresCore(
            user     = user,
            password = password            
        )

        self.conn_user = user
        self.cursor = self._client.cursor()

    # * * *    High-Level Methods    * * *
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
            result = self.find_item(table='basic_stock_info', columns=['isin_code', 'item_name'], condition=f"item_name LIKE CAST('%{item_name}%' AS {schema.TYPE_basic_stock_info['item_name']})")

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

    def  set_multiple_news(self, news_list:list):
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
