# Author      : 조익준
# Contact     : harry960629@naver.com
# Date        : 2022-10-27(목)

# [Description]
# - 한 종목에 대한 최신 뉴스 50개를 웹 크롤링을 통해 추출하여 뉴스 기사를 fine-tunning한 kb-albert 모델로 호재, 악재를 분석한다.
# - 분석이 끝난 후에는 DB에 [티커, 기사헤드라인, 기사날짜, 감정분석지표] 형태로 저장한다.



# Required Modules
import requests
from . import news_albert      as na
import pandas           as pd

from bs4  import BeautifulSoup



# Function Declaration
def get_last(stock_ticker):
    """
    뉴스 마지막 페이지 숫자를 구한다.
    [Parameters]
    stock_ticker (str) : 종목의 티커

    [Returns]
    last_number (int) : 뉴스 마지막 페이지
    """

    # crawling
    url = f'https://finance.naver.com/item/news_news.naver?code={stock_ticker}&page=&sm=title_entity_id.basic&clusterId='
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html.parser')
    
    try:
        last = soup.select_one('td.pgRR > a')['href']
        last_number = int(last.split('&')[1][5:])
    
    except TypeError:
        last_number = 1
    
    return last_number

def get_data(isin_code:str, short_isin_code:str):
    """
    해당 종목의 최신 뉴스를 최대 50개까지 받아와서 튜닝한 모델로 호재, 악재 분석 후
    [티커, 기사헤드라인, 기사날짜, 감정분석지표] 리스트 형태로 DB에 저장한다.

    [Parameters]
    isin_code       (str) : 국제 증권 식별 번호 (12자리)
    short_isin_code (str) : 국제 증권 식별 번호 (6자리, 축약형)

    [Returns]
    list : 뉴스 정보가 담긴 리스트 (list of list)
        isin_code (str)   : 국제 증권 식별 번호 (12자리)
        write_date(str)   : 뉴스 작성 일자
        headline  (str)   : 뉴스 헤드라인
        sentiment (float) : 감성도 [-1.0, +1.0]
    """
    
    # DB에 저장할 데이터 리스트 생성
    data_list = []

    # 뉴스 최대 페이지가 5페이지 미만일 경우 최대 페이지를 마지막 페이지로 지정
    last_number = get_last(short_isin_code)
    if last_number < 5:
        end = last_number + 1
    else: end = 6

    for page in range(1, end):

        # 뉴스 n page url 설정
        url = f'https://finance.naver.com/item/news_news.naver?code={short_isin_code}&page={page}&sm=title_entity_id.basic&clusterId='
        
        # 해당 뉴스 url 추출
        res = requests.get(url)
        # soup = BeautifulSoup(res.content, 'html.parser', from_encoding='utf-8')
        soup = BeautifulSoup(res.content.decode('euc-kr','replace'), 'html.parser')
        items = soup.select('body > div > table > tbody > tr > td > a')

        # 페이지 내 기사 각각의 호재, 악재 분석 후 data_list에 추가
        for i in range(len(items)):
            url2 = 'https://finance.naver.com' + items[i]['href']
            res_items = requests.get(url2)
            frame_items = BeautifulSoup(res_items.content.decode('euc-kr','replace'), 'html.parser')

            newsheadline = frame_items.select_one('strong.c').get_text()
            newsdate = ''.join(frame_items.select_one('span.tah').get_text().split('.')).strip()[:8]

            # Ignore Empty News
            if (newsheadline is None) or (newsdate is None):
                continue

            # Quotation 제거
            newsheadline = newsheadline.replace("'", " ")
            
            data_list.append([isin_code, newsdate, newsheadline, na.get_score(newsheadline)])   # [isin_code, write_date, headline, sentiment]

    return data_list
