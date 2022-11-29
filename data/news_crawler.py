# Author  : 조익준           
# Contact : harry960629@naver.com  
# Date    : 2022-11-29(화)



# Required Modules
import requests
import pandas as pd

from bs4 import BeautifulSoup



# Functions
def get_data(short_isin_code, page):
    """
    네이버 뉴스 포털에서 해당 종목에 대한 뉴스를 크롤링하여 반환한다.

    [Parameters]
    short_isin_code (str) : 국제 증권 식별 번호 (축약형, 6자리)
    page            (int) : 페이지 번호

    [Returns]
    pandas.DataFrame : 기사 정보를 저장한 DataFrame
        티커     (str) : 국제 증권 식별 번호 (축약형, 6자리)
        날짜     (str) : 기사 작성 날짜
        뉴스제목 (str) : 뉴스 헤드라인
    """

    # 뉴스 n page url 설정
    url = f'https://finance.naver.com/item/news_news.naver?code={short_isin_code}&page={page}&sm=title_entity_id.basic&clusterId='
    
    # 해당 뉴스 url 추출
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html.parser')
    items = soup.select('body > div > table > tbody > tr > td > a')
    
    # 뉴스 날짜시간, 제목 리스트로 저장
    head_list = []
    date_list = []
    for i in range(len(items)):
        url2 = 'https://finance.naver.com' + items[i]['href']
        res_items = requests.get(url2)
        frame_items = BeautifulSoup(res_items.content, 'html.parser')
        head_list.append(frame_items.select_one('strong.c').get_text())
        date_list.append(frame_items.select_one('span.tah').get_text())
    
    # 과정 확인 메시지 출력
    # print(f'{stock_ticker} : {page} page 완료')
    
    # 데이터프레임으로 합치기  
    df = pd.concat([pd.DataFrame({'날짜':date_list}), pd.DataFrame({'뉴스제목':head_list})],axis=1)
    df['티커'] = short_isin_code
    df = df[['티커', '날짜', '뉴스제목']]
    
    return df

# 뉴스 마지막 페이지 숫자 구하기
def get_last(short_isin_code):
    """
    해당 종목의 뉴스의 마지막 페이지 번호를 반환한다.

    [Parameters]
    short_isin_code (str) : 국제 증권 식별 번호 (축약형, 6자리)

    [Returns]
    int : 해당 종목의 뉴스의 마지막 페이지 번호
    """

    url = f'https://finance.naver.com/item/news_news.naver?code={short_isin_code}&page=&sm=title_entity_id.basic&clusterId='
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html.parser')
    last = soup.select_one('td.pgRR > a')['href']
    last_number = int(last.split('&')[1][5:])
    
    return last_number
