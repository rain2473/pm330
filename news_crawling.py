# Author  : 조익준          
# Contact : harry960629@naver.com      
# Date    : 2022-10-19(수)

# Required Modules  (from, import, as 구문들의 Indentation을 맞춘다.)
import requests
import time
import pandas as pd

from bs4    import BeautifulSoup
from pykrx  import stock

# Function Declaration
def get_data(stock_ticker, page):
    """
    종목티커와 페이지 번호를 받아서 해당 종목의 네이버 금융 종목 뉴스 페이지의 헤드라인과 게시날짜 및 시간을 데이터프레임으로 반환한다.
    
    [Parameters]
    stock_ticker : 종목의 티커
    page : 뉴스 페이지 번호

    [Returns]
    df : 헤드라인, 기사날짜, 티커가 포함된 데이터프레임
    """

    # 뉴스 n page url 설정
    url = f'https://finance.naver.com/item/news_news.naver?code={stock_ticker}&page={page}&sm=title_entity_id.basic&clusterId='
    
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
    print(f'{stock_ticker} : {page} page 완료')
    
    # 데이터프레임으로 합치기  
    df = pd.concat([pd.DataFrame({'날짜':date_list}), pd.DataFrame({'뉴스제목':head_list})],axis=1)
    df['티커'] = stock_ticker
    df = df[['티커', '날짜', '뉴스제목']]
    
    return df

def get_last(stock_ticker):
    """
    종목티커를 받아 해당 종목의 뉴스의 마지막 페이지 번호를 반환한다.
    
    [Parameters]
    stock_ticker : 종목의 티커

    [Returns]
    last_number : 뉴스 마지막 페이지 번호
    """

    url = f'https://finance.naver.com/item/news_news.naver?code={stock_ticker}&page=&sm=title_entity_id.basic&clusterId='
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html.parser')
    last = soup.select_one('td.pgRR > a')['href']
    last_number = int(last.split('&')[1][5:])
    
    return last_number


# Constants
# 티커 리스트(코스피 200) 사용 시 주석 해제
# TICKER_LIST = stock.get_index_portfolio_deposit_file("1028")

# 데이터 추출
for tick in TICKER_LIST:
    news_data = pd.DataFrame()
    try:
        start, end = 1, get_last(tick)
    except Exception as e:  # 뉴스가 1페이지 밖에 없을 경우
        print(e)
        start, end = 1, 1

    print(start, end)
    now_page = start

    while now_page <= end:
        try:
            news_data = pd.concat([news_data, get_data(tick, now_page)], ignore_index=True)
            now_page += 1
            time.sleep(2)
        except Exception as e:  # 로딩 오류 발생 시 재실행
            print(e)

    # 종목 뉴스 검색 종료 후 저장
    news_data.to_csv(f'./{tick}_{start}-{end}.csv')
    display(news_data)