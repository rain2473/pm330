# import requests
# from bs4 import BeautifulSoup
# import pandas as pd

# def get_data(stock_ticker, page):
#     # 뉴스 n page url 설정
#     url = f'https://finance.naver.com/item/news_news.naver?code={stock_ticker}&page={page}&sm=title_entity_id.basic&clusterId='
    
#     # 해당 뉴스 url 추출
#     res = requests.get(url)
#     soup = BeautifulSoup(res.content, 'html.parser')
#     items = soup.select('body > div > table > tbody > tr > td > a')
    
#     # 뉴스 날짜시간, 제목 리스트로 저장
#     head_list = []
#     date_list = []
#     for i in range(len(items)):
#         url2 = 'https://finance.naver.com' + items[i]['href']
#         res_items = requests.get(url2)
#         frame_items = BeautifulSoup(res_items.content, 'html.parser')
#         head_list.append(frame_items.select_one('strong.c').get_text())
#         date_list.append(frame_items.select_one('span.tah').get_text())
    
#     # 과정 확인 메시지 출력
#     # print(f'{stock_ticker} : {page} page 완료')
    
#     # 데이터프레임으로 합치기  
#     df = pd.concat([pd.DataFrame({'날짜':date_list}), pd.DataFrame({'뉴스제목':head_list})],axis=1)
#     df['티커'] = stock_ticker
#     df = df[['티커', '날짜', '뉴스제목']]
    
#     return df

# # 뉴스 마지막 페이지 숫자 구하기
# def get_last(stock_ticker):
#     url = f'https://finance.naver.com/item/news_news.naver?code={stock_ticker}&page=&sm=title_entity_id.basic&clusterId='
#     res = requests.get(url)
#     soup = BeautifulSoup(res.content, 'html.parser')
#     last = soup.select_one('td.pgRR > a')['href']
#     last_number = int(last.split('&')[1][5:])
    
#     return last_number
