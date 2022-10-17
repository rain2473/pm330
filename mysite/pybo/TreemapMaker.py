# Author  : <홍윤기> 
# Contact : <rain2473@naver.com>
# Date    : <2022-10-16>

# Required Modules
import pandas                                                           as pd
import numpy                                                            as np
import plotly.graph_objects                                     as go
from tkinter                                                             import Tk
from _tkinter                                                           import TclError
from selenium                                                           import webdriver
from selenium.webdriver.common.keys                     import Keys
from selenium.webdriver.common.action_chains       import ActionChains
from plotly                                                               import express                        as px

# Function Declaration
def copy_page():
    '''
    한경데이터 전종목시세 페이지를 열어 전체복사,  클립보드에 문자열을 저장하는 함수이다.
    [Parameters]
        없음
    [Returns]
        없음 
    '''
    # 크롬 브라우저 지정
    driver = webdriver.Chrome("templates\pybo\chromedriver.exe")
    # url 지정(한경 데이터 센터)
    url = "https://datacenter.hankyung.com/equities-all"
    # url 주소 page 접속
    driver.get(url)
    # ctrl a
    ActionChains(driver).key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).perform()
    # ctrl c
    ActionChains(driver).key_down(Keys.CONTROL).send_keys('c').key_up(Keys.CONTROL).perform()
    # 닫기
    driver.close()
    
def get_clipboard():
    '''
    클립보드에 저장된 전종목시세를 문자열 객체로 저장하는 함수이다.
    [Parameters]
        없음
    [Returns]
        str : 클립보드에 저장된 전종목시세 정보
    '''
    # 페이지 복사 함수 실행
    copy_page()
    # tkinter 모듈의 Tk() 호출
    root = Tk()
    # 외부 윈도우 창을 화면에서 제거
    root.withdraw()
    # result 객체를 선언함.
    result = None
    # 클립보드의 내용을 result 객체에 저장하는 것을 시도함.
    try:
        result = root.clipboard_get()
    # 클립보드가 비어있는 경우 오류코드를 보여줌.
    except TclError as e:
        print('클립보드 데이터가 비어있음')
    # 클립보드를 문자열로 반환함.
    return result

def make_list():
    '''
    문자열 result의 전종목시세를 2차원 list의 형태로 변환하는 함수이다.
    [Parameters]
        없음
    [Returns]
        list : 전종목 시세
    '''
    # 문자열을 list를 변환
    crawling_data = get_clipboard().split('\n')
    # 불필요한 정보를 제거
    crawling_data = crawling_data[23:5603]
    # 섹터 이름을 저장한 list를 선언
    sector_list = ['전기,전자',"금융업",'화학','서비스업','운수장비','의약품','유통업',
            '철강및금속','운수창고','기계','음식료품','통신업','비금속광물','전기가스업',
            '건설업','섬유,의복','의료정밀','종이,목재']
    # 전종목 시세를 저장할 list를 선언
    stock_list=[]
    # 2차원 list의 한 줄(한 종목)의 정보를 저장할 list를 선언
    one_row=[]
    # 원본 list에 대하여 반복문 실행
    for element in crawling_data:
        # 빈 문자열은 무시함
        if element == '':
            pass
        # 섹터 이름의 경우
        elif element in sector_list:
            # 변수 sector에 섹터 이름 저장
            sector = element
        # 문자열'%'가 포함된 등락율 정보가 나오는 경우
        elif '%' in element:
            # 이후 실수형으로 변환하기 위해 '%'를 제거
            element = element.replace('%','')
            # 해당 종목의 정보 list에 등락율 저장
            one_row.append(element)
            # 해당 종목의 모든 정보를 저장하였으므로, 문자열로 표시될 등락율%와 섹터 이름을 저장
            one_row += [(element+'%'),sector]
            # 해당 종목의 정보를 전종목 시세를 저장할 list에 저장
            stock_list.append(one_row)
            # 한 종목의 정보를 저장할 list를 초기화
            one_row=[]
        # 특별한 작업이 필요하지 않은 종목 정보인 경우
        else:
            # 현재가 정보인 경우
            if ',' in element:
                # 현재가를 정수형태로 변환하기 위해 ',' 제거
                element = element.replace(',','')
            # 정보를 해당 종목 list에 저장
            one_row.append(element)
    # 전종목 시세 리스트를 반환
    return stock_list

def make_df():
    '''
    list 형태의 데이터를 데이터 프레임으로 변환하며 전처리하는 함수이다.
    이 함수에서 진행하는 전처리는 다음과 같다.
    - DF columns 순서 변경
    - data type 변경
    - catagoraze
    [Parameters]
        없음
    [Returns]
        pandas DataFrame : 전종목 시세
    '''
    # DF를 생성, columns명 지정
    stock_df = pd.DataFrame(make_list(), columns=['종목','현재가','등락','등락율','등락율(%)','섹터'])
    # columns 순서를 변경
    stock_df = stock_df[['종목','섹터','현재가','등락','등락율','등락율(%)']]
    # 등락율과 등락가, 현재가의 data type을 실수화, 정수화 함.
    stock_df = stock_df.astype({'등락율':'float64','등락':'int32','현재가':'int32'})
    # 등락율을 조건에 맞게 catagoraze하는 조건을 선언
    condition = [stock_df['등락율']>=3,
                 (stock_df['등락율']>=2)&(stock_df['등락율']<3),
                 (stock_df['등락율']>=1)&(stock_df['등락율']<2),
                 (stock_df['등락율']>=0)&(stock_df['등락율']<1),
                 (stock_df['등락율']>=-1)&(stock_df['등락율']<0),
                 (stock_df['등락율']>=-2)&(stock_df['등락율']<-1),
                 stock_df['등락율']<-2]
    # 조건에 따라 변환될 catagory를 선언
    choices = ['+3','+2','+1','0','-1','-2','-3']
    # catagoraze한 등락율을 등락에 저장.
    stock_df['등락'] = np.select(condition,choices,default='0')
    # 전종목 시세의 DF 반환
    return stock_df

def make_mktcap_df():
    '''
    종목명과 티커, 시가총액을 크롤링하여 DF으로 저장하는 함수이다.
    [Parameters]
        없음
    [Returns]
        pandas DataFrame : 종목명, 티커, 시가총액
    '''
    # 임시 list 선언
    stock_mktcap_df = pd.read_csv("templates\pybo\datas\stock_mktcap_df.csv", encoding='cp949')
    return stock_mktcap_df

def make_data():
    '''
    위의 두 DF를 합쳐 최종 DF data를 생성하는 함수이다.
    [Parameters]
        없음
    [Returns]
        pandas DataFrame : 최종 전종목시세 정보
    '''
    # 두 DF를 inner 방식으로 합침.
    stock_df = pd.merge(make_df(),make_mktcap_df(),on='종목',how='inner')
    # 최종 data를 DF로 반환함.
    return stock_df

def get_html():
    '''
    DF를 이용하여 treemap 그래프를 html 파일로 변환하여 그 코드를 문자열로 저장하는 함수이다.
    [Parameters]
        없음
    [Returns]
        str : treemap 그래프를 나타내는 html 파일의 코드
    '''
    # 최종 data 생성
    data = make_data()
    # 트리맵 그래프 그림.
    fig = px.treemap(
    data, 
    # kopsi - 섹터- 종목 순으로 treemap 구현
    path=[px.Constant("KOSPI"),"섹터",'종목'],
    # treemap의 박스 크기를 지정하는 변수 - 시가총액
    values='시가총액',
    # treemap의 박스 색상을 지정하는 변수 - 등락
    color='등락',
    # 커서를 올리는 경우 표시할 정보 - 종목, 티커, 현재가, 등락율(%)
    hover_data =[data['종목'],data['티커'],data['현재가'],data['등락율(%)']],
    # 등락 정보에 따른 색상 지정
    color_discrete_map={'-3':'#2f89ff','-2':'#376eba','-1':'#3d5883',
                        '0':'#434653','+1':'#74404b','+2':'#b43a41','+3':'#f53538','(?)':'#60584c'},
    #
    labels={'labels':'종목','color':'등락'},    
    # 그래프의 높이 600으로 선언
    height = 600
    )
    # 그래프의 세부 정보 지정
    fig.update_traces(
        # 테두리 색상 회색
        root_color="#d9d9d9",
        # 표시될 문자 형식 - 이름(줄바꿈) 등락율(%)
        texttemplate='<a href="https://finance.naver.com/item/main.naver?code=%{customdata[1]}"><span style="color:#fff"> %{label}</span></a><br>%{customdata[3]}',
        # 표시될 문자의 위치 - 중단 중앙
        textposition="middle center",
        # 표시될 문자의 속성
        textfont={
            # 문자색상 노란색(선택옵션)
            #"color":'#ffbc00',
            # 문자색상 흰색(선택옵션)
            "color":'#fff',
            # 문자 크기 22
            "size": 22},
        # 호버링 문자열 형식 - 티커(줄바꿈) 현재가(줄바꿈) 등락율(%)
        hovertemplate='티커 = %{customdata[1]}<br>현재가 = %{customdata[2]}<br> 등락율(%) = %{customdata[3]}<extra></extra>'
    )
    # 그래프의 layout 지정 - 배경색상 회색
    fig.update_layout(go.Layout(paper_bgcolor='#d9d9d9'))
    # 생성된 그래프를 html로 변환하여 html_str에 저장(문자열)
    html_str = fig.to_html()
    # html의 불필요한 부분을 구현에 필요한 코드로 변환
    html_str = html_str.replace('<html>\n<head><meta charset="utf-8" /></head>\n<body>','{% extends "pybo/base.html" %}\n{% load static %}\n<body id="page-top">\n    {% block content %}\n    <div class="container px-4 px-lg-5 h-100" style="margin-top : 5.5rem; padding-top : 3.5rem">')
    html_str = html_str.replace('\n</body>\n</html>', '</div>\n    {% endblock content %}\n</body>')
    # html 코드를 문자열로 반환
    return html_str

def write_treemap_html():
    '''
    위에서 저장한 html 코드를 이용하여 treemap 그래프를 보여주는 페이지를 html 파일로 작성하는 함수이다.
    [Parameters]
        없음
    [Returns]
        html file : treemap 그래프를 보여주는 페이지의 html 파일
    '''
    # treemap 그래프를 보여주는 페이지의 코드를 변수에 저장
    html_str = get_html()
    # html 파일을 생성함.
    file = open('templates\pybo\KospiMarketMap.html','w',encoding='UTF-8')
    # html 파일을 작성함.
    file.write(html_str)
    # html 파일을 종료함.
    file.close()