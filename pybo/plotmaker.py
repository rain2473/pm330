import re
from plotly.offline import plot
from plotly.graph_objs import *
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data import postgres_handler as pg
from data import api_handler      as ap
from data import data_manipulator as dm
from data import conn_config as con

# import sys, os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# from optimize_pf import PF_optimizer as pf
pgdb = pg.PostgresHandler(user=con.ID_DR, password=con.PW_DR)

def myportfolio_plot_view(member_id):
    """ 
    View demonstrating how to display a graph object
    on a web page with Plotly. 
    """
    stock_list = pgdb.get_portfolio_by_member_id(member_id)
    my_stock=[]
    for stock in stock_list:
        now_price = pgdb.get_close_price(stock["isin_code"],start_date='20221018',end_date='20221018')[0]
        my_stock.append([pgdb.get_item_name_by_isin_code(stock["isin_code"]),int(now_price["close_price"]),stock["break_even_price"],stock["quantity"]])

    # Generating some data for plots.
    my_portfolio = pd.DataFrame(my_stock,
    columns=['종목','현재가','평단가','수량'])
    data = my_portfolio

    labels = data['종목']
    values = data['평단가']*data['수량']
    yields = ((round((data['현재가']/data['평단가']-1)*100,2)).astype('str')).str[0:]+'%'
    
    # List of graph objects for figure.
    # Each object will contain on series of data.
    graphs = [go.Pie(labels=labels, values=values, customdata = yields ,hole=.3, pull=0.03,
    # 표시될 문자 형식 - 이름(줄바꿈) 등락율(%)
    texttemplate='%{label}<br>%{percent}',
    # 표시될 문자의 위치 - 중단 중앙
    textposition="auto",
    # 표시될 문자의 속성
    textfont={
        # 문자색상 노란색(선택옵션)
        #"color":'#ffbc00',
        # 문자색상 흰색(선택옵션)
        "color":'#60584c',
        # 문자 크기 22
        "size": 18},
    marker={'colors': ['#ff0000','#ff6600','#ff6699','#cc99ff','#99ccff','#ff5050','#ff9966','#ff99ff','#ccccff','#e6f2ff']},
    # 호버링 문자열 형식 - 티커(줄바꿈) 현재가(줄바꿈) 등락율(%)
    hovertemplate='수익률 = %{customdata}<extra></extra>')]


    # Setting layout of the figure.
    layout = {
        'paper_bgcolor':"#d9d9d9",
        'height': 420,
        'width': 560,
    }

    # Getting HTML needed to render the plot.
    plot_div = plot({'data': graphs, 'layout': layout}, 
                    output_type='div')
    return plot_div

def lstm_plot_view():
    """ 
    View demonstrating how to display a graph object
    on a web page with Plotly. 
    """
    
    # Generating some data for plots.
    data = pd.read_csv("templates\pybo\datas\삼성전자_005930.csv", encoding="utf-8")

    # List of graph objects for figure.
    # Each object will contain on series of data.
    graphs = px.line(data,
                  x='날짜', y='종가', custom_data=['등락율'],
                  color_discrete_sequence=['#febf04'],
                 )
    graphs.update_traces(
        hovertemplate="<br>".join([
            "날짜: %{x}",
            "종가: %{y}",
            "등락율: %{customdata[0]}",
        ])
    )
    graphs.update_layout(
        paper_bgcolor = "#d9d9d9",
        plot_bgcolor = '#d9d9d9',
        height = 360,
        width = 480,
        margin={"r":15,"t":15,"l":15,"b":25},
    )

    # Getting HTML needed to render the plot.
    plot_div = plot( graphs, output_type='div')
    return plot_div

palette_red = {
    0:"https://postfiles.pstatic.net/MjAyMjEwMThfOTgg/MDAxNjY2MDkwMTg0NTQ0.CvF1HlK1wO4VHyQU7-k0EqtCn_UxImM4xNMVMXPHQJog.wpc5w698cscjZ2CDbEq2h_OKIXqByl48zsMe5E6uUhog.PNG.rain2473/0_%EB%B9%A8%EA%B0%95.png?type=w773",
    1:"https://postfiles.pstatic.net/MjAyMjEwMThfNzQg/MDAxNjY2MDkwMTg0NTY1.88DArKQLXrA600DfZgC7FsFecrPxzo3tEKRWrzzMd08g.vbLL3IUXrS2oleNsC5letx7WbHKfulP03ZdJoGFfHX4g.PNG.rain2473/1_%EB%B9%A8%EA%B0%95.png?type=w773",
    2:"https://postfiles.pstatic.net/MjAyMjEwMThfMTcy/MDAxNjY2MDkwMTg0NTQw.mzILVoqMDeZYCOuSYINHyoTZNEqrZvWyO3LW842odYIg.n0Ajuvnjao8nCAsAJ8sVm4GPFu-o3RQ7HpftMFaW-c8g.PNG.rain2473/2_%EB%B9%A8%EA%B0%95.png?type=w773",
    3:"https://postfiles.pstatic.net/MjAyMjEwMThfMjQg/MDAxNjY2MDkwMTg0NTQz.rTPuOjJjirgdbxvE-o2r_PJgwXpe5RlwhUwjfJh8qX8g.1yahyn7Vp2VEd9YBCvfLejk5qf20mdblhnjROnylhrsg.PNG.rain2473/3_%EB%B9%A8%EA%B0%95.png?type=w773",
    4:"https://postfiles.pstatic.net/MjAyMjEwMThfMTAg/MDAxNjY2MDkwMTg0NTU1.tQAKDtKPqZpYe6sIOPUkxZWBiu2YZ5ySJWPf4UzhV8Eg.7X2U5BIvmHph4iEyAJNJIWOiro19gau2t0OJJsGBwU8g.PNG.rain2473/4_%EB%B9%A8%EA%B0%95.png?type=w773",
    5:"https://postfiles.pstatic.net/MjAyMjEwMThfMjA1/MDAxNjY2MDkwMTg0NTYy.aCQIJCiGEC5eH3xIWfJfIQIFeCJPtb-r6h5FfNKYaJgg.RTl51UP_1JV_MVNRId7rirEhQ7FB0pMR9I91gzsNrncg.PNG.rain2473/5_%EB%B9%A8%EA%B0%95.png?type=w773",
    6:"https://postfiles.pstatic.net/MjAyMjEwMThfNTEg/MDAxNjY2MDkwMTg0Njcz.gID_UmBDnjX0nlv9yjiBT8wCcBzrH_EtvxgOM7rSLxkg.nJbiuKaaXHOvMx83TxiZ8_2NR7moFELORyIjJykqHY0g.PNG.rain2473/6_%EB%B9%A8%EA%B0%95.png?type=w773",
    7:"https://postfiles.pstatic.net/MjAyMjEwMThfNDcg/MDAxNjY2MDkwMTg0Njk3.lEhOF6xRLT_CNBvv0Y5uzLZxjZMvKUhBone77KyIoxQg.s2p-BLrCixWuvvZwLkkr0adurrEbo0xgdEMEYBK_w-kg.PNG.rain2473/7_%EB%B9%A8%EA%B0%95.png?type=w773",
    8:"https://postfiles.pstatic.net/MjAyMjEwMThfMTg1/MDAxNjY2MDkwMTg0Njk5.yMG99Rjkh4VZ2tMqEqA0uYS7JdtEOqcl5hNZmg2zFXUg.WGZt-ZexKfR8jKPzu7dtkoy91KE3pZhklC5Ufeqcsksg.PNG.rain2473/8_%EB%B9%A8%EA%B0%95.png?type=w773",
    9:"https://postfiles.pstatic.net/MjAyMjEwMThfODgg/MDAxNjY2MDkwMTg0Njk5.Aw3tkwrqne4NuOYS0ZJvD3T8aWuDX604KyjiP4ZUU30g.ZN-rciPn6-YEQ0Torpa-74E1ZiST0TT5GRmkGBoQpGUg.PNG.rain2473/9_%EB%B9%A8%EA%B0%95.png?type=w773"
}

palette_yellow = {
    0:"https://postfiles.pstatic.net/MjAyMjEwMThfMjI5/MDAxNjY2MDkwMTY5NDQx.9uu1pGucJgkdN49puEV1WxGV1rzouH8_LASlth9N1EUg.prTQ-cuL3kSnzBHT5aJS4zBnd86nshQxozwZG4Xjb9wg.PNG.rain2473/0_%EB%85%B8%EB%9E%91.png?type=w773",
    1:"https://postfiles.pstatic.net/MjAyMjEwMThfMjIw/MDAxNjY2MDkwMTY5NTA2.I0LUnYNHpoVzyO_VgUUSgohHYElRpN0U72Vn8e_dQ2gg.0Y78ieLy42yVjQsSx7PJ_aiSXi1QIzh-wNiH13knzhgg.PNG.rain2473/1_%EB%85%B8%EB%9E%91.png?type=w773",
    2:"https://postfiles.pstatic.net/MjAyMjEwMThfMTAg/MDAxNjY2MDkwMTY5NTA3.0xF1sVKyXsqiq3YXxUEfH23Dgj4e2q5Xnyintxnz_NYg.y7eMATu9yL8c-EgxpykamMAzHuxPWBOYXg7X94utKegg.PNG.rain2473/2_%EB%85%B8%EB%9E%91.png?type=w773",
    3:"https://postfiles.pstatic.net/MjAyMjEwMThfMTM4/MDAxNjY2MDkwMTY5NTcz.HzS7TzTXGGaKrIASG2NdJ0aiTl2YujoDcb8m1BmhFncg.nLzj_hZOq8Ma4NnVSOhNCwo0mU7SGxn7Caqr8qqAZ6Mg.PNG.rain2473/3_%EB%85%B8%EB%9E%91.png?type=w773",
    4:"https://postfiles.pstatic.net/MjAyMjEwMThfMjcx/MDAxNjY2MDkwMTY5NTc1.Nm156rUwtEyoHoV0R0ooRFMGZm3X7CE6LENQqkvd3vkg.R3CO5kGGDaLuaGPqKPeF385e8XA623H4B6KvRorhg1wg.PNG.rain2473/4_%EB%85%B8%EB%9E%91.png?type=w773",
    5:"https://postfiles.pstatic.net/MjAyMjEwMThfMjgz/MDAxNjY2MDkwMTY5NDQx.FPiTjrsoxaRpS6IaPNFLLVFPQEYZWBvZc8Ou7F52Ruog.jbSQRAApKuEBMBCYYZi5s3ESUkQSSnasfd74RPKjpnIg.PNG.rain2473/5_%EB%85%B8%EB%9E%91.png?type=w773",
    6:"https://postfiles.pstatic.net/MjAyMjEwMThfMjcw/MDAxNjY2MDkwMTY5NjY2.7VIkO3ZGilZ8eifS2wDQALNpCHzgeOG-fDNVIeB1xOwg.MC6Kg6XpfISU66Bj6uRjCog9Tvn7DKJipdaivv5ymJQg.PNG.rain2473/6_%EB%85%B8%EB%9E%91.png?type=w773",
    7:"https://postfiles.pstatic.net/MjAyMjEwMThfMTU0/MDAxNjY2MDkwMTY5NjY2.L4XYOSMtTGyL2UCvXcPgZqyh6zdUWYQwZiY2iCEhQGMg.R7w-q_pZ2YWguV4ovXHu3CnSx1TSj_hDDJRu5ST29DYg.PNG.rain2473/7_%EB%85%B8%EB%9E%91.png?type=w773",
    8:"https://postfiles.pstatic.net/MjAyMjEwMThfMTE4/MDAxNjY2MDkwMTY5NjY3.QwtOm8lG4EKBAYAOTA2hhW9luB-s2JUgXiAWNm_5kccg.WmTnK5AxtjL3I71zLojkjYakCHwuBnv2wGqnaS4KWaMg.PNG.rain2473/8_%EB%85%B8%EB%9E%91.png?type=w773",
    9:"https://postfiles.pstatic.net/MjAyMjEwMThfNDQg/MDAxNjY2MDkwMTY5NzM3.kc7AY_7g3o3ODSltwSWPXYitTylliop5uagJqLz_Gdog.zTOHUcuv7tVvQw4R8xDuYvUWcgFCF2tOlNqD6MuLbKkg.PNG.rain2473/9_%EB%85%B8%EB%9E%91.png?type=w773",
}

palette_green = {
    0:"https://postfiles.pstatic.net/MjAyMjEwMThfMjc1/MDAxNjY2MDkwMTc3MzEz.sOxOiZT1Kx1yEDQtvQWaiSlPq29UFOvK9qxGZqFa_xMg.SkyKmgHGCirUlbTjuyRmt7Rz72mXBIsDCUj8romIvv0g.PNG.rain2473/0_%EB%85%B9%EC%83%89.png?type=w773",
    1:"https://postfiles.pstatic.net/MjAyMjEwMThfMTc2/MDAxNjY2MDkwMTc3MzEy.KclDRxjdpChKBwz0PP5hF6c72oT11spY-xzfR90M_H4g.q0t7V5Ry5oJsAPUAfyzVhUGXHN6xdoYQH5LvSoHLbVYg.PNG.rain2473/1_%EB%85%B9%EC%83%89.png?type=w773",
    2:"https://postfiles.pstatic.net/MjAyMjEwMThfMjEw/MDAxNjY2MDkwMTc3MzEz.3clUtRJuPyudkY5OXL-yMjC1xcXaMvir5U2OY-JPPMQg.B3fuPXH_2Z9fUDCu3oIJsl1UXMLT_1EeV4Ftd7dHcOAg.PNG.rain2473/2_%EB%85%B9%EC%83%89.png?type=w773",
    3:"https://postfiles.pstatic.net/MjAyMjEwMThfODEg/MDAxNjY2MDkwMTc3MzEz.s2f9xg6PCUX8KF4JZmpOwZrnKdgfPhnrzhkuiawNKicg.ic4k4OQIpyXZbQCXTIAUvilM3OhpiUrrKxqi7C9zfVcg.PNG.rain2473/3_%EB%85%B9%EC%83%89.png?type=w773",
    4:"https://postfiles.pstatic.net/MjAyMjEwMThfMTgy/MDAxNjY2MDkwMTc3MzIy.BMRQ2ZUm49q33EBiIqr2Zc08pNLFzbXKUJmKQUHhyNsg.C6XcE4g_ZzhxLyua4HATMbeFWz9bEYatCPgSMdcSeQog.PNG.rain2473/4_%EB%85%B9%EC%83%89.png?type=w773",
    5:"https://postfiles.pstatic.net/MjAyMjEwMThfMjY4/MDAxNjY2MDkwMTc3MzI0.WeT6kmy0nV3vZgp9UZhTsDrAg6yr5SzqEw2Tlra1Q4Ug.wzo7UKbWHoWj1cUbooOFBAHr2iYvkC52laUe9TkDx68g.PNG.rain2473/5_%EB%85%B9%EC%83%89.png?type=w773",
    6:"https://postfiles.pstatic.net/MjAyMjEwMThfODgg/MDAxNjY2MDkwMTc3NDU0.0A1y58nmpD4iPCID1ppV4JMpQ_B5LTs_cVxDI1cRi_0g.UeORHrB1053gxjuENWEmXRa55dqREi-Z7p4n2WeeHGAg.PNG.rain2473/6_%EB%85%B9%EC%83%89.png?type=w773",
    7:"https://postfiles.pstatic.net/MjAyMjEwMThfODMg/MDAxNjY2MDkwMTc3NDQ4.xuYX8rbl5joYQmGcUf7UetKD19D1MdlijXhV1jkJQaog.kYE0Rnm_eN_lLNRC0lTf2x41Ahk6koP9j6gDI5sZxG8g.PNG.rain2473/7_%EB%85%B9%EC%83%89.png?type=w773",
    8:"https://postfiles.pstatic.net/MjAyMjEwMThfMTc4/MDAxNjY2MDkwMTc3NDcx.4aucmvfjy6mDnaA_iuV5BjVVvrY2B66ZAGVge85VIR8g.004TrVL1R51OqRirMuOGTk_h5TKjIrkV9M-_wfpixNkg.PNG.rain2473/8_%EB%85%B9%EC%83%89.png?type=w773",
    9:"https://postfiles.pstatic.net/MjAyMjEwMThfMzAw/MDAxNjY2MDkwMTc3NDc1.gHugex8JQJ0MpUShPejCClU29FdU3D42HDsLszPP38Yg.2ye2Gzf-xynMHXPtCRH0Xb2FiUkZ_ZJ-u746hzINuLAg.PNG.rain2473/9_%EB%85%B9%EC%83%89.png?type=w773",
}

palette_blue = {
    0:"https://postfiles.pstatic.net/MjAyMjEwMThfMjgg/MDAxNjY2MDkwMTkyNTYw.TKwgET0CFiLirAsT9X3l1dS_r6r3F2ALo4425jAuq38g.j2t4ZVM_HGkq8N8OfD5xk-2yn-zWYZOu_PjuHpDoKZQg.PNG.rain2473/0_%ED%8C%8C%EB%9E%91.png?type=w773",
    1:"https://postfiles.pstatic.net/MjAyMjEwMThfNzMg/MDAxNjY2MDkwMTkyNTU5.d-QmlLP06-RR47rrkLrAeTdxUGaxO_ZI_3NjKso2s1sg.8vJuC-FMC26izlqBp0dV4Jn3zy1Ux2M-aHugVjoKFs4g.PNG.rain2473/1_%ED%8C%8C%EB%9E%91.png?type=w773",
    2:"https://postfiles.pstatic.net/MjAyMjEwMThfMjcw/MDAxNjY2MDkwMTkyNTYw.oACEXC6kwO6Aqww1Cg1nEzyoxSZKT7o3xZM4u1pKCxIg.9hEUyZlSF43bz7s5LTMqUNPzuxWw1d90N-6Uf2gx8i8g.PNG.rain2473/2_%ED%8C%8C%EB%9E%91.png?type=w773",
    3:"https://postfiles.pstatic.net/MjAyMjEwMThfMTYx/MDAxNjY2MDkwMTkyNTU2.xATZ62pLCwaS-wVdJhu35eSzZS7Lu1E72t_008TCqZkg.QWKVea90Nw_osbTx0inOi8fy4UgefnzSV6qHRrKoWhQg.PNG.rain2473/3_%ED%8C%8C%EB%9E%91.png?type=w773",
    4:"https://postfiles.pstatic.net/MjAyMjEwMThfMjgy/MDAxNjY2MDkwMTkyNTYw.ke6THPwD60vAv5972qXwiXhBr-3hxYVxmUHrhZswbyIg.9tsWPuTdIc7YTYJ9NUt9wv1WAoppBuFnH0NF5JFbR48g.PNG.rain2473/4_%ED%8C%8C%EB%9E%91.png?type=w773",
    5:"https://postfiles.pstatic.net/MjAyMjEwMThfMTc3/MDAxNjY2MDkwMTkyNTYw.6GGHrpYuy-C1QWmYYCRsuyNAPXInQQHyrH-0ORWcxoMg.86VYeG70lhH2nlflTA5Y3b3No5JVgFepWfIZZ0WOd08g.PNG.rain2473/5_%ED%8C%8C%EB%9E%91.png?type=w773",
    6:"https://postfiles.pstatic.net/MjAyMjEwMThfMTI2/MDAxNjY2MDkwMTkyNzAx.vgwiAHRIB_7iDVt2U9jaPb6THSqQPlLt9j6fnZgLT_Ug.ALK6AcHMjZAOpYYTB0Quq3SIGw0D0BhcMQoyTr_s120g.PNG.rain2473/6_%ED%8C%8C%EB%9E%91.png?type=w773",
    7:"https://postfiles.pstatic.net/MjAyMjEwMThfMTYw/MDAxNjY2MDkwMTkyNzIy.a2bstTlN2gr1y_oNi5UD2-C7O0L1KY88h_OCVyhWFYgg.F7ixLro9YFEwmI7EHPYjSprdYo6XB6GiQOAkX6vn3-Ug.PNG.rain2473/7_%ED%8C%8C%EB%9E%91.png?type=w773",
    8:"https://postfiles.pstatic.net/MjAyMjEwMThfMjQ2/MDAxNjY2MDkwMTkyNzAw.qbIQdmWK8EyUUyW5xVXE_LWIvQsJonWl-dIjp2E2s30g.D5hpkBnjPcZcm9fKz5hKwZiFB9iqf49nCE79W9qQ6Ksg.PNG.rain2473/8_%ED%8C%8C%EB%9E%91.png?type=w773",
    9:"https://postfiles.pstatic.net/MjAyMjEwMThfMTgx/MDAxNjY2MDkwMTkyNzI1.T51ueSrTXNrFOnLLPy1D9L1Ol6pAb06PcYWwCUeQmkQg.J4pi2-anWlg5URz9-4ocZkXJkEJ8UogyiW8Gz2eiovcg.PNG.rain2473/9_%ED%8C%8C%EB%9E%91.png?type=w773",
}

def score_display(score):
    if score <30:
        palette = palette_red
    elif score <60:
        palette = palette_yellow
    elif score <90:
        palette = palette_green
    else:
        palette = palette_blue
    tens = palette[(score//10)]
    ones = palette[(score%10)]
    return tens, ones



def mapbox_maker():

    yesterday = dm.YESTERDAY
    just_yesterday = str(int(yesterday)-1)

    dataframe = pd.DataFrame([])
    ticker = ["^KS11","^N225","399001.SZ","000001.SS","^HSI","^TWII","^GSPC","^DJI","^IXIC","^VIX","\^FTSE","^STOXX50E","^N100","^GDAXI","^FCHI"]
    for i in range(len(ticker)):
        tmp = ap.get_world_index(ticker[i],just_yesterday,yesterday)
        tmp = pd.DataFrame(tmp)
        dataframe = pd.concat([dataframe,tmp])
    dataframe = dataframe[dataframe['base_date'] == yesterday][['ticker',"base_date","close_price","fluctuation_rate"]]
    dataframe['fluctuation_rate'] = dataframe['fluctuation_rate']*100
    dataframe['fluctuation_rate'] = dataframe['fluctuation_rate'].round(decimals = 2)
    dataframe['close_price'] = round(dataframe['close_price']).astype("int")
    trace1 = {
    "ticker" : ["^KS11","^N225","399001.SZ","000001.SS","^HSI","^TWII","^GSPC","^DJI","^IXIC","^VIX","\^FTSE","^STOXX50E","^N100","^GDAXI","^FCHI"],
    "nation" : ["한국","일본","중국","중국","대만","대만","미국","미국","미국","미국","영국", "유럽", "프랑스","독일","프랑스"],
    "index_name": ["KOSPI", "NIKKEI",  "SHENZHEN", "SSE", "HANGSENG", "TSEC", "S&P500", "DOW JONES", "NASDAQ", "VIX", "FTSE 100", "ESTX 50", "EURONEXT", "DAX", "CAC 40"], 
    "lat":        [ 37.5,    36.4,      37.1,       29.1,  22.1,       24.4,   39.1,     38.7,        42.5,     44.1,  53.5,       41.9,      46.4,       52.8,  48.2], 
    "lon":        [ 128.00,  138.24,    116.5,      119.2, 120.1,      121.3,  -118.3,   -94.2,       -74.1,    -84.2, -2.1,       13.5,      2.1,        10.45, 4.3], 
    "color": ["#810023", "#0D0863", "#1C7600", "#3BF400", "#E9C200", "#E2E200", "#B9005E", "#630497", "#920092", "#B88CDB", "#431F01","#ff0000","#B3A000", "#006B6B","#B4D900"],
    "textposition": ["top center", "bottom center", "middle left", "middle left", "bottom center", "middle right", "top right", "bottom center", "bottom right", 
                    "top center","top left", "bottom center", "middle left", "middle right", "middle right"]
    }
    data = pd.DataFrame(trace1)
    data = pd.merge(data,dataframe)
    data['text'] = data['index_name']+" : "+data["fluctuation_rate"].astype("str")+"%"
    data["customdata"] = data['nation']+'<br>' + data['index_name']+ " : "+data["close_price"].astype("str")
    color = list(data[['color']].to_dict('list').values())[0]
    customdata = []
    for element in data['customdata']:
        tmp = []
        tmp.append(element)
        customdata.append(tmp)
    data = data.drop(['color','ticker','nation','index_name','base_date','fluctuation_rate','close_price'],axis=1)
    data_dict = data.to_dict("list")
    trace1 = {
    "mode": "markers+text", 
    "type": "scattergeo", 
    "marker": {
        "line": {"width": 1}, 
        "size": 10, 
        "color": color
    }, 
    "textfont": {
        "size": 18, 
        "color": color, 
        "family": "Arial, sans-serif"
    },
    "customdata" : customdata,
    "hovertemplate": "%{customdata[0]}",
    }
    data_dict.update(trace1)
    data = Data([data_dict])

    layout = {
        "geo": {
            "lataxis": {"range": [-10, 70]}, 
            "lonaxis": {"range": [-125, 155]}
        },
        "margin": {"r":0,"t":0,"l":0,"b":0},
        "title": "Canadian cities", 
        "titlefont": {"size": 28}, 
        "showlegend": False,
        "width" : 1200,
        "height" : 400,
        "paper_bgcolor":"#d9d9d9",
    }

    fig = Figure(data=data, layout=layout)
    plot_div = fig.to_html()
    plot_div = plot_div.replace('<html>\n<head><meta charset="utf-8" /></head>\n<body>\n    <div>                        <script type="text/javascript">window.PlotlyConfig = {MathJaxConfig: \'local\'};</script>\n        <script',"<script")
    plot_div = plot_div.replace("</script>        </div>\n</body>\n</html>","</script>")
    file = open('pm330\templates\pybo\mapbox.html','w',encoding='UTF-8')
    file.write(plot_div)
    file.close()

def rollingbanner_maker():

    yesterday = dm.YESTERDAY
    just_yesterday = str(int(yesterday)-1)

    dataframe = pd.DataFrame([])
    ticker = ["^KS11","^N225","000001.SS","^HSI","^GSPC","^DJI","^IXIC","\^FTSE","^N100","^GDAXI","^FCHI"]
    for i in range(len(ticker)):
        tmp = ap.get_world_index(ticker[i],just_yesterday,yesterday)
        tmp = pd.DataFrame(tmp)
        dataframe = pd.concat([dataframe,tmp])
    dataframe = dataframe[dataframe['base_date'] == yesterday][['ticker',"base_date","close_price","fluctuation_rate"]]
    dataframe['fluctuation_rate'] = dataframe['fluctuation_rate']*100
    dataframe['fluctuation_rate'] = dataframe['fluctuation_rate'].round(decimals = 2)
    dataframe['close_price'] = round(dataframe['close_price']).astype("int")
    trace1 = {
    "ticker" : ["^KS11","^N225","399001.SZ","000001.SS","^HSI","^TWII","^GSPC","^DJI","^IXIC","^VIX","\^FTSE","^STOXX50E","^N100","^GDAXI","^FCHI"],
    "index_name": ["KOSPI", "NIKKEI",  "SHENZHEN", "SSE", "HANGSENG", "TSEC", "S&P500", "DOW JONES", "NASDAQ", "VIX","FTSE 100", "ESTX 50", "EURONEXT", "DAX", "CAC 40"], 
    }
    data = pd.DataFrame(trace1)
    data = pd.merge(data,dataframe)
    data.drop(['ticker','base_date'],axis=1,inplace=True)
    data = data.to_dict('list')
    result = []
    for i in range(len(data['index_name'])):
        tmp = {'index_name':data['index_name'][i],'close_price':data['close_price'][i],'fluctuation_rate':data['fluctuation_rate'][i]}
        result.append(tmp)
        
    html_code=""
    for element in result:
        if element["fluctuation_rate"] > 0:
            code = f'<li class="{element["index_name"]}"><a href="#"><strong class="name" style="color : #60584C;">{element["index_name"]}</strong><span class="status up"><span class="num" style="color : #60584C;">{element["close_price"]}</span><span class="rate" style="color : #60584C;"><em>{element["fluctuation_rate"]}%</em></span></span></a></li>'
        elif element["fluctuation_rate"] < 0:
            code = f'<li class="{element["index_name"]}"><a href="#"><strong class="name" style="color : #60584C;">{element["index_name"]}</strong><span class="status down"><span class="num" style="color : #60584C;">{element["close_price"]}</span><span class="rate" style="color : #60584C;"><em>{element["fluctuation_rate"]}%</em></span></span></a></li>'
        else:
            code = f'<li class="{element["index_name"]}"><a href="#"><strong class="name" style="color : #60584C;">{element["index_name"]}</strong><span class="num" style="color : #60584C;">{element["close_price"]}</span><span class="rate" style="color : #60584C;"><em>{element["fluctuation_rate"]}%</em></span></span></a></li>'
        html_code += code
    file = open('pm330\templates\pybo\rolling_bar.html','w',encoding='UTF-8')
    file.write(html_code)
    file.close()