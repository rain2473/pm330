from plotly.offline import plot
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

def myportfolio_plot_view():
    """ 
    View demonstrating how to display a graph object
    on a web page with Plotly. 
    """
    
    # Generating some data for plots.
    my_portfolio = pd.DataFrame([
    ['삼성전자',56500,50000,30,'2021-11-30'],
    ['LG에너지솔루션',48900,500000,5,'2022-04-20'],
    ['SK하이닉스',95800,87000,20,'2022-04-01'],
    ['LG화학',611000,598000,7,'2022-06-18'],
    ['현대차',168000,100000,15,'2022-02-15'],
    ['카카오',49400,12000,20,'2022-08-20']
    ],
    columns=['종목','현재가','평단가','수량','매수일자'])
    my_portfolio['매수일자'] = pd.to_datetime(my_portfolio['매수일자'])
    data = my_portfolio

    labels = data['종목']
    values = data['평단가']*data['수량']
    yields = ((round((data['현재가']/data['평단가']-1)*100,2)).astype('str')).str[0:]+'%'
    
    # List of graph objects for figure.
    # Each object will contain on series of data.
    graphs = [go.Pie(labels=labels, values=values, customdata = yields ,hole=.3,
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
        height = 420,
        width = 560,
        margin={"r":15,"t":15,"l":15,"b":25},
    )

    # Getting HTML needed to render the plot.
    plot_div = plot( graphs, output_type='div')
    return plot_div