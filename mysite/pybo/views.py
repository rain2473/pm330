# from django.shortcuts import render
# from django.http import HttpResponse

# # Create your views here.

# def index(request):
#     return HttpResponse("안녕하세요 pybo에 오신것을 환영합니다.")


from typing import Type
from django.http import HttpResponse
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from . import TreemapMaker
from . import plotmaker
from . import pf_recommend
from .models import Stocks
from .forms import StocksForm
from django.shortcuts import render, redirect, get_object_or_404
from . import front_newsdata
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data import postgres_handler as pg
from data import api_handler      as api
from data import data_manipulator as dm
from data import conn_config as con


pgdb = pg.PostgresHandler(user=con.ID_IJ, password=con.PW_IJ)

def index(request):
    return render(request, 'pybo/index.html')

@login_required(login_url='common:login')
def Mypage(request):
    author = request.user
    pgdb = pg.PostgresHandler(user=con.ID_DR, password=con.PW_DR)
    if author.is_authenticated:
        try:
            stock_list = pgdb.get_portfolio_by_member_id(member_id=request.user)
            data = []
            for stock in stock_list:
                stock_isin_code = stock['isin_code']
                stock_subject = pgdb.get_item_name_by_isin_code(stock_isin_code)
                stock_avg = stock['break_even_price']
                stock_quantity = stock['quantity']
                stock_short_code = pgdb.get_short_isin_code(stock_isin_code)
                print(stock_subject, stock_avg, stock_quantity, stock_short_code)
                data.append({'subject' : stock_subject, 'avg_price': stock_avg, 'quantity': stock_quantity, "stock_short_code":stock_short_code}) 

            context = {'stock_list': data}
            print(context, type(context))
            return render(request, 'pybo/Mypage.html', context)
        except TypeError:
            return render(request, 'pybo/Mypage.html')

@login_required(login_url='common:login')
def Myportfolio(request):
    member_id = request.user
    plot_div = plotmaker.myportfolio_plot_view(member_id)
    score = pf_recommend.return_score_rec(member_id)
    tens, ones = plotmaker.score_display(score)
    result_dict = front_newsdata.total_setiment(member_id)
    return render(request, 'pybo/Myportfolio.html', 
                    context={'plot_div': plot_div, "tens" : tens, "ones" : ones, "result_dict":result_dict})

@login_required(login_url='common:login')
def PortfolioReady(request):
    return render(request, 'pybo/portfolioReady.html')

@login_required(login_url='common:login')
def PortfolioFeedback(request): 
    member_id = request.user
    plot_div = pf_recommend.create_plot(member_id)
    recommend_list, momentum, stock_allocation = pf_recommend.return_momentum_list(member_id)
    result_dict = front_newsdata.total_recommend(recommend_list)
    return render(request, 'pybo/PortfolioFeedback.html', context={"result_dict":result_dict, "momentum":momentum, 'plot_div': plot_div, "stock_allocation":stock_allocation})

@login_required(login_url='common:login')
def FeedbackReady(request):
    member_id = request.user
    plot_div = pf_recommend.create_plot(member_id)
    recommend_list, momentum, stock_allocation = pf_recommend.return_momentum_list(member_id)
    result_dict = front_newsdata.total_recommend(recommend_list)
    return render(request, 'pybo/FeedbackReady.html', context={"result_dict":result_dict, "momentum":momentum, 'plot_div': plot_div, "stock_allocation":stock_allocation})

@login_required(login_url='common:login')
def News(request, short_isin):
    isin_code = pgdb.get_isin_code(short_isin)
    stock_name, result_dict = front_newsdata.show_news(isin_code)
    return render(request, 'pybo/News.html', context={"stock_name":stock_name, "result_dict":result_dict})

def Login(request): 
    return render(request, 'common/login.html')

def KospiReady(request): 
    return render(request, 'pybo/KospiReady.html')

def KospiMarketMap(request): 
    fig_div, created_time = TreemapMaker.get_fig()
    return render(request, 'pybo/KospiMarketMap.html',context={'fig_div': fig_div, "created_time" : created_time})

def KospiMarketMap_new(request): 
    fig_div = TreemapMaker.get_fig()
    return HttpResponse(context={'fig_div': fig_div})

def stock_create(request):
    # DB 연동
    pgdb = pg.PostgresHandler(user=con.ID_DR, password=con.PW_DR)
    
    if request.method == 'POST':
        form = StocksForm(request.POST)
        if form.is_valid():      #form이 유효한지 검사.
            stock = form.save(commit=False)
            stock.author = request.user
            
            # DB에 종목 추가
            subject = form.cleaned_data.get('subject')
            avg_price = form.cleaned_data.get('avg_price')
            quantity = form.cleaned_data.get('quantity')
            isin_code = pgdb.get_isin_code_by_item_name(subject)

            pgdb.add_transaction(member_id=str(request.user),isin_code=isin_code[0]['isin_code'],break_even_price=avg_price,quantity=quantity)      
         
            return redirect('pybo:Mypage') # 상단에 정의되어 있는 def Mypage를 호출하는 것으로 보임
    else:
        form = StocksForm()
    context = {'form': form}
    # return render(request, 'pybo/Mypage.html', context)
    return render(request, 'pybo/stock_create.html', context)

def stock_modify(request, stock_short_code):
    # DB 연동
    pgdb = pg.PostgresHandler(user=con.ID_DR, password=con.PW_DR)
    
    if request.method == 'POST':
        form = StocksForm(request.POST)
        if form.is_valid():      #form이 유효한지 검사.
            stock = form.save(commit=False)
            stock.author = request.user
            
            # DB에 종목 수정
            subject = form.cleaned_data.get('subject')
            avg_price = form.cleaned_data.get('avg_price')
            quantity = form.cleaned_data.get('quantity')
            isin_code = pgdb.get_isin_code_by_item_name(subject)
            pgdb.update_transaction(member_id=str(request.user),isin_code=isin_code[0]['isin_code'],break_even_price=avg_price,quantity=quantity)      
  
            return redirect('pybo:Mypage') # 상단에 정의되어 있는 def Mypage를 호출하는 것으로 보임
    else:
        form = StocksForm()
    context = {'form': form}
    # return render(request, 'pybo/stock_modify.html', context)
    return render(request, 'pybo/Mypage_modify.html', context)

def stock_delete(request, stock_short_code):
    # DB 연동
    pgdb = pg.PostgresHandler(user=con.ID_DR, password=con.PW_DR)
    
    # DB 에서 종목 삭제
    member_id = request.user
    isin_code = pgdb.get_isin_code(stock_short_code)

    pgdb.remove_transaction(member_id=str(member_id),isin_code=isin_code)
    return render(request, 'pybo/Mypage_deleted.html')