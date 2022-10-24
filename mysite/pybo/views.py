# from django.shortcuts import render
# from django.http import HttpResponse

# # Create your views here.

# def index(request):
#     return HttpResponse("안녕하세요 pybo에 오신것을 환영합니다.")


from django.http import HttpResponse
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from. import TreemapMaker
from . import plotmaker
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

def index(request):
    return render(request, 'pybo/index.html')

@login_required(login_url='common:login')
def Mypage(request):
    author = request.user
    if author.is_authenticated:
        try:
            stock_list = Stocks.objects.filter(author=author).order_by('-quantity')
            context = {'stock_list': stock_list}
            return render(request, 'pybo/Mypage.html', context)
        except Stocks.DoesNotExist:
            return render(request, 'pybo/Mypage.html')

@login_required(login_url='common:login')
def Myportfolio(request): 
    plot_div = plotmaker.myportfolio_plot_view()
    tens, ones = plotmaker.score_display(29)
    return render(request, 'pybo/Myportfolio.html', 
                    context={'plot_div': plot_div, "tens" : tens, "ones" : ones})

@login_required(login_url='common:login')
def PortfolioFeedback(request): 
    lstm_div = plotmaker.lstm_plot_view()
    return render(request, 'pybo/PortfolioFeedback.html',
                    context={'lstm_div': lstm_div})

@login_required(login_url='common:login')
def FeedbackReady(request): 
    return render(request, 'pybo/FeedbackReady.html')

@login_required(login_url='common:login')
def News(request): 
    isin_code = 'KR7005930003'
    stock_name, result_dict = front_newsdata.show_news(isin_code)
    return render(request, 'pybo/News.html', context={"stock_name":stock_name,"result_dict":result_dict})

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
    if request.method == 'POST':
        form = StocksForm(request.POST)
        if form.is_valid():      #form이 유효한지 검사.
            stock = form.save(commit=False)
            stock.author = request.user
            # stock.create_date = timezone.now()
            stock.save()
            return redirect('pybo:Mypage') # 상단에 정의되어 있는 def Mypage를 호출하는 것으로 보임
    else:
        form = StocksForm()
    context = {'form': form}
    # return render(request, 'pybo/Mypage.html', context)
    return render(request, 'pybo/stock_create.html', context)

def stock_modify(request, stock_id):
    stocks = get_object_or_404(Stocks, pk=stock_id)
    if request.method == 'POST':
        form = StocksForm(request.POST, instance=stocks)
        if form.is_valid():      #form이 유효한지 검사.
            stock = form.save(commit=False)
            # 주식 소유자 = POST요청 보낸 유저
            # stocks.author = request.user
            # stocks.subject = request.subject
            # stocks.now_price = request.now_price
            # stocks.avg_price = request.avg_price
            # stocks.quantity = request.quantity
            # stocks.create_date = request.create_date
            # stock.create_date = timezone.now()
            stock.save()
            return redirect('pybo:Mypage') # 상단에 정의되어 있는 def Mypage를 호출하는 것으로 보임
    else:
        form = StocksForm(instance=stocks)
    context = {'form': form}
    # return render(request, 'pybo/stock_modify.html', context)
    return render(request, 'pybo/Mypage_modify.html', context)

def stock_delete(request, stock_id):
    stocks = get_object_or_404(Stocks, pk=stock_id)
    stocks.delete()
    return render(request, 'pybo/Mypage_deleted.html')

