# from django.shortcuts import render
# from django.http import HttpResponse

# # Create your views here.

# def index(request):
#     return HttpResponse("안녕하세요 pybo에 오신것을 환영합니다.")


import imp
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from .TreemapMaker import write_treemap_html as create
from .plotmaker import myportfolio_plot_view as fig
from .plotmaker import lstm_plot_view as lstm
from .models import Stocks
from .forms import StocksForm
from django.shortcuts import render, redirect, get_object_or_404

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
    # return render(request, 'pybo/Mypage.html')

@login_required(login_url='common:login')
def Myportfolio(request): 
    plot_div = fig()
    return render(request, 'pybo/Myportfolio.html', 
                  context={'plot_div': plot_div})

@login_required(login_url='common:login')
def PortfolioFeedback(request): 
    lstm_div = lstm()
    return render(request, 'pybo/PortfolioFeedback.html',
                    context={'lstm_div': lstm_div})

@login_required(login_url='common:login')
def News(request): 
    return render(request, 'pybo/News.html')

def Login(request): 
    return render(request, 'common/login.html')

def KospiMarketMap(request): 
    create()
    return render(request, 'pybo/KospiMarketMap.html')

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

def stock_modify(request):
    return render(request, 'pybo/Mypage.html')
    

def stock_delete(request):
    q = get_object_or_404(Stocks)
    q.delete()
    return render(request, 'pybo/Mypage.html')