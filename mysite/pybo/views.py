# from django.shortcuts import render
# from django.http import HttpResponse

# # Create your views here.

# def index(request):
#     return HttpResponse("안녕하세요 pybo에 오신것을 환영합니다.")


from django.shortcuts import render


def index(request):
    return render(request, 'pybo/index.html')

def Mypage(request): 
    return render(request, 'pybo/Mypage.html') 

def Login(request): 
    return render(request, 'common/login.html')

def PortfolioFeedback(request): 
    return render(request, 'pybo/PortfolioFeedback.html')

def KospiMarketMap(request): 
    return render(request, 'pybo/KospiMarketMap.html')