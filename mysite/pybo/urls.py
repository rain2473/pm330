from django.urls import path
from . views import *

app_name = 'pybo'
urlpatterns = [
    path('', index, name='index'),
    path('Mypage/', Mypage, name='Mypage'),
    path('KospiMarketMap', KospiMarketMap, name='KospiMarketMap'),
    path('PortfolioFeedback', PortfolioFeedback, name='PortfolioFeedback'),
]