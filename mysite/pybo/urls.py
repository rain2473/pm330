from django.urls import path
from . views import *

app_name = 'pybo'
urlpatterns = [
    path('', index, name='index'),
    path('Mypage/', Mypage, name='Mypage'),
    path('KospiMarketMap/', KospiMarketMap, name='KospiMarketMap'),
    path('KospiReady/', KospiReady, name='KospiReady'),
    path('KospiMarketMap/KospiMarketMap_new', KospiMarketMap, name='KospiMarketMap_new'),
    path('FeedbackReady/', FeedbackReady, name='FeedbackReady'),
    path('PortfolioFeedback/', PortfolioFeedback, name='PortfolioFeedback'),
    path('Myportfolio/', Myportfolio, name='Myportfolio'),
    path('portfolioReady/', PortfolioReady, name='portfolioReady'),
    path('News/<str:short_isin>/', News, name='News'),
    path('Mypage/Stock/create', stock_create, name='stock_create'),
    path('Mypage/Stock/modify/<str:stock_short_code>/', stock_modify, name='stock_modify'),
    path('Mypage/Stock/delete/<str:stock_short_code>/', stock_delete, name='stock_delete'),

]