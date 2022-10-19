from django import forms
from .models import Stocks

class StocksForm(forms.ModelForm):
    class Meta:
        model = Stocks    # 사용할 모델
        fields = ['subject', 'now_price', 'avg_price', 'quantity', 'create_date']     # QuestionForm에서 사용할 Question 모델의 속성

        # widgets = {
        #     'subject': forms.TextInput(attrs={'class': 'form-control'}),
        #     'content': forms.Textarea(attrs={'class': 'form-control', 'rows': 10}),
        # }
        
        labels = {
            'subject': '종목', 
            'now_price': '현재가', 
            'avg_price': '평단가', 
            'quantity': '수량', 
            'create_date': '매수날짜',
        }