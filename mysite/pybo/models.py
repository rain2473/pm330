from django.db import models
from django.contrib.auth.models import User

# Create your models here.

# subject = 종목 이름, now_price = 현재가, avg_price = 평단가, quantity = 수량, create_date = 매수일자
class Stocks(models.Model):
    author = models.ForeignKey(User, on_delete=models.CASCADE)

    subject = models.CharField(max_length=20)
    avg_price = models.FloatField()
    quantity = models.IntegerField()

    def __str__(self):
        return self.subject