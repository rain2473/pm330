from django.shortcuts import render
from django.contrib.auth import authenticate, login
from django.shortcuts import render, redirect
from common.forms import UserForm
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data import postgres_handler as pg
from data import api_handler      as api
from data import data_manipulator as dm
from data import conn_config as con
import pandas as pd

def signup(request):
    pgdb = pg.PostgresHandler(user=con.ID_YK, password=con.PW_YK)

    if request.method == "POST":
        form = UserForm(request.POST)
        if form.is_valid():
            form.save()
            username = form.cleaned_data.get('username')
            raw_password = form.cleaned_data.get('password1')
            email = form.cleaned_data.get('email')
            pgdb.set_new_member(member_id=username, member_pw=raw_password, member_email=email)
            print(pgdb.get_all_data(table='member_info'))
            user = authenticate(username=username, password=raw_password)  # 사용자 인증
            login(request, user)  # 로그인
            return redirect('index')
            
    else:
        form = UserForm()

    return render(request, 'common/signup.html', {'form': form})