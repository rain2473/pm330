# Author  : 조익준           
# Contact : harry960629@naver.com  
# Date    : 2022-10-19(수)

# Required Modules
import csv
import requests
import pandas as pd

from bs4                   import BeautifulSoup
from pathlib               import Path
from tokenization_kbalbert import KbAlbertCharTokenizer
from transformers          import AlbertForSequenceClassification
from transformers          import pipeline

# 토크나이저 설정
tokenizer = KbAlbertCharTokenizer.from_pretrained('news_albert')
# 모델 설정
model = AlbertForSequenceClassification.from_pretrained('news_albert')
# 뉴스 분류 파이프라인 설정
news_classifier = pipeline(
  'sentiment-analysis',
  model=model.cpu(),
  tokenizer=tokenizer,
  framework='pt'
)

# Constants
# 뉴스 기사 헤드라인 입력 (데이터베이스에서 가져오는 걸로 대체)
NEWS_HEAD = input()
# 호재, 악재 확률 초기 설정
SCORE = 0

def get_score():
  """
  뉴스 헤드라인(전역 변수)를 받아 모델을 거쳐 호재, 악재 확률을 반환한다.
  [Parameters]
  NEWS_HEAD : 뉴스 헤드라인(전역 변수)

  [Returns]
  SCORE : 호재, 악재 확률
  """
  global NEWS_HEAD, SCORE # 전역 변수 설정

  # 모델 결과 저장 후 호재(+), 악재(-)에 따라 확률 반환
  results = news_classifier(NEWS_HEAD)

  for r in results:
    if r['label'] == 'LABEL_1':
      SCORE = r['score']
    else:
      SCORE = -r['score']

  return SCORE