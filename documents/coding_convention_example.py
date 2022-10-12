# Author  : <이름>           ex) 홍길동
# Contact : <이메일>         ex) exam@gmail.com
# Date    : <최종수정일자>    ex) 2022-10-12(수)



# Required Modules  (from, import, as 구문들의 Indentation을 맞춘다.)
import abc
import defg

from chicken import egg
from dog     import puppy

from people  import mad_guy       as mg
from car     import mercedes_benz as mb



# Constants   (전역적으로 사용할 상수들의 이름은 모두 대문자로 구성한다.)
TIMEZONE     = 'seoul/korea'
STOCK_MARKET = 'KOSPI'


# Class Declaration
class Person(): # 클래스의 이름은 대분자로 시작, 가급적 한 음절로 구성, 부득이하게 두음절 이상으로 구성할 경우 Camel Notation(ex: MadGuy)으로 구성
    """
    <클래스에 대한 개괄적인 설명> (~이다. 체)
    사람을 모델링한 클래스이다.
    """

    def __init__(self, prop1, prop2="abc"):
        """
        <메서드에 대한 개괄적인 설명> (~이다 체)
        Person 클래스의 생성자이다.

        [Parameters]
        self  (Person) : 클래스 인스턴스
        prop1 (int)    : <해당 매개변수에 대한 간략한 설명>
        prop2 (str)    : <해당 매개변수에 대한 간략한 설명> (default="abc")

        [Returns]
        self (Person) : 생성된 클래스 인스턴스 
        """

        self.prop1 = prop1
        self.prop2 = prop2


# Function Declaration
def get_stock_name(ticker:str):  # 함수의 이름은 동사로 시작, 각 음절 사이에는 언더바(_) 삽입, 함수 이름은 소문자로 구성
    """
    <함수에 대한 개괄적인 설명> (~이다 체)
    종목명을 조회할 종목의 티커를 입력받아 종목명을 반환한다.

    [Parameters]
    ticker (str) : 조회할 종목의 티커

    [Returns]
    stock_name (str) : 종목 이름
    """

    stock_name = example_func(ticker)  # 변수의 이름은 명사로 구성, 각 음절 사이에는 언더바(_) 삽입, 변수 이름은 소문자로 구성

    return stock_name

        

