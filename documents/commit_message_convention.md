# Commit Message Convention: 커밋 메시지 컨벤션
<br />
<br />
<br />

## Commit Message Structure: 커밋 메시지 구조
[\<type\>] \<title\> <br />
\<body\> <br />
\<footer\> <br />
<br />
<br />
<br />

## Commit Message Components: 커밋 메시지 구성요소
| Component | Description            | Option    |
|:---------:|:----------------------:|:---------:|
| \<type\>    | 커밋 메시지 타입              | Mandatory |
| \<title\>   | 커밋 메시지 제목              | Mandatory |
| \<body\>    | 커밋 메시지 상세내용            | Optional  |
| \<footer\>  | 참조 정보<br>(이슈 트래킹 ID 등) | Optional  |
<br />
<br />
<br />

## Commit Message Types: 커밋 메시지 타입
| \<type\> | Description             |
|:--------:|:-----------------------:|
| FEAT     | 새로운 기능 관련               |
| FIX      | 버그 및 로직 수정 관련                |
| DOCS     | 문서 작업 관련                |
| STYLE    | 코드 의미와는 무관한, 코드 변경사항 관련 |
| REFACTOR | 코드 리팩토링 관련              |
| TEST     | 테스트 코드 관련               |
| CHORE    | 빌더/패키지 매니저 코드 관련        |
| DESIGN   | UI 디자인 관련               |
| RENAME   | 파일 이름 수정 관련             |
| REMOVE   | 파일 삭제 관련                |
<br />
<br />
<br />

## Commit Message Examples: 커밋 메시지 예시
1. [DOCS] create new coding convention <br />: 새로운 코딩 컨벤션 관련 문서를 만듦 <br />

2. [FEAT] create stock search bar <br />: 주식 검색창 기능을 만듦 <br />

3. [RENAME] rename 'apiHandler.py' to 'api_handler.py' <br />: apiHandler.py 파일의 이름을 'api_handler.py'로 변경함 <br />

4. [FIX] modify 'news_crawler.py's logic to operate faster <br />: news_crawler.py 파일의 로직을 더 빠르게 구동되도록 수정함 <br />
