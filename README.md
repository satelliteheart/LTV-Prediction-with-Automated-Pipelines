# ltv-pipeline
Development of LTV Prediction Algorithm &amp; Establishment of Automated Pipelines

## 요약
매체 API를 이용해 데이터 파이프라인을 구축하고, 특정 행동까지 이어진 유저들의 Critical Path를 분석해 캠페인별 LTV 예측을 진행
예측 LTV값 확인과 통합 채널 분석을 위해 Tableau로 데이터 시각화를 제공

## 시스템 구성도
![](/src/system.png)

## 파이프라인 설명

### data ingestion
- 채널별 API Call Limit을 고려한 효율적인 데이터 수집 주기 설정

### data cleansing
- Event Property 기반 중복 데이터 제거
- Test 데이터 및 Fraud User Data 등 Dirty Data 정제
- 데이터 Cross Checking을 통해 ADID / User ID 등 누락 필드 처리
- Facebook Data의 경우 전환 데이터 처리

### data processing
- Campaign 데이터 및 내부 데이터 연결 최적화
- 데이터 시각화를 위한 데이터 형태 통일 작업 진행
- 신규 캠페인 및 이벤트 처리 자동화 및 가이드 제공
- 통합 채널 분석을 위한 대시보드 데이터 처리 진행

### data caculating
- 분석 & 액션 목적에 부합한 데이터 마트 설계 및 구현
- 분석 데이터 추출을 위한 자동 연산 프로세스 진행


## 데이터 구조도
![](/src/db.png)

## 프로젝트 구조
.
├── README.md
├── ads-pipeline
│   ├── Dockerfile
│   ├── aws
│   ├── build_and_push.sh
│   ├── credentials
│   ├── leaf
│   │   ├── __init__.py
│   │   ├── adison.py
│   │   ├── apple.py
│   │   ├── appsflyer.py
│   │   ├── defintion
│   │   │   └── ingest.py
│   │   ├── facebook.py
│   │   ├── google.py
│   │   ├── mixpanel.py
│   │   ├── mixpanel_user.py
│   │   ├── naver.py
│   │   └── spreadsheet.py
│   ├── main.py
│   ├── mysql
│   ├── requirements.txt
│   └── tools
├── caculating-pipeline
│   ├── Dockerfile
│   ├── aws
│   ├── build_and_push.sh
│   ├── credentials
│   ├── leaf
│   │   ├── __init__.py
│   │   └── mysql.py
│   ├── main.py
│   ├── requirements.txt
│   └── tools
├── cleansing-pipeline
│   ├── Dockerfile
│   ├── aws
│   ├── branch
│   ├── build_and_push.sh
│   ├── credentials
│   ├── leaf
│   │   ├── __init__.py
│   │   ├── adchannels.py
│   │   └── mixpanel.py
│   ├── main.py
│   ├── requirements.txt
│   └── tools
└── processing-pipeline
    ├── Dockerfile
    ├── aws
    ├── build_and_push.sh
    ├── credentials
    ├── leaf
    │   ├── __init__.py
    │   ├── appsflyer.py
    │   ├── calculation.py
    │   ├── mixpanel.py
    │   └── mysql.py
    ├── main.py
    ├── requirements.txt
    └── tools
