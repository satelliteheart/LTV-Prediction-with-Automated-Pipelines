# ltv-pipeline
Development of LTV Prediction Algorithm &amp; Establishment of Automated Pipelines

## 요약
매체 API를 이용해 데이터 파이프라인을 구축하고, 특정 행동까지 이어진 유저들의 Critical Path를 분석해 캠페인별 LTV 예측을 진행
예측 LTV값 확인과 통합 채널 분석을 위해 Tableau로 데이터 시각화를 제공

## 시스템 구성도
![](/src/system.png)

## 데이터 구조도
![](/src/db.png)

## 결과 대시보드
![](/src/dashboard.jpeg)


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