# 정치 이슈 분석 시스템

한국의 정치 뉴스를 수집, 분석, 클러스터링하여 이슈별로 정리하고 다양한 관점의 요약을 제공하는 시스템입니다.

## 🏗️ 시스템 구조

```
opinion/
├── README.md                    # 프로젝트 개요
├── requirements.txt             # 의존성 관리
├── .env.example                 # 환경변수 예시
├── config/                      # 설정 파일들
│   └── crawler_config.py       # 크롤러 설정
├── crawler/                     # 데이터 수집
│   ├── api_based/              # API 기반 크롤러
│   └── html_parsing/           # HTML 파싱 크롤러
├── clustering/                  # 클러스터링 (HDBSCAN)
├── content/                     # 콘텐츠 생성 (LLM 기반)
├── utils/                       # 공통 유틸리티
├── scripts/                     # 실행 스크립트
├── report/                      # 보고서 및 결과물
└── tests/                       # 테스트 파일들
```

## 🚀 빠른 시작

### 1. 환경 설정

```bash
# 저장소 클론
git clone <repository-url>
cd opinion

# 가상환경 생성 및 활성화
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 의존성 설치
pip install -r requirements.txt

# 환경변수 설정
cp .env.example .env
# .env 파일에 필요한 API 키들 설정
```

### 2. 전체 파이프라인 실행

```bash
# 모든 단계 실행 (크롤링 → 전처리 → 클러스터링 → 콘텐츠 생성)
python scripts/run_full_pipeline.py --all

# 크롤링 건너뛰고 실행 (이미 데이터가 있는 경우)
python scripts/run_full_pipeline.py --all --skip-crawling
```

### 3. 단계별 실행

```bash
# 1단계: 크롤링
python scripts/run_crawler.py

# 2단계: 전처리
python preprocessing/run_preprocessing.py

# 3단계: 클러스터링
python scripts/run_clustering.py

# 4단계: 콘텐츠 생성
python scripts/run_content_generation.py --all
```

## 📋 상세 사용법

### 크롤링

9개 언론사의 정치 뉴스를 수집합니다:

```bash
# 전체 크롤링
python scripts/run_crawler.py

# 특정 단계만 실행
python scripts/run_crawler.py --stage 1  # 단순한 크롤러
python scripts/run_crawler.py --stage 2  # 복잡한 크롤러
```

**지원 언론사:**
- API 기반: 한겨레, 뉴스원, 경향신문, 조선일보
- HTML 파싱: 동아일보, 중앙일보, 뉴시스, 오마이뉴스, 연합뉴스

### 전처리

기사 내용을 정제하고 통합합니다:

```bash
python preprocessing/run_preprocessing.py
```

- KST 기준 날짜를 UTC로 변환
- 기사 본문에서 앞 5문장 추출
- 노이즈 제거 (언론사 정보, 기자명 등)
- `articles_cleaned` 테이블에 저장

### 클러스터링

HDBSCAN 알고리즘으로 유사한 기사들을 그룹화합니다:

```bash
python scripts/run_clustering.py
```

- 코사인 유사도 기반 클러스터링
- `issues` 테이블에 이슈 저장
- `issue_articles` 테이블에 연결 저장

### 콘텐츠 생성

각 이슈별로 다양한 관점의 콘텐츠를 생성합니다:

```bash
# 모든 콘텐츠 생성
python scripts/run_content_generation.py --all

# 특정 단계만 실행
python scripts/run_content_generation.py --step 1  # 제목/부제목
python scripts/run_content_generation.py --step 2  # 관점별 뷰
python scripts/run_content_generation.py --step 3  # 요약
python scripts/run_content_generation.py --step 4  # 배경 정보
```

## 🔧 설정

### 환경변수 (.env)

```env
# Supabase
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key

# OpenAI
OPENAI_API_KEY=your_openai_api_key

# Perplexity (선택사항)
PERPLEXITY_API_KEY=your_perplexity_api_key
```

### 크롤러 설정 (config/crawler_config.py)

```python
CRAWLER_PARAMS = {
    "max_pages": 10,        # 최대 페이지 수
    "max_articles": 100,    # 최대 기사 수
    "delay": 1.0           # 요청 간 대기 시간
}
```

## 📊 데이터베이스 스키마

### 주요 테이블

- `articles`: 원본 기사 데이터
- `articles_cleaned`: 전처리된 기사 데이터
- `issues`: 클러스터링된 이슈
- `issue_articles`: 이슈-기사 연결
- `media_outlets`: 언론사 정보

### 데이터베이스 검사

```bash
python utils/db_inspector.py
```

## 🛠️ 개발 및 디버깅

### 로그 확인

각 스크립트는 상세한 로그를 출력합니다:
- 진행 상황 표시
- 오류 메시지
- 성공/실패 통계

### 테스트

```bash
# 개별 모듈 테스트
python -m pytest tests/

# 특정 테스트 실행
python -m pytest tests/test_crawler.py
```

### 데이터 초기화

```bash
# 모든 데이터 삭제
python scripts/clear_all_data.py

# 특정 테이블만 삭제
python scripts/clear_articles.py
python scripts/clear_issues_data.py
```

## 📈 성능 최적화

- **페이지네이션**: 대용량 데이터 처리 시 1000개씩 배치 처리
- **병렬 처리**: 크롤러는 가능한 한 병렬 실행
- **재시도 로직**: 네트워크 오류 시 자동 재시도

## 🔍 문제 해결

### 일반적인 문제

1. **메모리 부족**: 크롤러를 순차 실행하거나 배치 크기 줄이기
2. **API 제한**: 요청 간 대기 시간 늘리기
3. **DB 연결 오류**: Supabase 설정 확인

### 로그 분석

각 스크립트는 상세한 오류 메시지를 제공합니다. 오류 발생 시:
1. 오류 메시지 확인
2. 관련 설정 파일 검토
3. 데이터베이스 상태 확인

## 📝 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.

## 🤝 기여하기

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## 📞 지원

문제가 발생하거나 질문이 있으시면 이슈를 생성해 주세요.
