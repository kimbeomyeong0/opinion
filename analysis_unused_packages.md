# 불필요한 확장 프로그램 분석 결과

## 실제 사용되는 핵심 패키지
- `supabase` - 데이터베이스 연결 및 관리
- `beautifulsoup4` - HTML 파싱
- `httpx` - 비동기 HTTP 클라이언트
- `playwright` - 웹 스크래핑 및 브라우저 자동화
- `rich` - 콘솔 출력 및 UI
- `pytz` - 시간대 처리
- `pandas` - 데이터 처리 및 분석
- `numpy` - 수치 연산
- `requests` - HTTP 요청
- `python-dotenv` - 환경변수 관리
- `scikit-learn` - 머신러닝 (클러스터링)
- `sentence-transformers` - 텍스트 임베딩
- `umap-learn` - 차원 축소
- `hdbscan` - 클러스터링

## 불필요할 가능성이 높은 패키지들

### 1. 개발 도구 (프로덕션에서 불필요)
- `jupyter*` 관련 패키지들 (jupyterlab, notebook 등) - 개발용
- `mypy` - 타입 체킹 (개발 시에만 필요)
- `pytest*` 관련 패키지들 - 테스트용
- `debugpy` - 디버깅용

### 2. 사용되지 않는 라이브러리
- `selenium` - 코드에서 사용 안됨 (playwright 사용)
- `aiohttp` - httpx 사용으로 중복
- `openai` - 코드에서 직접 사용 안됨
- `gemini-cli` - 코드에서 사용 안됨
- `altgraph`, `macholib` - macOS 앱 빌드용 (불필요)
- `feedparser` - RSS 파싱용이나 사용 안됨
- `keybert` - 키워드 추출용이나 사용 안됨
- `lxml` - BeautifulSoup이 사용하지만 직접 사용 안됨

### 3. 중복되거나 과도한 패키지
- `asyncio` - Python 내장 모듈과 중복
- `future` - Python 3에서 불필요
- `six` - Python 2/3 호환성 (불필요)

## 권장사항
1. requirements.txt 파일 생성하여 필요한 패키지만 명시
2. 개발용 패키지는 requirements-dev.txt로 분리
3. 사용하지 않는 패키지 제거로 환경 정리
