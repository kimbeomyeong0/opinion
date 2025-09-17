# 불필요한 패키지 정리 계획

## 1단계: 가상환경 정리
현재 많은 불필요한 패키지들이 설치되어 있어 환경을 정리하는 것이 좋습니다.

### 권장 방법:
```bash
# 새로운 가상환경 생성
python3 -m venv opinion_clean_env
source opinion_clean_env/bin/activate

# 필요한 패키지만 설치
pip install -r requirements.txt

# 개발 시에만 추가 패키지 설치
pip install -r requirements-dev.txt
```

## 2단계: 불필요한 패키지 목록

### 완전히 제거 가능한 패키지들:
- `selenium` (playwright 사용으로 대체됨)
- `aiohttp` (httpx 사용으로 중복)
- `openai` (직접 사용하지 않음)
- `gemini-cli` (사용하지 않음)
- `feedparser` (RSS 파싱하지 않음)
- `keybert` (키워드 추출 사용하지 않음)
- `altgraph`, `macholib` (macOS 앱 빌드용)
- `asyncio` (Python 내장 모듈과 중복)
- `future`, `six` (Python 3에서 불필요)

### 개발환경에서만 필요한 패키지들:
- `jupyterlab`, `notebook` 관련 패키지들
- `mypy` (타입 체킹)
- `pytest` 관련 패키지들
- `debugpy`

## 3단계: 환경 최적화 스크립트

```bash
#!/bin/bash
# cleanup_environment.sh

echo "현재 설치된 패키지 수: $(pip list | wc -l)"

# 불필요한 패키지 제거
pip uninstall -y selenium aiohttp openai gemini-cli feedparser keybert altgraph macholib asyncio future six

echo "정리 후 패키지 수: $(pip list | wc -l)"
echo "requirements.txt 기반으로 환경을 재구성하는 것을 권장합니다."
```

## 4단계: 지속적인 관리
1. 새로운 패키지 설치 시 requirements.txt 업데이트
2. 정기적으로 사용하지 않는 패키지 점검
3. 개발용과 프로덕션용 의존성 분리 유지
