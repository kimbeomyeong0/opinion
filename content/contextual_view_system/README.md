# 맥락 기반 관점 생성 시스템

이슈 특성과 맥락을 고려한 지능형 관점 생성 시스템입니다.

## 📁 디렉토리 구조

```
content/
├── contextual_view_system/               # 맥락 기반 관점 생성 모듈들
│   ├── __init__.py                       # 모듈 초기화
│   ├── issue_analyzer.py                 # 이슈 특성 분석
│   ├── contextual_bias_interpreter.py    # 맥락 기반 성향 해석
│   ├── multi_layer_view_generator.py     # 다층적 관점 생성
│   ├── intelligent_prompt_generator.py   # 지능형 프롬프트 생성
│   ├── view_quality_checker.py          # 관점 품질 검증
│   └── README.md                        # 이 파일
├── run_contextual_view_generator.py     # 통합 실행 시스템
└── run_view_generator.py                # 기존 인터페이스 (하위 호환성)
```

## 🔧 모듈 설명

### 1. IssueAnalyzer (`issue_analyzer.py`)
- **기능**: 이슈의 본질적 특성을 분석
- **주요 기능**:
  - 이슈 유형 자동 분류 (경제, 환경, 안보, 기술, 사회, 정치)
  - 이해관계자 파악 (정부, 기업, 시민, 국제기구 등)
  - 핵심 가치 갈등 분석 (자유 vs 평등, 효율 vs 공정 등)
  - 복잡도 및 긴급성 평가

### 2. ContextualBiasInterpreter (`contextual_bias_interpreter.py`)
- **기능**: 이슈 맥락에 맞게 성향을 해석
- **주요 기능**:
  - 이슈별 맞춤형 성향 가이드라인
  - 고정된 스테레오타입 회피
  - 맥락적 뉘앙스 반영

### 3. MultiLayerViewGenerator (`multi_layer_view_generator.py`)
- **기능**: 다층적 관점 구조 생성
- **주요 기능**:
  - 3단계 구조: [기본입장] → [근거설명] → [대안관점]
  - 기사 분석을 통한 인사이트 추출
  - 맥락 기반 관점 구성

### 4. IntelligentPromptGenerator (`intelligent_prompt_generator.py`)
- **기능**: 적응형 프롬프트 생성
- **주요 기능**:
  - 이슈 특성에 맞는 동적 프롬프트
  - 맥락 가이드라인 통합
  - 후속 질문 생성

### 5. ViewQualityChecker (`view_quality_checker.py`)
- **기능**: 관점 품질 검증
- **주요 기능**:
  - 7가지 품질 기준 평가
  - 자동 점수 계산
  - 개선 제안 생성

### 6. ContextualViewGenerator (`../run_contextual_view_generator.py`)
- **기능**: 통합 실행 시스템
- **주요 기능**:
  - 병렬 처리로 효율성 향상
  - 실시간 품질 모니터링
  - 상세한 로깅 및 보고서

## 🚀 사용 방법

### 기본 사용법
```python
from content.run_contextual_view_generator import ContextualViewGenerator

# 생성기 초기화
generator = ContextualViewGenerator()

# 단일 이슈 처리
success = generator.process_issue(issue_id)

# 모든 이슈 처리
success = generator.process_all_issues()
```

### 명령행 실행
```bash
# 모든 이슈 처리
python content/run_view_generator.py

# 단일 이슈 테스트
python content/run_view_generator.py test
```

## 📊 품질 검증 기준

1. **성향 일관성** (25%): 성향에 맞는 키워드 사용
2. **이슈 관련성** (20%): 이슈와 관련된 구체적 내용
3. **뉘앙스 존재** (15%): 균형잡힌 표현 사용
4. **스테레오타입 회피** (15%): 극단적 표현 피하기
5. **건설적 톤** (10%): 해결책 중심의 표현
6. **명확성** (10%): 이해하기 쉬운 문장 구조
7. **길이 적절성** (5%): 80-100자 내외

## 🔄 기존 시스템과의 차이점

| 구분 | 기존 시스템 | 새로운 시스템 |
|------|-------------|---------------|
| **성향 해석** | 고정된 3개 가이드라인 | 이슈별 맞춤형 해석 |
| **관점 구조** | 단순한 30-50자 텍스트 | 다층적 구조 |
| **프롬프트** | 정적 템플릿 | 동적 적응형 프롬프트 |
| **품질 관리** | 없음 | 7가지 기준 자동 검증 |
| **맥락 고려** | 없음 | 이슈 특성 반영 |

## 📈 예상 효과

- **더 정확한 관점**: 이슈의 맥락을 고려한 맞춤형 관점
- **스테레오타입 회피**: 자연스럽고 뉘앙스 있는 관점
- **품질 보장**: 자동 검증을 통한 일관된 품질
- **사용자 경험 향상**: 더 깊이 있고 이해하기 쉬운 관점
- **시스템 신뢰성**: 다각도 검증을 통한 안정성

## 🔧 개발자 정보

이 시스템은 기존 view_generator의 한계를 극복하기 위해 개발되었습니다.
- **기존 파일 백업**: `run_view_generator_backup.py`
- **하위 호환성**: 기존 인터페이스 유지
- **모듈화**: 각 기능별 독립적 모듈 구성
