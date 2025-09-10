# 임베딩 모듈

기사 데이터의 임베딩 벡터를 생성하는 모듈입니다.

## 기능

- `articles_cleaned` 테이블의 `merged_content`를 임베딩
- OpenAI `text-embedding-3-small` 모델 사용 (1536차원)
- `articles_embeddings` 테이블에 저장

## 사용법

```bash
python3 run_embedding.py
```

## 처리 과정

1. **테이블 초기화**: `articles_embeddings` 테이블의 모든 데이터 삭제
2. **기사 조회**: `articles_cleaned` 테이블에서 모든 기사 조회
3. **텍스트 전처리**: 
   - 최대 4000자로 제한
   - 10자 미만 제외
4. **배치 처리**: 100개씩 묶어서 처리
5. **임베딩 생성**: OpenAI API 호출
6. **저장**: `articles_embeddings` 테이블에 저장

## 설정

- **모델**: `text-embedding-3-small`
- **배치 크기**: 100개
- **최대 텍스트 길이**: 4000자
- **벡터 차원**: 1536차원

## 출력

- 진행 상황: `처리 중: 100/1000 (10.0%)`
- 최종 통계: 성공/실패 건수 및 성공률

## 주의사항

- OpenAI API 키가 `.env` 파일에 설정되어 있어야 함
- 네트워크 연결 필요
- API 사용량에 따른 비용 발생
