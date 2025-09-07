#!/usr/bin/env python3
"""
임베딩 설정 파일
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# OpenAI 설정
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMBEDDING_MODEL = "text-embedding-3-small"  # 클러스터링에 적합한 모델
EMBEDDING_DIMENSIONS = 1536  # text-embedding-3-small의 기본 차원

# 배치 처리 설정
BATCH_SIZE = 100  # 한 번에 처리할 기사 수
MAX_RETRIES = 3  # API 호출 재시도 횟수
RETRY_DELAY = 1  # 재시도 간격 (초)

# 데이터베이스 설정
PAGINATION_SIZE = 1000  # 페이지네이션 크기

# 로깅 설정
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# 임베딩 타입 정의
EMBEDDING_TYPES = {
    "CLUSTERING": "combined",  # 클러스터링용 임베딩 (combined 타입 사용)
    "SIMILARITY": "content",   # 유사도 검색용 임베딩
    "SEARCH": "title"          # 검색용 임베딩
}

def get_config() -> Dict[str, Any]:
    """설정 딕셔너리 반환"""
    return {
        "openai_api_key": OPENAI_API_KEY,
        "embedding_model": EMBEDDING_MODEL,
        "embedding_dimensions": EMBEDDING_DIMENSIONS,
        "batch_size": BATCH_SIZE,
        "max_retries": MAX_RETRIES,
        "retry_delay": RETRY_DELAY,
        "pagination_size": PAGINATION_SIZE,
        "log_level": LOG_LEVEL,
        "log_format": LOG_FORMAT,
        "embedding_types": EMBEDDING_TYPES
    }
