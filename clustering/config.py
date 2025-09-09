#!/usr/bin/env python3
"""
클러스터링 설정 파일
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# OpenAI 설정
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = "gpt-4o-mini"

# 클러스터링 설정
UMAP_N_COMPONENTS = 2
UMAP_N_NEIGHBORS = 15  # 적절한 값으로 조정
UMAP_MIN_DIST = 0.3  # 적절한 값으로 조정
HDBSCAN_MIN_CLUSTER_SIZE = 10  # 46개 클러스터 생성
HDBSCAN_MIN_SAMPLES = 3  # 46개 클러스터 생성

# 이슈 생성 설정
MAX_ISSUES = 10  # source 기준 10위까지만 저장
TOP_ISSUES_FULL_CONTENT = 5  # 5위까지는 모든 컬럼 생성
# MAX_ARTICLES_FOR_LLM 제거 - 모든 기사 사용
# MAX_TITLES_FOR_LLM 제거 - 모든 기사 제목 사용

# 언론사 성향 설정 (media_outlets 테이블의 bias 컬럼 사용)
MEDIA_BIAS_MAPPING = {
    'left': ['한겨레', '오마이뉴스'],
    'center': ['연합뉴스', '뉴시스'],
    'right': ['조선일보', '동아일보', '중앙일보', '경향신문', '뉴스원', '한국경제', '매일경제']
}

# 데이터베이스 설정
PAGINATION_SIZE = 100
BATCH_SIZE = 100

def get_config() -> Dict[str, Any]:
    """설정 딕셔너리 반환"""
    return {
        "openai_api_key": OPENAI_API_KEY,
        "openai_model": OPENAI_MODEL,
        "umap_n_components": UMAP_N_COMPONENTS,
        "umap_n_neighbors": UMAP_N_NEIGHBORS,
        "umap_min_dist": UMAP_MIN_DIST,
        "hdbscan_min_cluster_size": HDBSCAN_MIN_CLUSTER_SIZE,
        "hdbscan_min_samples": HDBSCAN_MIN_SAMPLES,
        "max_issues": MAX_ISSUES,
        "top_issues_full_content": TOP_ISSUES_FULL_CONTENT,
        "media_bias_mapping": MEDIA_BIAS_MAPPING,
        "pagination_size": PAGINATION_SIZE,
        "batch_size": BATCH_SIZE
    }
