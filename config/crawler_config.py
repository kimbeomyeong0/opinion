#!/usr/bin/env python3
"""
크롤러 설정 파일 - KISS 원칙 적용
"""

# 크롤러별 실행 파라미터
CRAWLER_PARAMS = {
    # 기존 크롤러들
    "ohmynews_politics": {"num_pages": 8},
    "yonhap_politics": {"num_pages": 10},
    "hani_politics": {"num_pages": 10},
    "newsone_politics": {"total_limit": 150},
    "khan_politics": {"num_pages": 15},
    "donga_politics": {"num_pages": 15},
    "joongang_politics": {"num_pages": 7},
    "newsis_politics": {"num_pages": 8},
    "chosun_politics": {"max_articles": 150},
    
    # 새로운 진보 성향 크롤러들
    "segye_politics": {"num_pages": 10},      # 156개 기사
    "munhwa_politics": {"num_pages": 13},     # 156개 기사
    "naeil_politics": {"num_pages": 8},       # 160개 기사
    "pressian_politics": {"num_pages": 16},   # 160개 기사
    "hankyung_politics": {"num_pages": 4},    # 160개 기사
    "sisain_politics": {"num_pages": 15}      # 158개 기사
}

# 크롤러 그룹 정의 (3단계로 확장)
CRAWLER_GROUPS = {
    "simple": {
        "crawlers": ["ohmynews_politics", "yonhap_politics", "hani_politics", "newsone_politics", "khan_politics"],
        "description": "기존 단순한 크롤러 (HTML/API 기반)",
        "execution_mode": "parallel",
        "max_concurrent": 3
    },
    "progressive": {
        "crawlers": ["segye_politics", "munhwa_politics", "naeil_politics", "pressian_politics", "hankyung_politics", "sisain_politics"],
        "description": "새로운 진보 성향 크롤러 (HTML/Hybrid 기반)",
        "execution_mode": "parallel",
        "max_concurrent": 4
    },
    "complex": {
        "crawlers": ["donga_politics", "joongang_politics", "newsis_politics", "chosun_politics"],
        "description": "기존 복잡한 크롤러 (Playwright 사용)",
        "execution_mode": "sequential",
        "max_concurrent": 1
    }
}

# Playwright 사용 크롤러 목록
PLAYWRIGHT_CRAWLERS = [
    "donga_politics", "joongang_politics", "newsis_politics", "chosun_politics"
]

# 단계별 대기 시간 (초)
STAGE_DELAYS = {
    "simple": 0,
    "progressive": 2,
    "complex": 3
}

# 재시도 설정
RETRY_CONFIG = {
    "max_retries": 3,
    "retry_delay": 5
}
