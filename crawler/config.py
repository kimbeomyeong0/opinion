#!/usr/bin/env python3
"""
크롤러 설정 파일
"""

# 크롤러별 실행 파라미터 (run() 메서드에 전달되는 파라미터만 포함)
CRAWLER_PARAMS = {
    "ohmynews_politics": {"num_pages": 8},
    "yonhap_politics": {"num_pages": 10},
    "hani_politics": {"num_pages": 10},
    "newsone_politics": {"total_limit": 150},
    "khan_politics": {"num_pages": 15},
    "donga_politics": {"num_pages": 15},
    "joongang_politics": {"num_pages": 7},
    "newsis_politics": {"num_pages": 8},
    "chosun_politics": {"max_articles": 150}
}

# 크롤러별 메타데이터
CRAWLER_METADATA = {
    "ohmynews_politics": {
        "description": "오마이뉴스 정치 기사",
        "max_articles_per_page": 1,
        "expected_total": 8
    },
    "yonhap_politics": {
        "description": "연합뉴스 정치 기사",
        "max_articles_per_page": 15,
        "expected_total": 150
    },
    "hani_politics": {
        "description": "한겨레 정치 기사",
        "max_articles_per_page": 15,
        "expected_total": 150
    },
    "newsone_politics": {
        "description": "뉴스원 정치 기사",
        "max_articles_per_page": 10,
        "expected_total": 150
    },
    "khan_politics": {
        "description": "경향신문 정치 기사",
        "max_articles_per_page": 10,
        "expected_total": 150
    },
    "donga_politics": {
        "description": "동아일보 정치 기사",
        "max_articles_per_page": 10,
        "expected_total": 150
    },
    "joongang_politics": {
        "description": "중앙일보 정치 기사",
        "max_articles_per_page": 24,
        "expected_total": 168
    },
    "newsis_politics": {
        "description": "뉴시스 정치 기사",
        "max_articles_per_page": 20,
        "expected_total": 160
    },
    "chosun_politics": {
        "description": "조선일보 정치 기사",
        "max_articles_per_page": 50,
        "expected_total": 150
    }
}

# 크롤러 그룹 정의
CRAWLER_GROUPS = {
    "stage1_simple_html": {
        "crawlers": ["ohmynews_politics", "yonhap_politics"],
        "description": "단순한 HTML 크롤러",
        "execution_mode": "parallel",
        "max_concurrent": 2
    },
    "stage2_api_based": {
        "crawlers": ["hani_politics", "newsone_politics", "khan_politics"],
        "description": "API 기반 크롤러",
        "execution_mode": "parallel",
        "max_concurrent": 3
    },
    "stage3_complex_html": {
        "crawlers": ["donga_politics", "joongang_politics", "newsis_politics"],
        "description": "복잡한 HTML 크롤러 (Playwright 사용)",
        "execution_mode": "sequential",
        "max_concurrent": 1
    },
    "stage4_complex_api": {
        "crawlers": ["chosun_politics"],
        "description": "복잡한 API 크롤러",
        "execution_mode": "single",
        "max_concurrent": 1
    }
}

# Playwright 사용 크롤러 목록
PLAYWRIGHT_CRAWLERS = [
    "donga_politics", "joongang_politics", "newsis_politics", "chosun_politics"
]

# 크롤러별 리소스 사용량 (높을수록 많은 리소스 사용)
RESOURCE_USAGE = {
    "ohmynews_politics": "low",
    "yonhap_politics": "low",
    "hani_politics": "medium",
    "newsone_politics": "medium",
    "khan_politics": "medium",
    "donga_politics": "high",
    "joongang_politics": "high",
    "newsis_politics": "high",
    "chosun_politics": "high"
}

# 실행 순서 정의
EXECUTION_ORDER = [
    "stage1_simple_html",
    "stage2_api_based", 
    "stage3_complex_html",
    "stage4_complex_api"
]

# 단계별 대기 시간 (초)
STAGE_DELAYS = {
    "stage1_simple_html": 0,
    "stage2_api_based": 3,
    "stage3_complex_html": 3,
    "stage4_complex_api": 3
}

# 크롤러별 재시도 설정
RETRY_CONFIG = {
    "max_retries": 3,
    "retry_delay": 5,  # 초
    "exponential_backoff": True
}

# 모니터링 설정
MONITORING_CONFIG = {
    "enable_detailed_logging": True,
    "log_level": "INFO",
    "save_results_to_file": True,
    "results_file": "crawler_results.json"
}
