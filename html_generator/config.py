#!/usr/bin/env python3
"""
HTML 제너레이터 설정 파일
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 출력 설정
OUTPUT_DIR = "reports"
OUTPUT_FILENAME = "issues_report.html"
TEMPLATE_DIR = "templates"
STYLES_DIR = "styles"
ASSETS_DIR = "assets"

# HTML 설정
HTML_TITLE = "정치 이슈 분석 레포트"
HTML_DESCRIPTION = "AI 기반 정치 이슈 클러스터링 및 관점 분석"
HTML_AUTHOR = "Opinion Project"

# 스타일 설정
THEME_COLORS = {
    'primary': '#2563eb',      # 파란색 (지지)
    'secondary': '#6b7280',    # 회색 (중립)
    'accent': '#dc2626',       # 빨간색 (비판)
    'background': '#f8fafc',   # 배경색
    'text': '#1f2937',         # 텍스트색
    'border': '#e5e7eb'        # 테두리색
}

# 레이아웃 설정
MAX_ISSUES_DISPLAY = 5
CARDS_PER_ROW = 1  # 반응형에서 조정됨

def get_config() -> Dict[str, Any]:
    """설정 딕셔너리 반환"""
    return {
        "output_dir": OUTPUT_DIR,
        "output_filename": OUTPUT_FILENAME,
        "template_dir": TEMPLATE_DIR,
        "styles_dir": STYLES_DIR,
        "assets_dir": ASSETS_DIR,
        "html_title": HTML_TITLE,
        "html_description": HTML_DESCRIPTION,
        "html_author": HTML_AUTHOR,
        "theme_colors": THEME_COLORS,
        "max_issues_display": MAX_ISSUES_DISPLAY,
        "cards_per_row": CARDS_PER_ROW
    }
