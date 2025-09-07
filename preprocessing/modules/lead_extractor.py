#!/usr/bin/env python3
"""
리드문 추출 모듈
본문에서 첫 2-3문장만 추출하여 전처리 성능을 최적화합니다.
"""

import re
from typing import Optional, List


class LeadExtractor:
    """리드문 추출 클래스"""

    def __init__(self, max_sentences: int = 3):
        """
        Args:
            max_sentences: 추출할 최대 문장 수 (기본값: 3)
        """
        self.max_sentences = max_sentences

    def _split_sentences(self, text: str) -> List[str]:
        """텍스트를 문장 단위로 분할"""
        # 마침표, 물음표, 느낌표를 기준으로 분할하되, 약어(ex. Dr.)나 숫자(ex. 1.2) 뒤의 마침표는 무시
        # 긍정형 후방탐색을 사용하여 마침표 뒤에 공백이나 다음 문자가 오는 경우만 분할
        sentences = re.split(r'(?<=[.?!])\s+(?=[가-힣A-Za-z0-9])', text)
        # 마지막 문장이 마침표로 끝나지 않으면 추가
        sentences = [s.strip() for s in sentences if s.strip()]
        return sentences

    def extract_lead_paragraph(self, content: str) -> str:
        """
        본문에서 리드문만 추출

        Args:
            content: 원본 본문 텍스트

        Returns:
            추출된 리드문 (첫 2-3문장)
        """
        if not content or not content.strip():
            return ""

        # 문장 단위로 분할 (마침표 기준)
        sentences = self._split_sentences(content)

        # 첫 N문장만 추출
        lead_sentences = sentences[:self.max_sentences]

        # 문장 완성도 확인 및 조합
        lead_paragraph = ' '.join(lead_sentences)
        # 마지막 문장이 마침표로 끝나지 않으면 추가 (단, 이미 구두점으로 끝나는 경우는 제외)
        if lead_paragraph and not re.search(r'[.?!]$', lead_paragraph):
            lead_paragraph += '.'

        return lead_paragraph
