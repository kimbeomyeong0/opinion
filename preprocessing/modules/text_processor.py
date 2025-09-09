#!/usr/bin/env python3
"""
통합 텍스트 처리 모듈 - KISS 원칙 적용
text_cleaner.py와 text_normalizer.py를 통합
"""

import re
from typing import List, Dict, Any, NamedTuple
from dataclasses import dataclass

class TextChange(NamedTuple):
    """텍스트 변경 사항"""
    original: str
    changed: str
    pattern: str

@dataclass
class TextProcessingResult:
    """텍스트 처리 결과"""
    normalized_text: str
    changes_made: List[TextChange]

class TextProcessor:
    """통합 텍스트 처리 클래스 - 정제 + 정규화"""
    
    def __init__(self):
        """초기화"""
        # 언론사별 패턴 정의 (동적으로 로드 가능하도록 개선)
        self.media_patterns = self._load_media_patterns()
        
        # 공통 패턴 정의
        self.common_patterns = [
            r'\[.*?\]',  # 대괄호 내용
            r'\(.*?\)',  # 괄호 내용
            r'【.*?】',   # 특수 괄호
            r'<.*?>',    # HTML 태그
            r'&[a-zA-Z0-9#]+;',  # HTML 엔티티
            r'[^\w\s가-힣]',  # 특수문자 (한글, 영문, 숫자, 공백 제외)
        ]
    
    def _load_media_patterns(self) -> Dict[str, List[str]]:
        """언론사별 패턴 동적 로드"""
        return {
            'chosun': [
                r'\[조선일보\]', r'조선일보', r'\[조선\]'
            ],
            'hani': [
                r'\[한겨레\]', r'한겨레', r'\[한겨레신문\]'
            ],
            'yonhap': [
                r'\[연합뉴스\]', r'연합뉴스', r'\[연합\]'
            ],
            'donga': [
                r'\[동아일보\]', r'동아일보', r'\[동아\]'
            ],
            'joongang': [
                r'\[중앙일보\]', r'중앙일보', r'\[중앙\]'
            ],
            'khan': [
                r'\[경향신문\]', r'경향신문', r'\[경향\]'
            ],
            'ohmynews': [
                r'\[오마이뉴스\]', r'오마이뉴스', r'\[오마이\]'
            ],
            'newsis': [
                r'\[뉴시스\]', r'뉴시스'
            ],
            'newsone': [
                r'\[뉴스원\]', r'뉴스원', r'\[뉴스1\]'
            ]
        }
    
    def clean_title(self, title: str, media_outlet: str = 'unknown') -> tuple:
        """제목 정제"""
        if not title:
            return '', []
        
        cleaned_title = title.strip()
        patterns_removed = []
        
        # 언론사별 패턴 제거
        if media_outlet in self.media_patterns:
            for pattern in self.media_patterns[media_outlet]:
                if re.search(pattern, cleaned_title):
                    cleaned_title = re.sub(pattern, '', cleaned_title).strip()
                    patterns_removed.append(pattern)
        
        # 공통 패턴 제거
        for pattern in self.common_patterns:
            if re.search(pattern, cleaned_title):
                cleaned_title = re.sub(pattern, '', cleaned_title).strip()
                patterns_removed.append(pattern)
        
        # 연속 공백 정리
        cleaned_title = re.sub(r'\s+', ' ', cleaned_title).strip()
        
        return cleaned_title, patterns_removed
    
    def clean_content(self, content: str, media_outlet: str = 'unknown') -> tuple:
        """내용 정제"""
        if not content:
            return '', []
        
        cleaned_content = content.strip()
        patterns_removed = []
        
        # 언론사별 패턴 제거
        if media_outlet in self.media_patterns:
            for pattern in self.media_patterns[media_outlet]:
                if re.search(pattern, cleaned_content):
                    cleaned_content = re.sub(pattern, '', cleaned_content).strip()
                    patterns_removed.append(pattern)
        
        # 공통 패턴 제거
        for pattern in self.common_patterns:
            if re.search(pattern, cleaned_content):
                cleaned_content = re.sub(pattern, '', cleaned_content).strip()
                patterns_removed.append(pattern)
        
        # 연속 공백 정리
        cleaned_content = re.sub(r'\s+', ' ', cleaned_content).strip()
        
        return cleaned_content, patterns_removed
    
    def normalize_text(self, text: str) -> TextProcessingResult:
        """텍스트 정규화"""
        if not text:
            return TextProcessingResult(normalized_text='', changes_made=[])
        
        normalized_text = text.strip()
        changes_made = []
        
        # 기본 정규화 규칙들
        normalization_rules = [
            (r'[가-힣]+', self._normalize_korean),
            (r'[a-zA-Z]+', self._normalize_english),
            (r'\d+', self._normalize_numbers),
            (r'\s+', self._normalize_whitespace),
        ]
        
        for pattern, normalizer in normalization_rules:
            matches = re.finditer(pattern, normalized_text)
            for match in matches:
                original = match.group()
                normalized = normalizer(original)
                if original != normalized:
                    changes_made.append(TextChange(
                        original=original,
                        changed=normalized,
                        pattern=pattern
                    ))
                    normalized_text = normalized_text.replace(original, normalized, 1)
        
        return TextProcessingResult(
            normalized_text=normalized_text,
            changes_made=changes_made
        )
    
    def _normalize_korean(self, text: str) -> str:
        """한글 정규화"""
        # 기본적인 한글 정규화 (간소화)
        return text.strip()
    
    def _normalize_english(self, text: str) -> str:
        """영문 정규화"""
        # 기본적인 영문 정규화 (간소화)
        return text.strip()
    
    def _normalize_numbers(self, text: str) -> str:
        """숫자 정규화"""
        # 기본적인 숫자 정규화 (간소화)
        return text.strip()
    
    def _normalize_whitespace(self, text: str) -> str:
        """공백 정규화"""
        # 연속 공백을 단일 공백으로 변환
        return re.sub(r'\s+', ' ', text).strip()
    
    def process_text(self, text: str, media_outlet: str = 'unknown') -> tuple:
        """텍스트 통합 처리 (정제 + 정규화)"""
        # 1단계: 정제
        cleaned_text, patterns_removed = self.clean_content(text, media_outlet)
        
        # 2단계: 정규화
        result = self.normalize_text(cleaned_text)
        
        return result.normalized_text, patterns_removed, result.changes_made
