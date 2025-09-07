#!/usr/bin/env python3
"""
텍스트 정제 모듈
- 언론사별 특화 패턴 제거 (기자 정보, 이미지 설명, 광고 등)
- 이메일, 괄호 정보, 플랫폼 문구 제거
- 8개 언론사 패턴 분석 결과 반영
"""

import re
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class TextCleaningResult:
    """텍스트 정제 결과"""
    original_title: str
    original_content: str
    cleaned_title: str
    cleaned_content: str
    patterns_removed: List[str]
    cleaning_metadata: Dict[str, Any]
    success: bool
    error_message: Optional[str] = None

class TextCleaner:
    """텍스트 정제 클래스"""
    
    def __init__(self):
        """텍스트 정제기 초기화"""
        self.patterns = self._initialize_patterns()
        
    def _remove_simple_bylines(self, text: str) -> str:
        """문장 앞의 [지역=언론사] 또는 (지역=언론사) 패턴 제거 (간단한 방법)"""
        if not text:
            return text
        
        # 문장 시작의 [지역=언론사] 패턴 제거
        text = re.sub(r'^\[[^\]]*\]\s*', '', text)
        
        # 문장 시작의 (지역=언론사) 패턴 제거  
        text = re.sub(r'^\([^)]*\)\s*', '', text)
        
        return text.strip()
        
    def _initialize_patterns(self) -> Dict[str, Dict[str, List[str]]]:
        """언론사별 정제 패턴 초기화"""
        return {
            # 공통 패턴 (모든 언론사에 적용)
            'common': {
                'title': [],
                'content': [
                    # 모든 언론사의 기자/특파원 바이라인 제거 (중점· 문자 포함 지역명 처리)
                    # 최강 패턴: 모든 기자명과 직책 조합 처리
                    r'^\([^=·]+[·]?[^=]*=뉴스1\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'^\([^=·]+[·]?[^=]*=뉴스원\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'^\([^=·]+[·]?[^=]*=연합뉴스\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'^\([^=·]+[·]?[^=]*=뉴시스\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'^\[[^=·]+[·]?[^=]*=뉴스1\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'^\[[^=·]+[·]?[^=]*=뉴스원\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'^\[[^=·]+[·]?[^=]*=연합뉴스\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'^\[[^=·]+[·]?[^=]*=뉴시스\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    # 여러 기자명이 있는 경우 (공백으로 구분)
                    r'^\([^=·]+[·]?[^=]*=뉴스1\)\s*[가-힣\s]+(기자|특파원|기지)\s*[가-힣\s]*(기자|특파원|기지)\s*=+\s*',
                    r'^\([^=·]+[·]?[^=]*=뉴스원\)\s*[가-힣\s]+(기자|특파원|기지)\s*[가-힣\s]*(기자|특파원|기지)\s*=+\s*',
                    r'^\([^=·]+[·]?[^=]*=연합뉴스\)\s*[가-힣\s]+(기자|특파원|기지)\s*[가-힣\s]*(기자|특파원|기지)\s*=+\s*',
                    r'^\([^=·]+[·]?[^=]*=뉴시스\)\s*[가-힣\s]+(기자|특파원|기지)\s*[가-힣\s]*(기자|특파원|기지)\s*=+\s*',
                    r'^\[[^=·]+[·]?[^=]*=뉴스1\]\s*[가-힣\s]+(기자|특파원|기지)\s*[가-힣\s]*(기자|특파원|기지)\s*=+\s*',
                    r'^\[[^=·]+[·]?[^=]*=뉴스원\]\s*[가-힣\s]+(기자|특파원|기지)\s*[가-힣\s]*(기자|특파원|기지)\s*=+\s*',
                    r'^\[[^=·]+[·]?[^=]*=연합뉴스\]\s*[가-힣\s]+(기자|특파원|기지)\s*[가-힣\s]*(기자|특파원|기지)\s*=+\s*',
                    r'^\[[^=·]+[·]?[^=]*=뉴시스\]\s*[가-힣\s]+(기자|특파원|기지)\s*[가-힣\s]*(기자|특파원|기지)\s*=+\s*',
                    # 대괄호 패턴 (공백 없이 연결된 경우)
                    r'^\[[^=·]+[·]?[^=]*=뉴시스\][가-힣\s]*(특파원|기자)\s*=+\s*',
                    r'^\[[^=·]+[·]?[^=]*=뉴스1\][가-힣\s]*(특파원|기자)\s*=+\s*',
                    r'^\[[^=·]+[·]?[^=]*=연합뉴스\][가-힣\s]*(특파원|기자)\s*=+\s*',
                    # 문장 중간에서도 제거
                    r'\([^=·]+[·]?[^=]*=뉴스1\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'\([^=·]+[·]?[^=]*=뉴스원\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'\([^=·]+[·]?[^=]*=연합뉴스\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'\([^=·]+[·]?[^=]*=뉴시스\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'\[[^=·]+[·]?[^=]*=뉴스1\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'\[[^=·]+[·]?[^=]*=뉴스원\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'\[[^=·]+[·]?[^=]*=연합뉴스\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'\[[^=·]+[·]?[^=]*=뉴시스\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    # 추가 패턴: 기자 앞에 직책이 있는 경우
                    r'^\([^=·]+[·]?[^=]*=뉴스1\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*',
                    r'^\([^=·]+[·]?[^=]*=뉴스원\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*',
                    r'^\([^=·]+[·]?[^=]*=연합뉴스\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*',
                    r'^\([^=·]+[·]?[^=]*=뉴시스\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*',
                    r'\([^=·]+[·]?[^=]*=뉴스1\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*',
                    r'\([^=·]+[·]?[^=]*=뉴스원\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*',
                    r'\([^=·]+[·]?[^=]*=연합뉴스\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*',
                    r'\([^=·]+[·]?[^=]*=뉴시스\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*'
                ]
            },
            # 연합뉴스 패턴
            'yonhap': {
                'title': [
                    r'^\[속보\]\s*',
                    r'^\[게시판\]\s*',
                    r'^\[2보\]\s*',
                    r'^\[영상\]\s*'
                ],
                'content': [
                    # 본문 시작 (지역=연합뉴스) 기자/특파원 패턴 제거 (문장 시작부터)
                    r'^\([^=]+=연합뉴스\)\s*[가-힣\s]*\s*(기자|특파원)\s*=\s*',
                    # 기자/특파원 바이라인 제거 (문장 중간에서도)
                    r'\([^=]+=연합뉴스\)\s*[가-힣\s]*\s*(기자|특파원)\s*=\s*',
                    # [게시판] 기사 특수 패턴
                    r'^▲[^\n]*\n',  # 시작 세모
                    r'\([^=]+=연합뉴스\)\s*$'  # 끝 지역 정보
                ]
            },
            
            # 경향신문 패턴
            'khan': {
                'title': [
                    r'^\[속보\]\s*'
                ],
                'content': [
                    # 타 언론사 인용 패턴
                    r'[가-힣]+통신이?\s+\d+일\s+보도했다',
                    r'[가-힣]+\([A-Z]+\)는\s+\d+일\([^)]+\)',
                    r'[A-Z]+\s*<[^>]+>\s*에\s+출연해'
                ]
            },
            
            # 중앙일보 패턴
            'joongang': {
                'title': [
                    r'^\[속보\]\s*'
                ],
                'content': [
                    # 이미지 설명 문장 전체 제거 (더 정확한 패턴)
                    r'(?:^|(?<=\.\s))[^.]*(?:캡처)[^.]*\.\s*',  # 캡처가 포함된 문장
                    r'(?:^|(?<=\.\s))[^.]*(?:편집\s+[가-힣]+\s+PD)[^.]*\.\s*',  # 편집 이름 PD 패턴
                    r'(?:^|(?<=\.\s))[^.]*(?:[가-힣]+\s+기자)[^.]*\.\s*',  # 이름 기자 패턴
                    r'(?:^|(?<=\.\s))[^.]*(?:사진\s+[^.]*\s+제공)[^.]*\.\s*',  # 사진 제공 패턴
                    # 문장 끝 언론사 이름만 제거
                    r'\.\s*(뉴스1|연합뉴스|뉴시스|KBS|MBC|SBS|JTBC|YTN)\s*'
                ]
            },
            
            # 조선일보 패턴
            'chosun': {
                'title': [
                    r'^\[News&How\]\s*',
                    r'^\[정치 인사이드\]\s*',
                    r'^\[속보\]\s*',
                    r'^\[단독\]\s*'
                ],
                'content': []  # 조선일보는 매우 깔끔함
            },
            
            # 동아일보 패턴
            'donga': {
                'title': [
                    r'^\[속보\]\s*',
                    r'^\[단독\]\s*'
                ],
                'content': [
                    # 본문 끝 지역=언론사 패턴 제거 (가장 중요!)
                    r'\.\s*\([^)]*=[^)]+\)\s*$',
                    # JavaScript 광고 코드 제거 (매우 중요!)
                    r'googletag\.cmd\.push\(function\(\)\s*\{\s*googletag\.display\([^)]+\);\s*\}\);?',
                    # 이미지 설명 문장 제거
                    r'통신은[^.]*사진[^.]*보도했다\.\s*',
                    r'[^.]*사진을 함께 보도하며[^.]*\.\s*'
                ]
            },
            
            # 오마이뉴스 패턴
            'ohmynews': {
                'title': [
                    r'^\[오마이포토\]\s*',
                    r'^\[[^\]]*핫스팟[^\]]*\]\s*',
                    r'^\[속보\]\s*'
                ],
                'content': [
                    # 덧붙이는 글 섹션 전체 제거 (가장 중요!)
                    r'덧붙이는 글\s*\|[\s\S]*?글쓴이[^.]*\.',
                    # 방송 프로그램 정보 제거
                    r'■\s*방송\s*:[\s\S]*?■\s*대담\s*:[^\n]*',
                    # 인용 출처 표기 제거
                    r'※\s*내용 인용할 때[^.]*\.',
                    # 기사보강 태그 제거
                    r'\[기사보강:[^\]]*\]',
                    # 인터뷰 대화 형식 정리
                    r'◎\s*[^>]*>\s*',
                    # 광고 표시 제거
                    r'^AD$'
                ]
            },
            
            # 뉴시스 패턴
            'newsis': {
                'title': [
                    r'^\[속보\]\s*',
                    r'\[뉴시스Pic\]\s*$',
                    r'^\[단독\]\s*',
                    r'^\[종합\]\s*',
                    r'^\[영상\]\s*'
                ],
                'content': [
                    # 본문 시작 [지역=뉴시스] 기자명 패턴 제거 (가장 중요!)
                    r'^\[[^=]+=뉴시스\]\s*[가-힣\s]*\s*',
                    # 기자 바이라인 제거
                    r'\[[^=]+=뉴시스\]\s*[가-힣\s]*\s*기자\s*=\s*'
                ]
            },
            
            # 뉴스1 패턴
            'news1': {
                'title': [
                    r'^\[속보\]\s*',
                    r'^\[단독\]\s*',
                    r'^\[종합\]\s*'
                ],
                'content': [
                    # 본문 시작 (지역=뉴스1) 기자/특파원 패턴 제거 (문장 시작부터)
                    r'^\([^=]+=뉴스1\)\s*[가-힣\s]*\s*(기자|특파원)\s*=\s*',
                    # 기자/특파원 바이라인 제거 (문장 중간에서도)
                    r'\([^=]+=뉴스1\)\s*[가-힣\s]*\s*(기자|특파원)\s*=\s*'
                ]
            },
            
            # 뉴스원 패턴
            'newsone': {
                'title': [
                    r'^\[속보\]\s*',
                    r'^\[단독\]\s*',
                    r'^\[종합\]\s*'
                ],
                'content': [
                    # 본문 시작 (지역=뉴스원) 기자/특파원 패턴 제거 (문장 시작부터)
                    r'^\([^=]+=뉴스원\)\s*[가-힣\s]*\s*(기자|특파원)\s*=\s*',
                    # 기자/특파원 바이라인 제거 (문장 중간에서도)
                    r'\([^=]+=뉴스원\)\s*[가-힣\s]*\s*(기자|특파원)\s*=\s*'
                ]
            },
            
            # 한겨레 패턴
            'hani': {
                'title': [
                    r'^\[속보\]\s*',
                    r'^\[단독\]\s*'
                ],
                'content': [
                    # 언론사/기관명 한글 풀어쓰기 정리 (선택적)
                    # 이 패턴은 신중하게 적용 - 의미가 있는 경우도 많음
                ]
            },
            
            # 공통 패턴 (모든 언론사)
            'common': {
                'title': [
                    r'^\[속보\]\s*',
                    r'^\[단독\]\s*',
                    r'^\[긴급\]\s*'
                ],
                'content': [
                    # 모든 언론사의 기자/특파원 바이라인 제거 (중점· 문자 포함 지역명 처리)
                    # 최강 패턴: 모든 기자명과 직책 조합 처리
                    r'^\([^=·]+[·]?[^=]*=뉴스1\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'^\([^=·]+[·]?[^=]*=뉴스원\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'^\([^=·]+[·]?[^=]*=연합뉴스\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'^\([^=·]+[·]?[^=]*=뉴시스\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'^\[[^=·]+[·]?[^=]*=뉴스1\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'^\[[^=·]+[·]?[^=]*=뉴스원\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'^\[[^=·]+[·]?[^=]*=연합뉴스\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'^\[[^=·]+[·]?[^=]*=뉴시스\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    # 문장 중간에서도 제거
                    r'\([^=·]+[·]?[^=]*=뉴스1\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'\([^=·]+[·]?[^=]*=뉴스원\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'\([^=·]+[·]?[^=]*=연합뉴스\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'\([^=·]+[·]?[^=]*=뉴시스\)\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'\[[^=·]+[·]?[^=]*=뉴스1\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'\[[^=·]+[·]?[^=]*=뉴스원\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'\[[^=·]+[·]?[^=]*=연합뉴스\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    r'\[[^=·]+[·]?[^=]*=뉴시스\]\s*[가-힣\s]*(기자|특파원|기지)[가-힣\s]*=+\s*',
                    # 추가 패턴: 기자 앞에 직책이 있는 경우
                    r'^\([^=·]+[·]?[^=]*=뉴스1\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*',
                    r'^\([^=·]+[·]?[^=]*=뉴스원\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*',
                    r'^\([^=·]+[·]?[^=]*=연합뉴스\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*',
                    r'^\([^=·]+[·]?[^=]*=뉴시스\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*',
                    r'\([^=·]+[·]?[^=]*=뉴스1\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*',
                    r'\([^=·]+[·]?[^=]*=뉴스원\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*',
                    r'\([^=·]+[·]?[^=]*=연합뉴스\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*',
                    r'\([^=·]+[·]?[^=]*=뉴시스\)\s*[가-힣\s]*[가-힣]*기자[가-힣\s]*=+\s*',
                    # 기자명이 없는 경우도 제거 (단순 지역=언론사 패턴)
                    r'^\([^=·]+[·]?[^=]*=뉴스1\)\s*',
                    r'^\([^=·]+[·]?[^=]*=뉴스원\)\s*',
                    r'^\([^=·]+[·]?[^=]*=연합뉴스\)\s*',
                    r'^\([^=·]+[·]?[^=]*=뉴시스\)\s*',
                    r'^\[[^=·]+[·]?[^=]*=뉴스1\]\s*',
                    r'^\[[^=·]+[·]?[^=]*=뉴스원\]\s*',
                    r'^\[[^=·]+[·]?[^=]*=연합뉴스\]\s*',
                    r'^\[[^=·]+[·]?[^=]*=뉴시스\]\s*',
                    r'\([^=·]+[·]?[^=]*=뉴스1\)\s*',
                    r'\([^=·]+[·]?[^=]*=뉴스원\)\s*',
                    r'\([^=·]+[·]?[^=]*=연합뉴스\)\s*',
                    r'\([^=·]+[·]?[^=]*=뉴시스\)\s*',
                    r'\[[^=·]+[·]?[^=]*=뉴스1\]\s*',
                    r'\[[^=·]+[·]?[^=]*=뉴스원\]\s*',
                    r'\[[^=·]+[·]?[^=]*=연합뉴스\]\s*',
                    r'\[[^=·]+[·]?[^=]*=뉴시스\]\s*',
                    # 이메일 제거
                    r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
                    # 일반적인 광고 문구
                    r'무단\s*전재\s*및\s*재배포\s*금지',
                    r'저작권자\s*ⓒ[^.\n]*',
                    # 플랫폼 꼬리표
                    r'네이버\s*메인에서\s*보기',
                    r'카카오\s*채널\s*구독하기',
                    # 빈 괄호나 대괄호
                    r'\[\s*\]',
                    r'\(\s*\)'
                ]
            }
        }
    
    def detect_media_outlet(self, url: str) -> str:
        """URL을 기반으로 언론사 식별"""
        url_lower = url.lower()
        
        if 'yonhapnews.co.kr' in url_lower:
            return 'yonhap'
        elif 'khan.co.kr' in url_lower:
            return 'khan'
        elif 'joongang.co.kr' in url_lower:
            return 'joongang'
        elif 'chosun.com' in url_lower:
            return 'chosun'
        elif 'donga.com' in url_lower:
            return 'donga'
        elif 'ohmynews.com' in url_lower:
            return 'ohmynews'
        elif 'newsis.com' in url_lower:
            return 'newsis'
        elif 'hani.co.kr' in url_lower:
            return 'hani'
        else:
            return 'unknown'
    
    def clean_title(self, title: str, media_outlet: str) -> tuple[str, List[str]]:
        """제목 정제"""
        cleaned_title = title
        patterns_removed = []
        
        # 간단한 바이라인 제거 먼저 적용
        original_title = cleaned_title
        cleaned_title = self._remove_simple_bylines(cleaned_title)
        if cleaned_title != original_title:
            patterns_removed.append("simple_bylines_removal")
        
        # 언론사별 패턴 적용
        if media_outlet in self.patterns:
            for pattern in self.patterns[media_outlet]['title']:
                if re.search(pattern, cleaned_title):
                    patterns_removed.append(f"title_{media_outlet}_{pattern[:20]}")
                    cleaned_title = re.sub(pattern, '', cleaned_title)
        
        # 공통 패턴 적용
        for pattern in self.patterns['common']['title']:
            if re.search(pattern, cleaned_title):
                patterns_removed.append(f"title_common_{pattern[:20]}")
                cleaned_title = re.sub(pattern, '', cleaned_title)
        
        # 앞뒤 공백 제거
        cleaned_title = cleaned_title.strip()
        
        return cleaned_title, patterns_removed
    
    def clean_content(self, content: str, media_outlet: str) -> tuple[str, List[str]]:
        """본문 정제"""
        cleaned_content = content
        patterns_removed = []
        
        # 간단한 바이라인 제거 먼저 적용
        original_content = cleaned_content
        cleaned_content = self._remove_simple_bylines(cleaned_content)
        if cleaned_content != original_content:
            patterns_removed.append("simple_bylines_removal")
        
        # 공통 패턴 적용 (먼저 적용)
        for pattern in self.patterns['common']['content']:
            matches = re.findall(pattern, cleaned_content, re.MULTILINE | re.DOTALL)
            if matches:
                patterns_removed.append(f"content_common_{pattern[:20]}")
                cleaned_content = re.sub(pattern, '', cleaned_content, flags=re.MULTILINE | re.DOTALL)
        
        # 언론사별 패턴 적용
        if media_outlet in self.patterns:
            for pattern in self.patterns[media_outlet]['content']:
                matches = re.findall(pattern, cleaned_content, re.MULTILINE | re.DOTALL)
                if matches:
                    patterns_removed.append(f"content_{media_outlet}_{pattern[:20]}")
                    cleaned_content = re.sub(pattern, '', cleaned_content, flags=re.MULTILINE | re.DOTALL)
        
        # 연속된 공백과 줄바꿈 정리
        cleaned_content = re.sub(r'\n\s*\n\s*\n+', '\n\n', cleaned_content)  # 3개 이상 줄바꿈 -> 2개
        cleaned_content = re.sub(r'\s+', ' ', cleaned_content)  # 연속 공백 -> 단일 공백
        cleaned_content = cleaned_content.strip()
        
        return cleaned_content, patterns_removed
    
    def clean_article(self, article: Dict[str, Any]) -> TextCleaningResult:
        """단일 기사 정제"""
        try:
            title = article.get('title', '')
            content = article.get('content', '')
            url = article.get('url', '')
            
            # 언론사 식별
            media_outlet = self.detect_media_outlet(url)
            
            # 제목 정제
            cleaned_title, title_patterns = self.clean_title(title, media_outlet)
            
            # 본문 정제
            cleaned_content, content_patterns = self.clean_content(content, media_outlet)
            
            # 정제 메타데이터
            cleaning_metadata = {
                'media_outlet': media_outlet,
                'original_title_length': len(title),
                'original_content_length': len(content),
                'cleaned_title_length': len(cleaned_title),
                'cleaned_content_length': len(cleaned_content),
                'title_reduction_ratio': (len(title) - len(cleaned_title)) / len(title) if len(title) > 0 else 0,
                'content_reduction_ratio': (len(content) - len(cleaned_content)) / len(content) if len(content) > 0 else 0
            }
            
            all_patterns = title_patterns + content_patterns
            
            return TextCleaningResult(
                original_title=title,
                original_content=content,
                cleaned_title=cleaned_title,
                cleaned_content=cleaned_content,
                patterns_removed=all_patterns,
                cleaning_metadata=cleaning_metadata,
                success=True
            )
            
        except Exception as e:
            logger.error(f"기사 정제 중 오류 발생: {str(e)}")
            return TextCleaningResult(
                original_title=article.get('title', ''),
                original_content=article.get('content', ''),
                cleaned_title=article.get('title', ''),
                cleaned_content=article.get('content', ''),
                patterns_removed=[],
                cleaning_metadata={},
                success=False,
                error_message=str(e)
            )
    
    def clean_articles(self, articles: List[Dict[str, Any]]) -> List[TextCleaningResult]:
        """여러 기사 일괄 정제"""
        results = []
        
        for article in articles:
            result = self.clean_article(article)
            results.append(result)
        
        return results
    
    def get_cleaning_statistics(self, results: List[TextCleaningResult]) -> Dict[str, Any]:
        """정제 통계 정보"""
        if not results:
            return {}
        
        successful_results = [r for r in results if r.success]
        
        total_articles = len(results)
        successful_articles = len(successful_results)
        
        # 언론사별 통계
        media_stats = {}
        for result in successful_results:
            media = result.cleaning_metadata.get('media_outlet', 'unknown')
            if media not in media_stats:
                media_stats[media] = {
                    'count': 0,
                    'total_patterns_removed': 0,
                    'avg_title_reduction': 0,
                    'avg_content_reduction': 0
                }
            
            media_stats[media]['count'] += 1
            media_stats[media]['total_patterns_removed'] += len(result.patterns_removed)
            media_stats[media]['avg_title_reduction'] += result.cleaning_metadata.get('title_reduction_ratio', 0)
            media_stats[media]['avg_content_reduction'] += result.cleaning_metadata.get('content_reduction_ratio', 0)
        
        # 평균 계산
        for media in media_stats:
            count = media_stats[media]['count']
            if count > 0:
                media_stats[media]['avg_title_reduction'] /= count
                media_stats[media]['avg_content_reduction'] /= count
        
        return {
            'total_articles': total_articles,
            'successful_articles': successful_articles,
            'failed_articles': total_articles - successful_articles,
            'success_rate': successful_articles / total_articles if total_articles > 0 else 0,
            'media_statistics': media_stats,
            'total_patterns_removed': sum(len(r.patterns_removed) for r in successful_results),
            'avg_patterns_per_article': sum(len(r.patterns_removed) for r in successful_results) / successful_articles if successful_articles > 0 else 0
        }

if __name__ == "__main__":
    # 테스트용 코드
    cleaner = TextCleaner()
    
    # 테스트 기사
    test_articles = [
        {
            'title': '[속보] 이재명 대통령, 경제정책 발표',
            'content': '[서울=뉴시스]김기자 이재명 대통령이 오늘 경제정책을 발표했다.',
            'url': 'https://www.newsis.com/test'
        },
        {
            'title': '정치 뉴스입니다',
            'content': '내용입니다. 편집 김철수 PD 추가 내용입니다.',
            'url': 'https://joongang.co.kr/test'
        }
    ]
    
    results = cleaner.clean_articles(test_articles)
    stats = cleaner.get_cleaning_statistics(results)
    
    print("=== 텍스트 정제 결과 ===")
    for i, result in enumerate(results, 1):
        print(f"\n기사 {i}:")
        print(f"원본 제목: {result.original_title}")
        print(f"정제 제목: {result.cleaned_title}")
        print(f"제거된 패턴: {result.patterns_removed}")
        print(f"언론사: {result.cleaning_metadata.get('media_outlet', 'unknown')}")
    
    print(f"\n=== 통계 ===")
    print(f"총 기사: {stats['total_articles']}")
    print(f"성공: {stats['successful_articles']}")
    print(f"성공률: {stats['success_rate']:.2%}")
