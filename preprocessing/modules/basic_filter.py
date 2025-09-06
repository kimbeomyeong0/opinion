#!/usr/bin/env python3
"""
기본 필터링 모듈
- 본문 없는 기사 제거
- 뉴스통신사업자 기사 제거 (연합뉴스, 뉴시스 등)
- 짧은 기사 제거 (문장 1-2개짜리 속보성 기사)
"""

import sys
import os
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

@dataclass
class BasicFilterResult:
    """기본 필터링 결과"""
    total_articles: int
    no_content_removed: int
    news_agency_removed: int
    short_articles_removed: int
    final_articles: int
    processing_time: float
    success: bool
    error_message: Optional[str] = None

class BasicFilter:
    """기본 필터링 클래스"""
    
    def __init__(self, min_sentences: int = 3, min_content_length: int = 100):
        """
        초기화
        
        Args:
            min_sentences: 최소 문장 수 (기본값: 3)
            min_content_length: 최소 본문 길이 (기본값: 100자)
        """
        self.min_sentences = min_sentences
        self.min_content_length = min_content_length
        
        # 뉴스통신사업자 목록 (제거 대상)
        self.news_agencies = {
            '연합뉴스', '뉴시스', '뉴스1', 'YTN', 'KBS', 'MBC', 'SBS',
            '연합뉴스TV', '뉴시스TV', '뉴스1TV', 'JTBC', 'TV조선', '채널A',
            '머니투데이', '이데일리', '파이낸셜뉴스', '서울경제', '한국경제',
            '매일경제', '아시아경제', '헤럴드경제', '비즈니스워치'
        }
    
    def has_content(self, article: Dict[str, Any]) -> bool:
        """
        기사에 본문이 있는지 확인
        
        Args:
            article: 기사 데이터
            
        Returns:
            본문 존재 여부
        """
        content = article.get('content', '').strip()
        
        # 본문이 없거나 빈 문자열인 경우
        if not content:
            return False
        
        # 본문이 너무 짧은 경우 (공백 제거 후)
        clean_content = re.sub(r'\s+', ' ', content).strip()
        if len(clean_content) < 10:  # 10자 미만
            return False
        
        return True
    
    def is_news_agency_article(self, article: Dict[str, Any]) -> bool:
        """
        뉴스통신사업자 인용 기사인지 확인
        - 통신사 직접 기사는 제외 (보존)
        - 다른 언론사에서 통신사 기사를 인용한 경우만 삭제
        
        Args:
            article: 기사 데이터
            
        Returns:
            삭제 대상 인용 기사 여부
        """
        url = article.get('url', '')
        
        # 1. 통신사 직접 기사는 보존 (삭제하지 않음)
        direct_agency_domains = {
            'yna.co.kr': '연합뉴스',
            'newsis.com': '뉴시스', 
            'news1.kr': '뉴스1',
            'ytn.co.kr': 'YTN',
            'kbs.co.kr': 'KBS',
            'mbc.co.kr': 'MBC',
            'sbs.co.kr': 'SBS'
        }
        
        for domain in direct_agency_domains:
            if domain in url:
                return False  # 직접 기사는 삭제하지 않음
        
        # 2. 다른 언론사에서 통신사 기사 인용한 경우만 삭제
        title = article.get('title', '')
        content = article.get('content', '')[:200]  # 좀 더 넓은 범위 확인
        
        # 통신사 이름이 제목이나 본문에 있으면 인용 기사로 판단
        for agency in self.news_agencies:
            if agency in title or agency in content:
                return True  # 인용 기사는 삭제 대상
        
        # 추가 패턴: "연합뉴스=", "뉴시스=", "(연합뉴스)", "[뉴스1]" 등
        citation_patterns = [
            r'연합뉴스\s*=',
            r'뉴시스\s*=', 
            r'뉴스1\s*=',
            r'\(연합뉴스\)',
            r'\(뉴시스\)',
            r'\(뉴스1\)',
            r'\[연합뉴스\]',
            r'\[뉴시스\]',
            r'\[뉴스1\]'
        ]
        
        import re
        full_text = f"{title} {content}"
        for pattern in citation_patterns:
            if re.search(pattern, full_text):
                return True  # 인용 패턴이 있으면 삭제 대상
        
        return False
    
    def is_short_article(self, article: Dict[str, Any]) -> bool:
        """
        짧은 기사인지 확인 (속보성 기사)
        
        Args:
            article: 기사 데이터
            
        Returns:
            짧은 기사 여부
        """
        content = article.get('content', '').strip()
        
        if not content:
            return True
        
        # 문장 수 계산 (마침표, 느낌표, 물음표 기준)
        sentences = re.split(r'[.!?]+', content)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        # 최소 문장 수 미달 AND 최소 길이 미달 (둘 다 만족해야 짧은 기사)
        if len(sentences) < self.min_sentences and len(content) < self.min_content_length:
            return True
        
        # 속보성 키워드 확인
        breaking_keywords = ['속보', '[속보]', '긴급', '[긴급]', '단독', '[단독]']
        title = article.get('title', '')
        
        for keyword in breaking_keywords:
            if keyword in title and len(content) < 200:  # 속보 + 짧은 내용
                return True
        
        return False
    
    def filter_no_content_articles(self, articles: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], int]:
        """
        본문 없는 기사 제거
        
        Args:
            articles: 기사 리스트
            
        Returns:
            (필터링된 기사 리스트, 제거된 기사 수)
        """
        if not articles:
            return articles, 0
        
        filtered_articles = []
        removed_count = 0
        
        for article in articles:
            if self.has_content(article):
                filtered_articles.append(article)
            else:
                removed_count += 1
        
        return filtered_articles, removed_count
    
    def filter_news_agency_articles(self, articles: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], int]:
        """
        뉴스통신사업자 기사 제거 (언론사별 통계 포함)
        
        Args:
            articles: 기사 리스트
            
        Returns:
            (필터링된 기사 리스트, 제거된 기사 수)
        """
        if not articles:
            return articles, 0
        
        filtered_articles = []
        removed_count = 0
        agency_stats = {}  # 언론사별 제거 통계
        
        # 언론사 도메인 매핑
        agency_domains = {
            'yna.co.kr': '연합뉴스',
            'newsis.com': '뉴시스',
            'news1.kr': '뉴스1',
            'ytn.co.kr': 'YTN',
            'kbs.co.kr': 'KBS',
            'mbc.co.kr': 'MBC',
            'sbs.co.kr': 'SBS'
        }
        
        for article in articles:
            if not self.is_news_agency_article(article):
                filtered_articles.append(article)
            else:
                removed_count += 1
                
                # 어떤 언론사인지 확인
                url = article.get('url', '')
                media_outlet = article.get('media_outlet', 'Unknown')
                
                # URL에서 언론사 확인
                agency_found = None
                for domain, agency_name in agency_domains.items():
                    if domain in url:
                        agency_found = agency_name
                        break
                
                # 언론사를 찾지 못했으면 media_outlet 사용
                if not agency_found:
                    agency_found = media_outlet
                
                # 통계 업데이트
                if agency_found not in agency_stats:
                    agency_stats[agency_found] = 0
                agency_stats[agency_found] += 1
        
        # 언론사별 제거 통계 출력
        if agency_stats:
            print("📊 뉴스통신사업자 기사 제거 현황:")
            for agency, count in sorted(agency_stats.items(), key=lambda x: x[1], reverse=True):
                print(f"  - {agency}: {count}개")
        
        return filtered_articles, removed_count
    
    def filter_short_articles(self, articles: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], int]:
        """
        짧은 기사 제거
        
        Args:
            articles: 기사 리스트
            
        Returns:
            (필터링된 기사 리스트, 제거된 기사 수)
        """
        if not articles:
            return articles, 0
        
        filtered_articles = []
        removed_count = 0
        
        for article in articles:
            if not self.is_short_article(article):
                filtered_articles.append(article)
            else:
                removed_count += 1
        
        return filtered_articles, removed_count
    
    def process_basic_filtering(self, articles: List[Dict[str, Any]]) -> BasicFilterResult:
        """
        기본 필터링 전체 프로세스 실행
        
        Args:
            articles: 기사 리스트
            
        Returns:
            기본 필터링 결과
        """
        start_time = datetime.now()
        
        try:
            print("🚀 기본 필터링 프로세스 시작...")
            
            if not articles:
                return BasicFilterResult(
                    total_articles=0,
                    no_content_removed=0,
                    news_agency_removed=0,
                    short_articles_removed=0,
                    final_articles=0,
                    processing_time=0,
                    success=True
                )
            
            # 1. 본문 없는 기사 제거 (비활성화 - 보존)
            print("🔍 본문 없는 기사 확인 중... (보존 모드)")
            articles_after_content = articles  # 제거하지 않고 그대로 유지
            no_content_removed = 0
            print(f"✅ 본문 없는 기사 {no_content_removed}개 제거 (모두 보존)")
            
            # 2. 뉴스통신사업자 인용 기사만 제거
            print("🔍 뉴스통신사업자 인용 기사 제거 중...")
            articles_after_agency, news_agency_removed = self.filter_news_agency_articles(articles_after_content)
            print(f"✅ 뉴스통신사업자 인용 기사 {news_agency_removed}개 제거 완료")
            
            # 3. 짧은 기사 제거 (비활성화 - 보존)
            print("🔍 짧은 기사 확인 중... (보존 모드)")
            articles_after_short = articles_after_agency  # 제거하지 않고 그대로 유지
            short_articles_removed = 0
            print(f"✅ 짧은 기사 {short_articles_removed}개 제거 (모두 보존)")
            
            # 결과 반환
            processing_time = (datetime.now() - start_time).total_seconds()
            
            print(f"📊 기본 필터링 완료: {len(articles)}개 → {len(articles_after_short)}개")
            
            return BasicFilterResult(
                total_articles=len(articles),
                no_content_removed=no_content_removed,
                news_agency_removed=news_agency_removed,
                short_articles_removed=short_articles_removed,
                final_articles=len(articles_after_short),
                processing_time=processing_time,
                success=True
            )
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"기본 필터링 프로세스 실패: {str(e)}"
            print(f"❌ {error_msg}")
            
            return BasicFilterResult(
                total_articles=len(articles) if articles else 0,
                no_content_removed=0,
                news_agency_removed=0,
                short_articles_removed=0,
                final_articles=0,
                processing_time=processing_time,
                success=False,
                error_message=error_msg
            )

# 사용 예시
if __name__ == "__main__":
    # 테스트 데이터
    test_articles = [
        {
            'id': '1',
            'title': '정치 뉴스 1',
            'content': '이것은 정치 뉴스 내용입니다. 매우 중요한 내용이 포함되어 있습니다. 정치권에서는 다양한 의견이 제시되고 있으며, 국민들의 관심이 높아지고 있는 상황입니다.',
            'url': 'https://example.com/news1'
        },
        {
            'id': '2',
            'title': '연합뉴스 - 경제 소식',  # 뉴스통신사업자
            'content': '경제 관련 소식입니다.',
            'url': 'https://yna.co.kr/news2'
        },
        {
            'id': '3',
            'title': '정치 뉴스 3',
            'content': '',  # 본문 없음
            'url': 'https://example.com/news3'
        },
        {
            'id': '4',
            'title': '[속보] 짧은 뉴스',  # 짧은 기사
            'content': '짧은 내용입니다.',
            'url': 'https://example.com/news4'
        }
    ]
    
    # 기본 필터링 실행
    basic_filter = BasicFilter()
    result = basic_filter.process_basic_filtering(test_articles)
    
    # 결과 출력
    print("\n📊 기본 필터링 결과:")
    print(f"  총 기사 수: {result.total_articles}")
    print(f"  본문 없는 기사 제거: {result.no_content_removed}개")
    print(f"  뉴스통신사업자 기사 제거: {result.news_agency_removed}개")
    print(f"  짧은 기사 제거: {result.short_articles_removed}개")
    print(f"  최종 기사 수: {result.final_articles}")
    print(f"  처리 시간: {result.processing_time:.2f}초")
    print(f"  성공 여부: {'✅ 성공' if result.success else '❌ 실패'}")
    
    if result.error_message:
        print(f"  오류 메시지: {result.error_message}")
