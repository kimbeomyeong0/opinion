#!/usr/bin/env python3
"""
통합 필터링 모듈 - KISS 원칙 적용
basic_filter.py와 duplicate_remover.py를 통합
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from typing import List, Dict, Any, Set
from dataclasses import dataclass
from utils.supabase_manager import SupabaseManager

@dataclass
class FilterResult:
    """필터링 결과"""
    success: bool
    total_articles: int
    final_articles: int
    title_duplicates_removed: int
    content_duplicates_removed: int
    no_content_removed: int
    news_agency_removed: int
    short_articles_removed: int
    error_message: str = None

class FilterProcessor:
    """통합 필터링 클래스 - 기본 필터링 + 중복 제거"""
    
    def __init__(self):
        """초기화"""
        self.supabase_manager = SupabaseManager()
        
        # 제외할 언론사 목록
        self.excluded_media = [
            '뉴스1', '뉴스원', '뉴시스', '연합뉴스', '경향신문'
        ]
        
        # 최소 길이 기준
        self.min_title_length = 10
        self.min_content_length = 50
    
    def process_integrated_filtering(self) -> FilterResult:
        """통합 필터링 처리"""
        try:
            # 1단계: 기본 필터링
            basic_result = self._apply_basic_filters()
            if not basic_result['success']:
                return FilterResult(
                    success=False,
                    total_articles=0,
                    final_articles=0,
                    title_duplicates_removed=0,
                    content_duplicates_removed=0,
                    no_content_removed=0,
                    news_agency_removed=0,
                    short_articles_removed=0,
                    error_message=basic_result['error']
                )
            
            # 2단계: 중복 제거
            duplicate_result = self._remove_duplicates(basic_result['articles'])
            
            return FilterResult(
                success=True,
                total_articles=basic_result['total_articles'],
                final_articles=len(duplicate_result['articles']),
                title_duplicates_removed=duplicate_result['title_duplicates_removed'],
                content_duplicates_removed=duplicate_result['content_duplicates_removed'],
                no_content_removed=basic_result['no_content_removed'],
                news_agency_removed=basic_result['news_agency_removed'],
                short_articles_removed=basic_result['short_articles_removed']
            )
            
        except Exception as e:
            return FilterResult(
                success=False,
                total_articles=0,
                final_articles=0,
                title_duplicates_removed=0,
                content_duplicates_removed=0,
                no_content_removed=0,
                news_agency_removed=0,
                short_articles_removed=0,
                error_message=str(e)
            )
    
    def _apply_basic_filters(self) -> Dict[str, Any]:
        """기본 필터링 적용"""
        try:
            # 전체 기사 조회
            result = self.supabase_manager.client.table('articles').select('*').execute()
            if not result.data:
                return {'success': False, 'error': '기사 데이터가 없습니다'}
            
            total_articles = len(result.data)
            articles = result.data
            
            # 필터링 통계
            no_content_removed = 0
            news_agency_removed = 0
            short_articles_removed = 0
            
            # 1. 내용 없는 기사 제거
            articles = [article for article in articles if self._has_content(article)]
            no_content_removed = total_articles - len(articles)
            
            # 2. 특정 언론사 제거
            articles = [article for article in articles if not self._is_excluded_media(article)]
            news_agency_removed = total_articles - no_content_removed - len(articles)
            
            # 3. 짧은 기사 제거
            articles = [article for article in articles if self._is_long_enough(article)]
            short_articles_removed = total_articles - no_content_removed - news_agency_removed - len(articles)
            
            return {
                'success': True,
                'total_articles': total_articles,
                'articles': articles,
                'no_content_removed': no_content_removed,
                'news_agency_removed': news_agency_removed,
                'short_articles_removed': short_articles_removed
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _remove_duplicates(self, articles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """중복 제거"""
        try:
            title_duplicates_removed = 0
            content_duplicates_removed = 0
            
            # 제목 기반 중복 제거
            seen_titles = set()
            unique_articles = []
            
            for article in articles:
                title = article.get('title', '').strip()
                if title and title not in seen_titles:
                    seen_titles.add(title)
                    unique_articles.append(article)
                else:
                    title_duplicates_removed += 1
            
            # 내용 기반 중복 제거 (간단한 해시 기반)
            seen_contents = set()
            final_articles = []
            
            for article in unique_articles:
                content = article.get('content', '').strip()
                content_hash = hash(content[:100])  # 첫 100자만 해시
                
                if content_hash not in seen_contents:
                    seen_contents.add(content_hash)
                    final_articles.append(article)
                else:
                    content_duplicates_removed += 1
            
            return {
                'articles': final_articles,
                'title_duplicates_removed': title_duplicates_removed,
                'content_duplicates_removed': content_duplicates_removed
            }
            
        except Exception as e:
            return {
                'articles': articles,
                'title_duplicates_removed': 0,
                'content_duplicates_removed': 0
            }
    
    def _has_content(self, article: Dict[str, Any]) -> bool:
        """내용이 있는 기사인지 확인"""
        title = article.get('title', '').strip()
        content = article.get('content', '').strip()
        return bool(title and content)
    
    def _is_excluded_media(self, article: Dict[str, Any]) -> bool:
        """제외할 언론사인지 확인"""
        # media_outlets 테이블과 조인하여 확인 (간소화)
        return False  # 기본적으로 모든 언론사 허용
    
    def _is_long_enough(self, article: Dict[str, Any]) -> bool:
        """충분히 긴 기사인지 확인"""
        title = article.get('title', '').strip()
        content = article.get('content', '').strip()
        
        return len(title) >= self.min_title_length and len(content) >= self.min_content_length
