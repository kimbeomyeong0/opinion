#!/usr/bin/env python3
"""
통합 내용 처리 모듈 - KISS 원칙 적용
content_merger.py와 lead_extractor.py를 통합
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from utils.supabase_manager import SupabaseManager

@dataclass
class ContentMergeResult:
    """내용 통합 결과"""
    successful_saves: int
    failed_saves: int
    total_articles: int
    successful_merges: int
    failed_merges: int
    merge_strategies: Dict[str, int]

class ContentProcessor:
    """통합 내용 처리 클래스 - 리드문 추출 + 내용 통합"""
    
    def __init__(self):
        """초기화"""
        self.supabase_manager = SupabaseManager()
        
        # 통합 전략 정의
        self.merge_strategies = {
            'title_only': 0,
            'lead_only': 0,
            'title_lead': 0,
            'full_content': 0
        }
    
    def process_content_merge(self) -> ContentMergeResult:
        """내용 통합 처리"""
        try:
            # 통합할 기사들 조회
            articles = self._fetch_articles_for_merge()
            
            if not articles:
                return ContentMergeResult(
                    successful_saves=0,
                    failed_saves=0,
                    total_articles=0,
                    successful_merges=0,
                    failed_merges=0,
                    merge_strategies=self.merge_strategies
                )
            
            successful_saves = 0
            failed_saves = 0
            successful_merges = 0
            failed_merges = 0
            
            for article in articles:
                try:
                    # 리드문 추출
                    lead_paragraph = self._extract_lead_paragraph(article)
                    
                    # 내용 통합
                    merged_content = self._merge_content(article, lead_paragraph)
                    
                    if merged_content:
                        # 데이터베이스에 저장
                        success = self._save_merged_content(article['id'], merged_content, lead_paragraph)
                        
                        if success:
                            successful_saves += 1
                            successful_merges += 1
                        else:
                            failed_saves += 1
                            failed_merges += 1
                    else:
                        failed_saves += 1
                        failed_merges += 1
                        
                except Exception as e:
                    failed_saves += 1
                    failed_merges += 1
                    continue
            
            return ContentMergeResult(
                successful_saves=successful_saves,
                failed_saves=failed_saves,
                total_articles=len(articles),
                successful_merges=successful_merges,
                failed_merges=failed_merges,
                merge_strategies=self.merge_strategies
            )
            
        except Exception as e:
            return ContentMergeResult(
                successful_saves=0,
                failed_saves=0,
                total_articles=0,
                successful_merges=0,
                failed_merges=0,
                merge_strategies=self.merge_strategies
            )
    
    def _fetch_articles_for_merge(self) -> List[Dict[str, Any]]:
        """통합할 기사들 조회 (articles_cleaned 테이블의 모든 기사)"""
        try:
            result = self.supabase_manager.client.table('articles_cleaned').select(
                'id, title_cleaned, lead_paragraph'
            ).is_('merged_content', 'null').execute()
            
            print(f"📅 content_processor: articles_cleaned 테이블의 모든 기사 처리")
            return result.data if result else []
        except Exception as e:
            print(f"❌ 기사 조회 실패: {str(e)}")
            return []
    
    def _extract_lead_paragraph(self, article: Dict[str, Any]) -> str:
        """리드문 추출"""
        # 기존 lead_paragraph가 있으면 사용
        lead_paragraph = article.get('lead_paragraph', '').strip()
        
        if lead_paragraph:
            return lead_paragraph
        
        # lead_paragraph가 없으면 title_cleaned를 리드문으로 사용
        title_cleaned = article.get('title_cleaned', '').strip()
        if title_cleaned:
            return title_cleaned
        
        # 둘 다 없으면 빈 문자열
        return ''
    
    def _merge_content(self, article: Dict[str, Any], lead_paragraph: str) -> Optional[str]:
        """내용 통합"""
        title_cleaned = article.get('title_cleaned', '').strip()
        lead = lead_paragraph.strip()
        
        if not title_cleaned and not lead:
            return None
        
        # 통합 전략 결정
        if title_cleaned and lead:
            merged = f"{title_cleaned}\n\n{lead}"
            self.merge_strategies['title_lead'] += 1
        elif title_cleaned:
            merged = title_cleaned
            self.merge_strategies['title_only'] += 1
        elif lead:
            merged = lead
            self.merge_strategies['lead_only'] += 1
        else:
            return None
        
        return merged
    
    def _save_merged_content(self, article_id: str, merged_content: str, lead_paragraph: str) -> bool:
        """통합된 내용 저장"""
        try:
            result = self.supabase_manager.client.table('articles_cleaned').update({
                'merged_content': merged_content,
                'lead_paragraph': lead_paragraph,
                'updated_at': 'now()'
            }).eq('id', article_id).execute()
            
            return bool(result.data)
        except Exception as e:
            return False
