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
                        success = self._save_merged_content(article, merged_content)
                        
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
                    print(f"❌ 기사 처리 실패 (ID: {article.get('id', 'unknown')}): {str(e)}")
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
        """통합할 기사들 조회 (articles 테이블에서 articles_cleaned에 없는 기사들) - 페이지네이션 적용"""
        try:
            # 먼저 이미 전처리된 기사 ID들을 가져옴
            processed_result = self.supabase_manager.client.table('articles_cleaned').select('article_id').execute()
            processed_ids = set(item['article_id'] for item in processed_result.data)
            
            # 페이지네이션을 위한 변수들
            all_articles = []
            page_size = 1000  # Supabase 제한
            offset = 0
            
            print(f"📅 content_processor: articles 테이블의 전처리 대기 기사 처리 (페이지네이션 적용)")
            
            while True:
                # articles 테이블에서 페이지별로 기사 조회
                result = self.supabase_manager.client.table('articles').select(
                    'id, title, content, media_id, published_at'
                ).range(offset, offset + page_size - 1).execute()
                
                if not result.data:
                    break  # 더 이상 데이터가 없으면 종료
                
                # 이미 전처리된 기사 제외
                new_articles = [article for article in result.data if article['id'] not in processed_ids]
                all_articles.extend(new_articles)
                
                print(f"  - 페이지 {offset//page_size + 1}: {len(result.data)}개 조회, {len(new_articles)}개 신규 (총 {len(all_articles)}개)")
                
                # 마지막 페이지인 경우 (조회된 데이터가 page_size보다 적으면)
                if len(result.data) < page_size:
                    break
                
                offset += page_size
            
            print(f"✅ 총 {len(all_articles)}개 신규 기사 조회 완료")
            return all_articles
            
        except Exception as e:
            print(f"❌ 기사 조회 실패: {str(e)}")
            return []
    
    def _extract_lead_paragraph(self, article: Dict[str, Any]) -> str:
        """리드문 추출 - content에서 첫 번째 문단 추출"""
        content = article.get('content', '').strip()
        
        if not content:
            return ''
        
        # 첫 번째 문단 추출 (줄바꿈으로 구분)
        paragraphs = content.split('\n')
        first_paragraph = paragraphs[0].strip() if paragraphs else ''
        
        # 첫 번째 문단이 너무 짧으면 처음 200자 사용
        if len(first_paragraph) < 50:
            return content[:200].strip()
        
        return first_paragraph
    
    def _merge_content(self, article: Dict[str, Any], lead_paragraph: str) -> Optional[str]:
        """내용 통합 - title + lead만 통합 (기사 본문 제외)"""
        title = article.get('title', '').strip()
        lead = lead_paragraph.strip()
        
        if not title and not lead:
            return None
        
        # 통합 전략 결정 (제목 + 리드만)
        merged_parts = []
        
        if title:
            merged_parts.append(f"제목: {title}")
            self.merge_strategies['title_only'] += 1
        
        if lead:
            merged_parts.append(f"리드: {lead}")
            self.merge_strategies['lead_only'] += 1
        
        if len(merged_parts) > 1:
            self.merge_strategies['title_lead'] += 1
        
        return '\n\n'.join(merged_parts)
    
    def _save_merged_content(self, article: Dict[str, Any], merged_content: str) -> bool:
        """통합된 내용을 articles_cleaned 테이블에 저장"""
        try:
            # articles_cleaned 테이블에 새 레코드 생성
            data = {
                'article_id': article['id'],
                'merged_content': merged_content,
                'media_id': article['media_id'],
                'published_at': article['published_at']
            }
            
            result = self.supabase_manager.client.table('articles_cleaned').insert(data).execute()
            
            if result.data:
                # articles 테이블에는 preprocessing_status 컬럼이 없으므로 업데이트 불필요
                return True
            
            return False
        except Exception as e:
            print(f"❌ 저장 실패: {str(e)}")
            return False
