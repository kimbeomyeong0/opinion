#!/usr/bin/env python3
"""
제목+본문 통합 모듈
- title_cleaned와 content_cleaned를 합쳐서 merged_content 생성
- 임베딩에 최적화된 텍스트 생성
"""

import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import logging
import sys
import os

# 상위 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.supabase_manager import SupabaseManager

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class MergeResult:
    """제목+본문 통합 결과"""
    article_id: str
    original_title: str
    original_content: str
    merged_content: str
    merge_strategy: str
    merge_metadata: Dict[str, Any]
    success: bool
    error_message: Optional[str] = None

class ContentMerger:
    """제목+본문 통합 클래스"""
    
    def __init__(self):
        """제목+본문 통합기 초기화"""
        self.supabase_manager = SupabaseManager()
        
    def merge_title_content(self, title: str, content: str) -> Tuple[str, str]:
        """제목과 본문을 통합"""
        title = title.strip() if title else ""
        content = content.strip() if content else ""
        
        if title and content:
            # 제목 끝에 마침표가 없으면 추가
            if not title.endswith(('.', '!', '?', ':', ';')):
                title += '.'
            
            merged = f"{title} {content}"
            strategy = "title_and_content"
            
        elif title:
            merged = title
            strategy = "title_only"
            
        elif content:
            merged = content
            strategy = "content_only"
            
        else:
            merged = ""
            strategy = "empty"
        
        return merged, strategy
    
    def calculate_merge_statistics(self, title: str, content: str, merged: str) -> Dict[str, Any]:
        """통합 통계 계산"""
        title_len = len(title) if title else 0
        content_len = len(content) if content else 0
        merged_len = len(merged)
        
        title_words = len(title.split()) if title else 0
        content_words = len(content.split()) if content else 0
        merged_words = len(merged.split()) if merged else 0
        
        return {
            'title_length': title_len,
            'content_length': content_len,
            'merged_length': merged_len,
            'title_words': title_words,
            'content_words': content_words,
            'merged_words': merged_words,
            'length_increase_ratio': (merged_len / content_len) if content_len > 0 else float('inf') if title_len > 0 else 1.0
        }
    
    def merge_single_article(self, article: Dict[str, Any]) -> MergeResult:
        """단일 기사 제목+본문 통합"""
        try:
            article_id = article.get('id', '')
            title = article.get('title_cleaned', '')
            content = article.get('content_cleaned', '')
            
            # 제목+본문 통합
            merged_content, merge_strategy = self.merge_title_content(title, content)
            
            # 통계 계산
            merge_metadata = self.calculate_merge_statistics(title, content, merged_content)
            merge_metadata['merge_strategy'] = merge_strategy
            
            return MergeResult(
                article_id=article_id,
                original_title=title,
                original_content=content,
                merged_content=merged_content,
                merge_strategy=merge_strategy,
                merge_metadata=merge_metadata,
                success=True
            )
            
        except Exception as e:
            logger.error(f"기사 통합 중 오류 발생: {str(e)}")
            return MergeResult(
                article_id=article.get('id', ''),
                original_title=article.get('title_cleaned', ''),
                original_content=article.get('content_cleaned', ''),
                merged_content='',
                merge_strategy='error',
                merge_metadata={},
                success=False,
                error_message=str(e)
            )
    
    def fetch_articles_for_merge(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """통합할 기사들 조회 (merged_content가 비어있는 것들) - 페이지네이션 적용"""
        try:
            articles = []
            page_size = 1000
            offset = 0
            
            while True:
                query = self.supabase_manager.client.table('articles_cleaned').select(
                    'id, title_cleaned, content_cleaned, merged_content'
                )
                
                # merged_content가 NULL이거나 빈 문자열인 경우만 조회
                query = query.or_('merged_content.is.null,merged_content.eq.')
                query = query.range(offset, offset + page_size - 1)
                
                result = query.execute()
                page_data = result.data if result else []
                
                if not page_data:
                    break
                    
                articles.extend(page_data)
                
                # limit이 설정된 경우 제한 확인
                if limit and len(articles) >= limit:
                    articles = articles[:limit]
                    break
                
                if len(page_data) < page_size:
                    break
                    
                offset += page_size
            
            if articles:
                logger.info(f"통합 대상 기사 {len(articles)}개 조회 완료")
                return articles
            else:
                logger.info("통합할 기사가 없습니다")
                return []
                
        except Exception as e:
            logger.error(f"기사 조회 중 오류: {str(e)}")
            return []
    
    def save_merged_content(self, merge_results: List[MergeResult]) -> int:
        """통합 결과를 데이터베이스에 저장"""
        successful_saves = 0
        
        for result in merge_results:
            if not result.success:
                continue
                
            try:
                # merged_content 업데이트
                update_result = self.supabase_manager.client.table('articles_cleaned').update({
                    'merged_content': result.merged_content,
                    'preprocessing_metadata': result.merge_metadata,
                    'updated_at': 'now()'
                }).eq('id', result.article_id).execute()
                
                if update_result.data:
                    successful_saves += 1
                    logger.debug(f"기사 {result.article_id} 통합 결과 저장 완료")
                else:
                    logger.warning(f"기사 {result.article_id} 저장 실패")
                    
            except Exception as e:
                logger.error(f"기사 {result.article_id} 저장 중 오류: {str(e)}")
        
        logger.info(f"총 {successful_saves}개 기사 통합 결과 저장 완료")
        return successful_saves
    
    def process_content_merge(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """전체 제목+본문 통합 프로세스"""
        import time
        start_time = time.time()
        
        try:
            # 1. 통합할 기사들 조회
            articles = self.fetch_articles_for_merge(limit)
            
            if not articles:
                return {
                    'total_articles': 0,
                    'successful_merges': 0,
                    'failed_merges': 0,
                    'processing_time': 0,
                    'merge_strategies': {},
                    'success': True,
                    'message': '통합할 기사가 없습니다'
                }
            
            # 2. 각 기사별 통합 수행
            merge_results = []
            for article in articles:
                result = self.merge_single_article(article)
                merge_results.append(result)
            
            # 3. 결과 저장
            successful_saves = self.save_merged_content(merge_results)
            
            # 4. 통계 계산
            successful_merges = len([r for r in merge_results if r.success])
            failed_merges = len([r for r in merge_results if not r.success])
            
            # 통합 전략별 통계
            merge_strategies = {}
            for result in merge_results:
                if result.success:
                    strategy = result.merge_strategy
                    merge_strategies[strategy] = merge_strategies.get(strategy, 0) + 1
            
            processing_time = time.time() - start_time
            
            return {
                'total_articles': len(articles),
                'successful_merges': successful_merges,
                'failed_merges': failed_merges,
                'successful_saves': successful_saves,
                'processing_time': processing_time,
                'merge_strategies': merge_strategies,
                'success': True,
                'message': f'{successful_saves}개 기사 통합 완료'
            }
            
        except Exception as e:
            logger.error(f"통합 프로세스 중 오류: {str(e)}")
            return {
                'total_articles': 0,
                'successful_merges': 0,
                'failed_merges': 0,
                'processing_time': time.time() - start_time,
                'merge_strategies': {},
                'success': False,
                'error_message': str(e)
            }
    
    def get_merge_statistics(self) -> Dict[str, Any]:
        """현재 통합 상태 통계"""
        try:
            # 전체 기사 수
            total_result = self.supabase_manager.client.table('articles_cleaned').select('id', count='exact').execute()
            total_articles = total_result.count if total_result.count else 0
            
            # 통합 완료된 기사 수
            merged_result = self.supabase_manager.client.table('articles_cleaned').select('id', count='exact').not_.is_('merged_content', 'null').neq('merged_content', '').execute()
            merged_articles = merged_result.count if merged_result.count else 0
            
            # 통합 대기 중인 기사 수
            pending_articles = total_articles - merged_articles
            
            return {
                'total_articles': total_articles,
                'merged_articles': merged_articles,
                'pending_articles': pending_articles,
                'merge_completion_rate': (merged_articles / total_articles * 100) if total_articles > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"통계 조회 중 오류: {str(e)}")
            return {
                'total_articles': 0,
                'merged_articles': 0,
                'pending_articles': 0,
                'merge_completion_rate': 0,
                'error': str(e)
            }

if __name__ == "__main__":
    # 테스트용 코드
    merger = ContentMerger()
    
    # 현재 상태 확인
    print("=== 제목+본문 통합 현황 ===")
    stats = merger.get_merge_statistics()
    print(f"전체 기사: {stats['total_articles']}개")
    print(f"통합 완료: {stats['merged_articles']}개")
    print(f"통합 대기: {stats['pending_articles']}개")
    print(f"완료율: {stats['merge_completion_rate']:.1f}%")
    
    # 샘플 통합 테스트 (5개만)
    if stats['pending_articles'] > 0:
        print(f"\n=== 샘플 통합 테스트 (5개) ===")
        result = merger.process_content_merge(limit=5)
        
        print(f"처리 결과: {result['message']}")
        print(f"성공: {result['successful_merges']}개")
        print(f"실패: {result['failed_merges']}개")
        print(f"처리 시간: {result['processing_time']:.2f}초")
        print(f"통합 전략: {result['merge_strategies']}")
