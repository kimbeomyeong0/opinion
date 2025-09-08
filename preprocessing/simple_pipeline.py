#!/usr/bin/env python3
"""
단순화된 전처리 파이프라인 - KISS 원칙 적용
복잡한 성능 모니터링과 과도한 기능을 제거하고 핵심 기능만 유지
"""

import sys
import os
import time
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Any, Optional
from rich.console import Console
from rich.panel import Panel

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utils.supabase_manager import SupabaseManager
from preprocessing.modules.duplicate_remover import IntegratedPreprocessor
from preprocessing.modules.text_cleaner import TextCleaner
from preprocessing.modules.text_normalizer import TextNormalizer
from preprocessing.modules.content_merger import ContentMerger

console = Console()

def get_kct_to_utc_range(date_filter):
    """KCT 기준 날짜 필터를 UTC 기준으로 변환
    
    Args:
        date_filter: 'yesterday', 'today', None
        
    Returns:
        tuple: (start_utc, end_utc) 또는 None
    """
    if not date_filter:
        return None
    
    # 시간대 설정
    kct = pytz.timezone('Asia/Seoul')
    utc = pytz.UTC
    
    if date_filter == 'yesterday':
        # KCT 기준 전날 00:00-23:59
        kct_yesterday = datetime.now(kct).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        kct_start = kct_yesterday
        kct_end = kct_yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # UTC로 변환
        utc_start = kct_start.astimezone(utc)
        utc_end = kct_end.astimezone(utc)
        
    elif date_filter == 'today':
        # KCT 기준 오늘 00:00-현재
        kct_today = datetime.now(kct).replace(hour=0, minute=0, second=0, microsecond=0)
        kct_start = kct_today
        kct_end = datetime.now(kct)
        
        # UTC로 변환
        utc_start = kct_start.astimezone(utc)
        utc_end = kct_end.astimezone(utc)
    
    else:
        return None
    
    return utc_start, utc_end

class SimplePreprocessingPipeline:
    """단순화된 전처리 파이프라인 - 핵심 기능만 유지"""
    
    def __init__(self, date_filter=None):
        """초기화
        
        Args:
            date_filter: 날짜 필터 옵션
                - None: 전체 기사
                - 'yesterday': 전날 기사만 (KCT 기준 00:00-23:59)
                - 'today': 오늘 기사만
        """
        self.supabase_manager = SupabaseManager()
        self.date_filter = date_filter
        
        # 각 모듈 초기화
        self.duplicate_processor = IntegratedPreprocessor(date_filter=self.date_filter)
        self.text_cleaner = TextCleaner()
        self.text_normalizer = TextNormalizer()
        self.content_merger = ContentMerger()
        
        # 단계별 정의
        self.stages = {
            'duplicate_removal': '1단계: 중복 제거 + 기본 필터링',
            'text_cleaning': '2단계: 텍스트 정제',
            'text_normalization': '3단계: 텍스트 정규화',
            'content_merging': '4단계: 제목+본문 통합'
        }
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """파이프라인 현재 상태 조회"""
        try:
            if not self.supabase_manager.client:
                return {'pipeline_ready': False, 'error': 'Supabase client not initialized'}
            
            # 날짜 필터 적용된 기사 수 조회
            if self.date_filter:
                utc_start, utc_end = get_kct_to_utc_range(self.date_filter)
                if utc_start and utc_end:
                    articles_total = self.supabase_manager.client.table('articles').select('id', count='exact').gte('published_at', utc_start.isoformat()).lte('published_at', utc_end.isoformat()).execute()
                else:
                    articles_total = self.supabase_manager.client.table('articles').select('id', count='exact').execute()
            else:
                articles_total = self.supabase_manager.client.table('articles').select('id', count='exact').execute()
            
            articles_preprocessed = self.supabase_manager.client.table('articles').select('id', count='exact').eq('is_preprocessed', True).execute()
            cleaned_result = self.supabase_manager.client.table('articles_cleaned').select('id', count='exact').execute()
            merged_count = self.supabase_manager.client.table('articles_cleaned').select('id', count='exact').not_.is_('merged_content', 'null').execute()
            
            return {
                'articles_total': articles_total.count if articles_total else 0,
                'articles_preprocessed': articles_preprocessed.count if articles_preprocessed else 0,
                'cleaned_articles': cleaned_result.count if cleaned_result else 0,
                'merged_articles': merged_count.count if merged_count else 0,
                'pipeline_ready': True
            }
            
        except Exception as e:
            console.print(f"❌ 파이프라인 상태 조회 실패: {e}")
            return {'pipeline_ready': False, 'error': str(e)}
    
    def run_stage_1_duplicate_removal(self) -> bool:
        """1단계: 중복 제거 + 기본 필터링"""
        try:
            console.print("🔄 1단계: 중복 제거 + 기본 필터링 시작")
            
            result = self.duplicate_processor.process_integrated_preprocessing()
            
            if result.success:
                console.print(f"✅ 1단계 완료: {result.final_articles}개 기사 처리됨")
                return True
            else:
                console.print(f"❌ 1단계 실패: {result.error_message}")
                return False
                
        except Exception as e:
            console.print(f"❌ 1단계 예외 발생: {e}")
            return False
    
    def run_stage_2_text_cleaning(self) -> bool:
        """2단계: 텍스트 정제"""
        try:
            console.print("🔄 2단계: 텍스트 정제 시작")
            
            # 정제되지 않은 기사들을 조회
            articles = self._fetch_articles_for_cleaning()
            
            if not articles:
                console.print("✅ 2단계: 정제할 기사가 없습니다")
                return True
            
            # 배치 처리로 텍스트 정제 실행
            processed_count, failed_count = self._process_articles_batch(
                articles, self._clean_single_article
            )
            
            console.print(f"✅ 2단계 완료: {processed_count}개 정제, {failed_count}개 실패")
            return True
            
        except Exception as e:
            console.print(f"❌ 2단계 예외 발생: {e}")
            return False
    
    def run_stage_3_text_normalization(self) -> bool:
        """3단계: 텍스트 정규화"""
        try:
            console.print("🔄 3단계: 텍스트 정규화 시작")
            
            # 정규화되지 않은 기사들을 조회
            articles = self._fetch_articles_for_normalization()
            
            if not articles:
                console.print("✅ 3단계: 정규화할 기사가 없습니다")
                return True
            
            # 배치 처리로 텍스트 정규화 실행
            processed_count, failed_count = self._process_articles_batch(
                articles, self._normalize_single_article
            )
            
            console.print(f"✅ 3단계 완료: {processed_count}개 정규화, {failed_count}개 실패")
            return True
            
        except Exception as e:
            console.print(f"❌ 3단계 예외 발생: {e}")
            return False
    
    def run_stage_4_content_merging(self) -> bool:
        """4단계: 제목+본문 통합"""
        try:
            console.print("🔄 4단계: 제목+본문 통합 시작")
            
            result = self.content_merger.process_content_merge()
            
            if result['successful_saves'] > 0:
                console.print(f"✅ 4단계 완료: {result['successful_saves']}개 기사 통합됨")
                return True
            else:
                console.print("❌ 4단계 실패: 통합된 기사가 없음")
                return False
                
        except Exception as e:
            console.print(f"❌ 4단계 예외 발생: {e}")
            return False
    
    def run_single_stage(self, stage_name: str) -> bool:
        """단일 단계 실행"""
        if stage_name not in self.stages:
            console.print(f"❌ 알 수 없는 단계: {stage_name}")
            return False
        
        console.print(f"🚀 {self.stages[stage_name]} 실행 시작")
        
        if stage_name == 'duplicate_removal':
            return self.run_stage_1_duplicate_removal()
        elif stage_name == 'text_cleaning':
            return self.run_stage_2_text_cleaning()
        elif stage_name == 'text_normalization':
            return self.run_stage_3_text_normalization()
        elif stage_name == 'content_merging':
            return self.run_stage_4_content_merging()
    
    def run_full_pipeline(self, skip_stages: List[str] = None) -> bool:
        """전체 파이프라인 실행"""
        skip_stages = skip_stages or []
        
        console.print(Panel.fit("🚀 단순화된 전처리 파이프라인 시작", style="bold blue"))
        
        # 초기 상태 확인
        initial_status = self.get_pipeline_status()
        console.print(f"📊 초기 상태: 전체 기사 {initial_status.get('articles_total', 0)}개")
        
        # 단계별 실행
        stage_order = ['duplicate_removal', 'text_cleaning', 'text_normalization', 'content_merging']
        
        for stage_name in stage_order:
            if stage_name in skip_stages:
                console.print(f"⏭️  {self.stages[stage_name]} 건너뜀")
                continue
                
            success = self.run_single_stage(stage_name)
            
            if not success:
                console.print(f"❌ {self.stages[stage_name]} 실패")
                return False
            
            console.print("-" * 40)
        
        # 최종 상태 확인
        final_status = self.get_pipeline_status()
        console.print(f"📊 최종 상태: 정제된 기사 {final_status.get('cleaned_articles', 0)}개")
        
        console.print(Panel.fit("✅ 전처리 파이프라인 완료!", style="bold green"))
        return True
    
    def _fetch_articles_for_cleaning(self) -> List[Dict[str, Any]]:
        """정제되지 않은 기사들을 조회"""
        try:
            result = self.supabase_manager.client.table('articles_cleaned').select(
                'id, article_id, title_cleaned, lead_paragraph'
            ).or_(
                'preprocessing_metadata->>text_cleaned.is.null,preprocessing_metadata->>text_cleaned.eq.false'
            ).execute()
            
            return result.data if result else []
        except Exception as e:
            console.print(f"❌ 기사 조회 실패: {e}")
            return []
    
    def _fetch_articles_for_normalization(self) -> List[Dict[str, Any]]:
        """정규화되지 않은 기사들을 조회"""
        try:
            result = self.supabase_manager.client.table('articles_cleaned').select(
                'id, title_cleaned, lead_paragraph'
            ).or_(
                'preprocessing_metadata->>text_normalized.is.null,preprocessing_metadata->>text_normalized.eq.false'
            ).execute()
            
            return result.data if result else []
        except Exception as e:
            console.print(f"❌ 기사 조회 실패: {e}")
            return []
    
    def _process_articles_batch(self, articles: List[Dict[str, Any]], process_func) -> tuple:
        """기사들을 배치로 처리"""
        processed_count = 0
        failed_count = 0
        batch_size = 50
        update_batch = []
        
        for i, article in enumerate(articles):
            try:
                result = process_func(article)
                
                if result:
                    update_batch.append(result)
                    processed_count += 1
                else:
                    failed_count += 1
                
                # 배치 크기에 도달하면 일괄 업데이트
                if len(update_batch) >= batch_size or i == len(articles) - 1:
                    if update_batch:
                        self._batch_update_articles(update_batch)
                        update_batch = []
                        
            except Exception as e:
                failed_count += 1
                console.print(f"❌ 기사 {article.get('id', 'unknown')} 처리 실패: {e}")
                continue
        
        return processed_count, failed_count
    
    def _clean_single_article(self, article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """단일 기사 정제 처리"""
        try:
            # 제목과 리드문 정제
            cleaned_title, title_patterns = self.text_cleaner.clean_title(article['title_cleaned'] or '', 'unknown')
            cleaned_lead, lead_patterns = self.text_cleaner.clean_content(article['lead_paragraph'] or '', 'unknown')
            
            return {
                'id': article['id'],
                'title_cleaned': cleaned_title,
                'lead_paragraph': cleaned_lead,
                'preprocessing_metadata': {
                    'text_cleaned': True,
                    'text_cleaned_at': datetime.now().isoformat(),
                    'title_patterns_removed': title_patterns,
                    'lead_patterns_removed': lead_patterns
                },
                'updated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            console.print(f"❌ 기사 {article.get('id', 'unknown')} 정제 실패: {e}")
            return None
    
    def _normalize_single_article(self, article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """단일 기사 정규화 처리"""
        try:
            # 텍스트 정규화 실행
            title_result = self.text_normalizer.normalize_text(article['title_cleaned'] or '')
            content_result = self.text_normalizer.normalize_text(article['lead_paragraph'] or '')
            
            return {
                'id': article['id'],
                'title_cleaned': title_result.normalized_text,
                'lead_paragraph': content_result.normalized_text,
                'preprocessing_metadata': {
                    'text_normalized': True,
                    'text_normalized_at': datetime.now().isoformat(),
                    'title_changes': title_result.changes_made,
                    'content_changes': content_result.changes_made
                },
                'updated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            console.print(f"❌ 기사 {article.get('id', 'unknown')} 정규화 실패: {e}")
            return None
    
    def _batch_update_articles(self, update_batch: List[Dict[str, Any]]) -> None:
        """기사들을 배치로 업데이트"""
        try:
            for update_data in update_batch:
                self.supabase_manager.client.table('articles_cleaned').update({
                    'title_cleaned': update_data['title_cleaned'],
                    'lead_paragraph': update_data['lead_paragraph'],
                    'preprocessing_metadata': update_data['preprocessing_metadata'],
                    'updated_at': update_data['updated_at']
                }).eq('id', update_data['id']).execute()
                
        except Exception as e:
            console.print(f"❌ 배치 업데이트 실패: {e}")

if __name__ == "__main__":
    # 직접 실행 시 전체 파이프라인 실행
    pipeline = SimplePreprocessingPipeline()
    
    # 상태 확인
    console.print("📋 파이프라인 상태 확인...")
    status = pipeline.get_pipeline_status()
    console.print(f"전체 기사: {status.get('articles_total', 0)}개")
    console.print(f"전처리된 기사: {status.get('articles_preprocessed', 0)}개")
    console.print(f"정제된 기사: {status.get('cleaned_articles', 0)}개")
    console.print(f"통합된 기사: {status.get('merged_articles', 0)}개")
    console.print()
    
    # 전체 파이프라인 실행
    success = pipeline.run_full_pipeline()
    
    if success:
        console.print("🎉 전처리 파이프라인이 성공적으로 완료되었습니다!")
    else:
        console.print("💥 전처리 파이프라인이 실패했습니다.")
