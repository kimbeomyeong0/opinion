#!/usr/bin/env python3
"""
종합 전처리 파이프라인
- 모든 전처리 단계를 순차적으로 실행
- 단계별 실행 및 전체 실행 지원
- 진행 상황 추적 및 오류 처리
"""

import sys
import os
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass
import logging

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utils.supabase_manager import SupabaseManager
from preprocessing.modules.duplicate_remover import IntegratedPreprocessor
from preprocessing.modules.text_cleaner import TextCleaner
from preprocessing.modules.text_normalizer import TextNormalizer
from preprocessing.modules.content_merger import ContentMerger

# 로깅 설정
def setup_logging(verbose: bool = False):
    """구조화된 로깅 설정"""
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # 로그 포맷터 설정
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # 기존 핸들러 제거
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 콘솔 핸들러 추가
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # 파일 핸들러 추가 (선택적)
    try:
        file_handler = logging.FileHandler('preprocessing.log', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    except Exception as e:
        print(f"⚠️  로그 파일 생성 실패: {e}")

# 기본 로깅 설정
setup_logging()
logger = logging.getLogger(__name__)

@dataclass
class PipelineResult:
    """파이프라인 실행 결과"""
    stage: str
    success: bool
    total_articles: int
    processed_articles: int
    processing_time: float
    message: str
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    memory_usage: Optional[float] = None  # MB
    throughput: Optional[float] = None  # articles/second

@dataclass
class FullPipelineResult:
    """전체 파이프라인 실행 결과"""
    total_execution_time: float
    stages_completed: List[str]
    stages_failed: List[str]
    final_article_count: int
    stage_results: Dict[str, PipelineResult]
    overall_success: bool

class PreprocessingPipeline:
    """종합 전처리 파이프라인 클래스"""
    
    # 상수 정의
    DEFAULT_PAGE_SIZE = 1000
    PROGRESS_LOG_INTERVAL = 100
    BATCH_SIZE = 50
    
    def __init__(self, verbose: bool = False):
        """파이프라인 초기화"""
        # 로깅 설정
        setup_logging(verbose)
        self.logger = logging.getLogger(__name__)
        
        self.supabase_manager = SupabaseManager()
        
        # 각 모듈 초기화
        self.duplicate_processor = IntegratedPreprocessor()
        self.text_cleaner = TextCleaner()
        self.text_normalizer = TextNormalizer()
        self.content_merger = ContentMerger()
        
        # 리드문 추출기 초기화 (성능 최적화)
        from preprocessing.modules.lead_extractor import LeadExtractor
        self.lead_extractor = LeadExtractor()
        
        # 단계별 정의
        self.stages = {
            'duplicate_removal': '1단계: 중복 제거 + 기본 필터링',
            'text_cleaning': '2단계: 텍스트 정제',
            'text_normalization': '3단계: 텍스트 정규화',
            'content_merging': '4단계: 제목+본문 통합'
        }
        
        # 성능 모니터링
        self.performance_metrics = {}
    
    def _get_memory_usage(self) -> float:
        """현재 메모리 사용량 조회 (MB)"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024  # MB
        except ImportError:
            # psutil이 없는 경우 대략적 추정
            import sys
            return sys.getsizeof(self) / 1024 / 1024
        except Exception:
            return 0.0
    
    def _calculate_throughput(self, processed_articles: int, processing_time: float) -> float:
        """처리량 계산 (articles/second)"""
        if processing_time > 0:
            return processed_articles / processing_time
        return 0.0
    
    def _log_performance_metrics(self, stage: str, result: PipelineResult):
        """성능 메트릭 로깅"""
        self.performance_metrics[stage] = {
            'processing_time': result.processing_time,
            'processed_articles': result.processed_articles,
            'throughput': result.throughput,
            'memory_usage': result.memory_usage,
            'success': result.success
        }
        
        self.logger.info(f"📊 {stage} 성능 메트릭:")
        self.logger.info(f"  ⏱️  처리 시간: {result.processing_time:.2f}초")
        self.logger.info(f"  📈 처리량: {result.throughput:.2f} articles/sec")
        self.logger.info(f"  💾 메모리 사용량: {result.memory_usage:.2f}MB")
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """파이프라인 현재 상태 조회"""
        try:
            if not self.supabase_manager.client:
                return {'pipeline_ready': False, 'error': 'Supabase client not initialized'}
            
            # 간단한 카운트 조회
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
            self.logger.error(f"파이프라인 상태 조회 실패: {e}")
            return {'pipeline_ready': False, 'error': str(e)}
    
    def run_stage_1_duplicate_removal(self) -> PipelineResult:
        """1단계: 중복 제거 + 기본 필터링"""
        start_time = time.time()
        stage = 'duplicate_removal'
        
        try:
            self.logger.info("🔄 1단계: 중복 제거 + 기본 필터링 시작")
            
            # 통합 전처리 실행
            result = self.duplicate_processor.process_integrated_preprocessing()
            
            processing_time = time.time() - start_time
            
            if result.success:
                message = f"✅ 1단계 완료: {result.final_articles}개 기사 처리됨"
                self.logger.info(message)
                
                return PipelineResult(
                    stage=stage,
                    success=True,
                    total_articles=result.total_articles,
                    processed_articles=result.final_articles,
                    processing_time=processing_time,
                    message=message,
                    metadata={
                        'title_duplicates_removed': result.title_duplicates_removed,
                        'content_duplicates_removed': result.content_duplicates_removed,
                        'no_content_removed': result.no_content_removed,
                        'news_agency_removed': result.news_agency_removed,
                        'short_articles_removed': result.short_articles_removed
                    }
                )
            else:
                error_msg = f"❌ 1단계 실패: {result.error_message}"
                self.logger.error(error_msg)
                
                return PipelineResult(
                    stage=stage,
                    success=False,
                    total_articles=0,
                    processed_articles=0,
                    processing_time=processing_time,
                    message=error_msg,
                    error_message=result.error_message
                )
                
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"❌ 1단계 예외 발생: {e}"
            self.logger.error(error_msg)
            
            return PipelineResult(
                stage=stage,
                success=False,
                total_articles=0,
                processed_articles=0,
                processing_time=processing_time,
                message=error_msg,
                error_message=str(e)
            )
    
    def run_stage_2_text_cleaning(self) -> PipelineResult:
        """2단계: 텍스트 정제 (최적화된 배치 처리)"""
        start_time = time.time()
        stage = 'text_cleaning'
        
        try:
            self.logger.info("🔄 2단계: 텍스트 정제 시작")
            
            # 1. 언론사 정보를 미리 조회하여 캐시
            media_cache = self._build_media_cache()
            
            # 2. 정제되지 않은 기사들을 배치로 조회
            articles = self._fetch_articles_for_cleaning()
            
            if not articles:
                message = "✅ 2단계: 정제할 기사가 없습니다"
                self.logger.info(message)
                
                return PipelineResult(
                    stage=stage,
                    success=True,
                    total_articles=0,
                    processed_articles=0,
                    processing_time=time.time() - start_time,
                    message=message
                )
            
            # 3. 배치 처리로 텍스트 정제 실행
            processed_count, failed_count = self._process_articles_batch(
                articles, media_cache, self._clean_single_article
            )
            
            processing_time = time.time() - start_time
            message = f"✅ 2단계 완료: {processed_count}개 정제, {failed_count}개 실패"
            self.logger.info(message)
            
            # 성능 메트릭 계산
            memory_usage = self._get_memory_usage()
            throughput = self._calculate_throughput(processed_count, processing_time)
            
            result = PipelineResult(
                stage=stage,
                success=True,
                total_articles=len(articles),
                processed_articles=processed_count,
                processing_time=processing_time,
                message=message,
                metadata={'failed_count': failed_count},
                memory_usage=memory_usage,
                throughput=throughput
            )
            
            # 성능 메트릭 로깅
            self._log_performance_metrics(stage, result)
            
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"❌ 2단계 예외 발생: {e}"
            self.logger.error(error_msg)
            
            return PipelineResult(
                stage=stage,
                success=False,
                total_articles=0,
                processed_articles=0,
                processing_time=processing_time,
                message=error_msg,
                error_message=str(e)
            )
    
    def run_stage_3_text_normalization(self) -> PipelineResult:
        """3단계: 텍스트 정규화 (최적화된 배치 처리)"""
        start_time = time.time()
        stage = 'text_normalization'
        
        try:
            self.logger.info("🔄 3단계: 텍스트 정규화 시작")
            
            # 정규화되지 않은 기사들을 배치로 조회
            articles = self._fetch_articles_for_normalization()
            
            if not articles:
                message = "✅ 3단계: 정규화할 기사가 없습니다"
                self.logger.info(message)
                
                return PipelineResult(
                    stage=stage,
                    success=True,
                    total_articles=0,
                    processed_articles=0,
                    processing_time=time.time() - start_time,
                    message=message
                )
            
            # 배치 처리로 텍스트 정규화 실행
            processed_count, failed_count = self._process_articles_batch(
                articles, {}, self._normalize_single_article
            )
            
            processing_time = time.time() - start_time
            message = f"✅ 3단계 완료: {processed_count}개 정규화, {failed_count}개 실패"
            self.logger.info(message)
            
            return PipelineResult(
                stage=stage,
                success=True,
                total_articles=len(articles),
                processed_articles=processed_count,
                processing_time=processing_time,
                message=message,
                metadata={'failed_count': failed_count}
            )
            
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"❌ 3단계 예외 발생: {e}"
            self.logger.error(error_msg)
            
            return PipelineResult(
                stage=stage,
                success=False,
                total_articles=0,
                processed_articles=0,
                processing_time=processing_time,
                message=error_msg,
                error_message=str(e)
            )
    
    def run_stage_4_content_merging(self) -> PipelineResult:
        """4단계: 제목+본문 통합"""
        start_time = time.time()
        stage = 'content_merging'
        
        try:
            self.logger.info("🔄 4단계: 제목+본문 통합 시작")
            
            # 통합 실행
            result = self.content_merger.process_content_merge()
            
            processing_time = time.time() - start_time
            
            if result['successful_saves'] > 0:
                message = f"✅ 4단계 완료: {result['successful_saves']}개 기사 통합됨"
                self.logger.info(message)
                
                return PipelineResult(
                    stage=stage,
                    success=True,
                    total_articles=result['total_articles'],
                    processed_articles=result['successful_saves'],
                    processing_time=processing_time,
                    message=message,
                    metadata={
                        'successful_merges': result['successful_merges'],
                        'failed_merges': result['failed_merges'],
                        'merge_strategies': result['merge_strategies']
                    }
                )
            else:
                error_msg = f"❌ 4단계 실패: 통합된 기사가 없음"
                self.logger.error(error_msg)
                
                return PipelineResult(
                    stage=stage,
                    success=False,
                    total_articles=result['total_articles'],
                    processed_articles=0,
                    processing_time=processing_time,
                    message=error_msg,
                    error_message="No articles were merged"
                )
                
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"❌ 4단계 예외 발생: {e}"
            self.logger.error(error_msg)
            
            return PipelineResult(
                stage=stage,
                success=False,
                total_articles=0,
                processed_articles=0,
                processing_time=processing_time,
                message=error_msg,
                error_message=str(e)
            )
    
    def run_single_stage(self, stage_name: str) -> PipelineResult:
        """단일 단계 실행"""
        if stage_name not in self.stages:
            return PipelineResult(
                stage=stage_name,
                success=False,
                total_articles=0,
                processed_articles=0,
                processing_time=0,
                message=f"❌ 알 수 없는 단계: {stage_name}",
                error_message=f"Available stages: {list(self.stages.keys())}"
            )
        
        self.logger.info(f"🚀 {self.stages[stage_name]} 실행 시작")
        
        if stage_name == 'duplicate_removal':
            return self.run_stage_1_duplicate_removal()
        elif stage_name == 'text_cleaning':
            return self.run_stage_2_text_cleaning()
        elif stage_name == 'text_normalization':
            return self.run_stage_3_text_normalization()
        elif stage_name == 'content_merging':
            return self.run_stage_4_content_merging()
    
    def _map_media_outlet_name(self, outlet_name: str) -> str:
        """한글 언론사 이름을 영문 키로 변환"""
        mapping = {
            '연합뉴스': 'yonhap',
            '경향신문': 'khan',
            '중앙일보': 'joongang',
            '조선일보': 'chosun',
            '동아일보': 'donga',
            '오마이뉴스': 'ohmynews',
            '뉴시스': 'newsis',
            '한겨레': 'hani',
            '뉴스1': 'news1',
            '뉴스원': 'newsone'
        }
        return mapping.get(outlet_name, 'unknown')
    
    def _build_media_cache(self) -> Dict[str, str]:
        """언론사 정보를 미리 조회하여 캐시 구축"""
        try:
            # articles와 media_outlets를 조인하여 한 번에 조회
            result = self.supabase_manager.client.table('articles').select(
                'id, media_id, media_outlets(name)'
            ).execute()
            
            cache = {}
            for article in result.data:
                if article.get('media_outlets') and article['media_outlets'].get('name'):
                    outlet_name = article['media_outlets']['name']
                    cache[article['id']] = self._map_media_outlet_name(outlet_name)
                else:
                    cache[article['id']] = 'unknown'
            
            self.logger.info(f"📊 언론사 캐시 구축 완료: {len(cache)}개 기사")
            return cache
            
        except Exception as e:
            self.logger.warning(f"언론사 캐시 구축 실패, 기본값 사용: {e}")
            return {}
    
    def _fetch_articles_for_cleaning(self) -> List[Dict[str, Any]]:
        """정제되지 않은 기사들을 효율적으로 조회"""
        articles = []
        page_size = self.DEFAULT_PAGE_SIZE
        offset = 0
        
        while True:
            try:
                articles_result = self.supabase_manager.client.table('articles_cleaned').select(
                    'id, article_id, title_cleaned, lead_paragraph'
                ).or_(
                    'preprocessing_metadata->>text_cleaned.is.null,preprocessing_metadata->>text_cleaned.eq.false'
                ).order('created_at').range(offset, offset + page_size - 1).execute()
                
                page_data = articles_result.data if articles_result else []
                if not page_data:
                    break
                    
                articles.extend(page_data)
                
                if len(page_data) < page_size:
                    break
                    
                offset += page_size
                
            except Exception as e:
                self.logger.error(f"기사 조회 중 오류 (offset {offset}): {e}")
                break
        
        return articles
    
    def _process_articles_batch(self, articles: List[Dict[str, Any]], 
                               media_cache: Dict[str, str], 
                               process_func) -> Tuple[int, int]:
        """기사들을 배치로 처리"""
        processed_count = 0
        failed_count = 0
        batch_size = 50  # 배치 크기
        update_batch = []
        
        for i, article in enumerate(articles):
            try:
                # 개별 기사 처리
                result = process_func(article, media_cache)
                
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
                
                # 진행 상황 로깅
                if (i + 1) % self.PROGRESS_LOG_INTERVAL == 0:
                    self.logger.info(f"진행 상황: {i + 1}/{len(articles)} 기사 처리 완료")
                    
            except Exception as e:
                failed_count += 1
                self.logger.error(f"기사 {article.get('id', 'unknown')} 처리 실패: {e}")
                continue
        
        return processed_count, failed_count
    
    def _clean_single_article(self, article: Dict[str, Any], 
                            media_cache: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """단일 기사 정제 처리 (리드문 기반)"""
        try:
            media_outlet = media_cache.get(article['article_id'], 'unknown')
            
            # 제목과 리드문 정제
            cleaned_title, title_patterns = self._clean_article_title(article, media_outlet)
            cleaned_lead, lead_patterns, lead_paragraph = self._clean_article_lead(article, media_outlet)
            
            return self._build_cleaned_article_result(
                article, cleaned_title, title_patterns, 
                cleaned_lead, lead_patterns, lead_paragraph, media_outlet
            )
            
        except Exception as e:
            self.logger.error(f"기사 {article.get('id', 'unknown')} 정제 실패: {e}")
            return None
    
    def _clean_article_title(self, article: Dict[str, Any], media_outlet: str) -> Tuple[str, List[str]]:
        """기사 제목 정제"""
        return self.text_cleaner.clean_title(article['title_cleaned'] or '', media_outlet)
    
    def _clean_article_lead(self, article: Dict[str, Any], media_outlet: str) -> Tuple[str, List[str], str]:
        """기사 리드문 정제"""
        lead_paragraph = article.get('lead_paragraph', '')
        if not lead_paragraph:
            lead_paragraph = article.get('lead_paragraph', '')
        
        cleaned_lead, lead_patterns = self.text_cleaner.clean_content(lead_paragraph, media_outlet)
        return cleaned_lead, lead_patterns, lead_paragraph
    
    def _build_cleaned_article_result(self, article: Dict[str, Any], cleaned_title: str, 
                                    title_patterns: List[str], cleaned_lead: str, 
                                    lead_patterns: List[str], lead_paragraph: str, 
                                    media_outlet: str) -> Dict[str, Any]:
        """정제된 기사 결과 구성"""
        return {
            'id': article['id'],
            'title_cleaned': cleaned_title,
            'lead_paragraph': cleaned_lead,  # 정제된 리드문
            'preprocessing_metadata': {
                'text_cleaned': True,
                'text_cleaned_at': datetime.now().isoformat(),
                'title_patterns_removed': title_patterns,
                'lead_patterns_removed': lead_patterns,
                'media_outlet': media_outlet,
                'lead_extraction': {
                    'original_lead_length': len(lead_paragraph),
                    'cleaned_lead_length': len(cleaned_lead)
                }
            },
            'updated_at': datetime.now().isoformat()
        }
    
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
            self.logger.error(f"배치 업데이트 실패: {e}")
            # 개별 업데이트로 폴백
            for update_data in update_batch:
                try:
                    self.supabase_manager.client.table('articles_cleaned').update({
                        'title_cleaned': update_data['title_cleaned'],
                        'lead_paragraph': update_data['lead_paragraph'],
                        'preprocessing_metadata': update_data['preprocessing_metadata'],
                        'updated_at': update_data['updated_at']
                    }).eq('id', update_data['id']).execute()
                except Exception as individual_error:
                    self.logger.error(f"개별 업데이트 실패 (ID: {update_data['id']}): {individual_error}")
    
    def _fetch_articles_for_normalization(self) -> List[Dict[str, Any]]:
        """정규화되지 않은 기사들을 효율적으로 조회"""
        articles = []
        page_size = self.DEFAULT_PAGE_SIZE
        offset = 0
        
        while True:
            try:
                articles_result = self.supabase_manager.client.table('articles_cleaned').select(
                    'id, title_cleaned, lead_paragraph'
                ).or_(
                    'preprocessing_metadata->>text_normalized.is.null,preprocessing_metadata->>text_normalized.eq.false'
                ).order('created_at').range(offset, offset + page_size - 1).execute()
                
                page_data = articles_result.data if articles_result else []
                if not page_data:
                    break
                    
                articles.extend(page_data)
                
                if len(page_data) < page_size:
                    break
                    
                offset += page_size
                
            except Exception as e:
                self.logger.error(f"기사 조회 중 오류 (offset {offset}): {e}")
                break
        
        return articles
    
    def _normalize_single_article(self, article: Dict[str, Any], 
                                media_cache: Dict[str, str]) -> Optional[Dict[str, Any]]:
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
                    'content_changes': content_result.changes_made,
                    'normalization_stats': {
                        'title_changes_count': len(title_result.changes_made),
                        'content_changes_count': len(content_result.changes_made)
                    }
                },
                'updated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"기사 {article.get('id', 'unknown')} 정규화 실패: {e}")
            return None
    
    def run_full_pipeline(self, skip_stages: List[str] = None) -> FullPipelineResult:
        """전체 파이프라인 실행"""
        start_time = time.time()
        skip_stages = skip_stages or []
        
        self.logger.info("🚀 종합 전처리 파이프라인 시작")
        self.logger.info("=" * 60)
        
        # 초기 상태 확인
        initial_status = self.get_pipeline_status()
        self.logger.info(f"📊 초기 상태: 전체 기사 {initial_status.get('articles_total', 0)}개")
        
        stage_results = {}
        stages_completed = []
        stages_failed = []
        
        # 단계별 실행
        stage_order = ['duplicate_removal', 'text_cleaning', 'text_normalization', 'content_merging']
        
        for stage_name in stage_order:
            if stage_name in skip_stages:
                self.logger.info(f"⏭️  {self.stages[stage_name]} 건너뜀")
                continue
                
            result = self.run_single_stage(stage_name)
            stage_results[stage_name] = result
            
            if result.success:
                stages_completed.append(stage_name)
                self.logger.info(f"✅ {self.stages[stage_name]} 완료")
            else:
                stages_failed.append(stage_name)
                self.logger.error(f"❌ {self.stages[stage_name]} 실패")
                
                # 중요한 단계 실패 시 파이프라인 중단
                if stage_name in ['duplicate_removal']:
                    self.logger.error("🛑 중요한 단계 실패로 파이프라인 중단")
                    break
            
            self.logger.info("-" * 40)
        
        # 최종 상태 확인
        final_status = self.get_pipeline_status()
        final_article_count = final_status.get('cleaned_articles', 0)
        
        total_execution_time = time.time() - start_time
        overall_success = len(stages_failed) == 0
        
        self.logger.info("=" * 60)
        self.logger.info(f"🏁 파이프라인 완료")
        self.logger.info(f"⏱️  총 실행 시간: {total_execution_time:.2f}초")
        self.logger.info(f"✅ 완료된 단계: {len(stages_completed)}개")
        self.logger.info(f"❌ 실패한 단계: {len(stages_failed)}개")
        self.logger.info(f"📊 최종 처리된 기사: {final_article_count}개")
        
        return FullPipelineResult(
            total_execution_time=total_execution_time,
            stages_completed=stages_completed,
            stages_failed=stages_failed,
            final_article_count=final_article_count,
            stage_results=stage_results,
            overall_success=overall_success
        )

if __name__ == "__main__":
    # 직접 실행 시 전체 파이프라인 실행
    pipeline = PreprocessingPipeline()
    
    # 상태 확인
    print("📋 파이프라인 상태 확인...")
    status = pipeline.get_pipeline_status()
    print(f"전체 기사: {status.get('articles_total', 0)}개")
    print(f"전처리된 기사: {status.get('articles_preprocessed', 0)}개")
    print(f"정제된 기사: {status.get('cleaned_articles', 0)}개")
    print(f"통합된 기사: {status.get('merged_articles', 0)}개")
    print()
    
    # 전체 파이프라인 실행
    result = pipeline.run_full_pipeline()
    
    print("\n🎯 최종 결과:")
    print(f"전체 성공: {'✅' if result.overall_success else '❌'}")
    print(f"실행 시간: {result.total_execution_time:.2f}초")
    print(f"최종 기사 수: {result.final_article_count}개")
