#!/usr/bin/env python3
"""
중복 제거 메인 모듈
- Supabase에서 기사 데이터 조회
- 제목과 본문 중복 제거
- 최신 기사만 유지하여 articles_cleaned에 저장
"""

import sys
import os
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.supabase_manager import SupabaseManager
from preprocessing.utils.similarity_calculator import SimilarityCalculator, SimilarityResult
from preprocessing.modules.basic_filter import BasicFilter
from preprocessing.modules.lead_extractor import LeadExtractor

@dataclass
class PreprocessingResult:
    """전처리 결과 (중복 제거 + 기본 필터링)"""
    total_articles: int
    title_duplicates_removed: int
    content_duplicates_removed: int
    no_content_removed: int
    news_agency_removed: int
    short_articles_removed: int
    final_articles: int
    processing_time: float
    success: bool
    error_message: Optional[str] = None

class IntegratedPreprocessor:
    """통합 전처리 클래스 (중복 제거 + 기본 필터링)"""
    
    def __init__(self, title_threshold: float = 1.0, content_threshold: float = 0.95, 
                 min_sentences: int = 3, min_content_length: int = 100, 
                 max_lead_sentences: int = 3, date_filter=None):
        """
        초기화
        
        Args:
            title_threshold: 제목 유사도 임계값 (기본값: 1.0 = 정확한 매칭)
            content_threshold: 본문 유사도 임계값 (기본값: 0.95)
            min_sentences: 최소 문장 수 (기본값: 3)
            min_content_length: 최소 본문 길이 (기본값: 100자)
            max_lead_sentences: 리드문 최대 문장 수 (기본값: 3)
        """
        self.supabase_manager = SupabaseManager()
        self.similarity_calculator = SimilarityCalculator(title_threshold, content_threshold)
        self.basic_filter = BasicFilter(min_sentences, min_content_length)
        self.lead_extractor = LeadExtractor(max_lead_sentences)
        self.date_filter = date_filter
        
    def fetch_articles_from_supabase(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Supabase에서 기사 데이터 조회 (메모리 효율적 페이지네이션)
        
        Args:
            limit: 조회할 기사 수 제한 (None이면 모든 기사)
            
        Returns:
            기사 리스트
        """
        if not self.supabase_manager.client:
            raise Exception("Supabase 클라이언트가 초기화되지 않았습니다.")
        
        try:
            # 먼저 이미 전처리된 기사 ID들을 가져옴
            print("🔍 이미 전처리된 기사 확인 중...")
            processed_result = self.supabase_manager.client.table('articles_cleaned').select('article_id').execute()
            processed_ids = set(item['article_id'] for item in processed_result.data)
            print(f"✅ {len(processed_ids)}개 기사가 이미 전처리됨")
            
            all_articles = []
            page_size = 500  # 메모리 효율성을 위해 페이지 크기 감소
            offset = 0
            total_processed = 0
            total_filtered = 0
            
            while True:
                try:
                    # 필요한 컬럼만 선택하여 메모리 사용량 최적화
                    query = self.supabase_manager.client.table('articles').select(
                        'id, title, content, published_at, url, media_id'
                    ).order('published_at', desc=True)
                    
                    # 날짜 필터링 적용
                    if self.date_filter:
                        from datetime import datetime, timedelta
                        import pytz
                        
                        kct = pytz.timezone('Asia/Seoul')
                        utc = pytz.UTC
                        
                        if self.date_filter == 'yesterday':
                            # KCT 기준 전날 00:00-23:59
                            kct_yesterday = datetime.now(kct).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
                            kct_start = kct_yesterday
                            kct_end = kct_yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
                            
                            # UTC로 변환
                            utc_start = kct_start.astimezone(utc)
                            utc_end = kct_end.astimezone(utc)
                            
                            query = query.gte('published_at', utc_start.isoformat()).lte('published_at', utc_end.isoformat())
                            
                        elif self.date_filter == 'today':
                            # KCT 기준 오늘 00:00-현재
                            kct_today = datetime.now(kct).replace(hour=0, minute=0, second=0, microsecond=0)
                            kct_start = kct_today
                            kct_end = datetime.now(kct)
                            
                            # UTC로 변환
                            utc_start = kct_start.astimezone(utc)
                            utc_end = kct_end.astimezone(utc)
                            
                            query = query.gte('published_at', utc_start.isoformat()).lte('published_at', utc_end.isoformat())
                    
                    query = query.range(offset, offset + page_size - 1)
                    
                    result = query.execute()
                    
                    if not result.data:
                        break  # 더 이상 데이터가 없으면 종료
                    
                    # 이미 전처리된 기사 제외
                    filtered_articles = [
                        article for article in result.data 
                        if article['id'] not in processed_ids
                    ]
                    
                    current_batch_size = len(result.data)
                    filtered_count = len(filtered_articles)
                    total_filtered += filtered_count
                    
                    all_articles.extend(filtered_articles)
                    total_processed += current_batch_size
                    
                    print(f"📄 {current_batch_size}개 기사 조회, {filtered_count}개 신규 기사 (총 {total_processed}개 조회, {total_filtered}개 신규)")
                    
                    # limit이 설정되어 있고 도달했으면 중단
                    if limit and total_filtered >= limit:
                        all_articles = all_articles[:limit]
                        break
                    
                    # 마지막 페이지인 경우 (조회된 데이터가 page_size보다 적으면)
                    if current_batch_size < page_size:
                        break
                    
                    offset += page_size
                    
                    # 메모리 사용량 체크 (대략적)
                    if total_processed % 5000 == 0:
                        print(f"💾 메모리 사용량 체크: {total_processed}개 기사 처리됨")
                    
                except Exception as page_error:
                    print(f"⚠️  페이지 조회 오류 (offset {offset}): {page_error}")
                    # 페이지 오류 시 다음 페이지로 계속 진행
                    offset += page_size
                    continue
            
            print(f"✅ 총 {len(all_articles)}개 신규 기사 조회 완료")
            return all_articles
            
        except Exception as e:
            raise Exception(f"기사 데이터 조회 실패: {str(e)}")
    
    def remove_duplicate_titles(self, articles: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        """
        제목 중복 제거 (가장 최신 것만 유지)
        
        Args:
            articles: 기사 리스트
            
        Returns:
            (중복 제거된 기사 리스트, 제거된 중복 수)
        """
        if not articles:
            return articles, 0
        
        # published_at 기준으로 정렬 (최신순)
        sorted_articles = sorted(
            articles, 
            key=lambda x: x.get('published_at', ''), 
            reverse=True
        )
        
        # 제목별로 그룹화
        title_groups = {}
        for article in sorted_articles:
            title = article.get('title', '').strip()
            if not title:
                continue
                
            if title not in title_groups:
                title_groups[title] = []
            title_groups[title].append(article)
        
        # 각 그룹에서 첫 번째(최신) 기사만 유지
        unique_articles = []
        duplicates_removed = 0
        
        for title, group in title_groups.items():
            if len(group) > 1:
                duplicates_removed += len(group) - 1
            unique_articles.append(group[0])  # 최신 기사만 유지
        
        return unique_articles, duplicates_removed
    
    def remove_duplicate_contents(self, articles: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        """
        본문 중복 제거 (유사도 0.95 기준, 가장 최신 것만 유지)
        
        Args:
            articles: 기사 리스트
            
        Returns:
            (중복 제거된 기사 리스트, 제거된 중복 수)
        """
        if not articles:
            return articles, 0
        
        # published_at 기준으로 정렬 (최신순)
        sorted_articles = sorted(
            articles, 
            key=lambda x: x.get('published_at', ''), 
            reverse=True
        )
        
        # 중복 본문 찾기
        content_duplicates = self.similarity_calculator.find_duplicate_contents(sorted_articles)
        
        # 제거할 기사 인덱스 집합
        indices_to_remove = set()
        
        for i, j, similarity_result in content_duplicates:
            # 더 최신 기사(인덱스가 작은 것)를 유지하고 나머지 제거
            indices_to_remove.add(j)  # j가 더 오래된 기사
        
        # 제거할 인덱스가 아닌 기사만 유지
        unique_articles = [
            article for idx, article in enumerate(sorted_articles)
            if idx not in indices_to_remove
        ]
        
        return unique_articles, len(indices_to_remove)
    
    def remove_hybrid_duplicate_contents(self, articles: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        """
        하이브리드 방식으로 본문 중복 제거 (O(n) 시간 복잡도)
        
        Args:
            articles: 기사 리스트
            
        Returns:
            (중복 제거된 기사 리스트, 제거된 중복 수)
        """
        if not articles:
            return articles, 0
        
        # published_at 기준으로 정렬 (최신순)
        sorted_articles = sorted(
            articles, 
            key=lambda x: x.get('published_at', ''), 
            reverse=True
        )
        
        # 하이브리드 중복 찾기 (O(n) 복잡도)
        content_duplicates = self.similarity_calculator.find_hybrid_duplicates(sorted_articles)
        
        # 제거할 기사 인덱스 집합
        indices_to_remove = set()
        
        for i, j, similarity_result in content_duplicates:
            # 더 최신 기사(인덱스가 작은 것)를 유지하고 나머지 제거
            indices_to_remove.add(j)  # j가 더 오래된 기사
        
        # 중복이 아닌 기사들만 유지
        unique_articles = [
            article for idx, article in enumerate(sorted_articles) 
            if idx not in indices_to_remove
        ]
        
        duplicates_removed = len(indices_to_remove)
        return unique_articles, duplicates_removed
    
    def save_to_articles_cleaned(self, articles: List[Dict[str, Any]]) -> bool:
        """
        중복 제거된 기사를 articles_cleaned 테이블에 저장
        
        Args:
            articles: 중복 제거된 기사 리스트
            
        Returns:
            저장 성공 여부
        """
        if not self.supabase_manager.client:
            return False
        
        try:
            # articles_cleaned 테이블에 저장할 데이터 준비
            cleaned_articles = []
            
            for article in articles:
                # 리드문 추출 (전처리 성능 최적화)
                lead_paragraph = self.lead_extractor.extract_lead_paragraph(article.get('content', ''))
                
                cleaned_article = {
                    'article_id': article['id'],
                    'title_cleaned': article.get('title', ''),
                    'lead_paragraph': lead_paragraph,
                    'preprocessing_metadata': {
                        'duplicate_removal': {
                            'processed_at': datetime.now().isoformat(),
                            'title_duplicates_removed': 0,  # 개별 기사에서는 0
                            'content_duplicates_removed': 0,
                            'similarity_threshold': self.similarity_calculator.content_threshold,
                            'method': 'hybrid'
                        },
                        'basic_filter': {
                            'processed_at': datetime.now().isoformat(),
                            'no_content_removed': False,
                            'news_agency_removed': False,
                            'short_article_removed': False
                        },
                        'lead_extraction': {
                            'processed_at': datetime.now().isoformat(),
                            'max_sentences': self.lead_extractor.max_sentences,
                            'lead_length': len(lead_paragraph),
                            'original_length': len(article.get('content', ''))
                        }
                    },
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat()
                }
                cleaned_articles.append(cleaned_article)
            
            # 개별 저장으로 변경 (배치 저장 오류 방지)
            success_count = 0
            for article in cleaned_articles:
                try:
                    result = self.supabase_manager.client.table('articles_cleaned').insert([article]).execute()
                    if result.data:
                        success_count += 1
                except Exception as e:
                    print(f"⚠️ 기사 저장 실패 (ID: {article.get('article_id', 'unknown')}): {e}")
                    continue
            
            print(f"✅ {success_count}/{len(cleaned_articles)}개 기사 저장 완료")
            return success_count > 0
            
        except Exception as e:
            print(f"❌ articles_cleaned 저장 실패: {str(e)}")
            return False
    
    def process_integrated_preprocessing(self, limit: Optional[int] = None) -> PreprocessingResult:
        """
        통합 전처리 프로세스 실행 (중복 제거 + 기본 필터링)
        
        Args:
            limit: 처리할 기사 수 제한
            
        Returns:
            통합 전처리 결과
        """
        start_time = datetime.now()
        
        try:
            print("🚀 통합 전처리 프로세스 시작...")
            
            # 1. Supabase에서 기사 데이터 조회
            print("📡 기사 데이터 조회 중...")
            articles = self.fetch_articles_from_supabase(limit)
            print(f"✅ {len(articles)}개 기사 조회 완료")
            
            if not articles:
                return PreprocessingResult(
                    total_articles=0,
                    title_duplicates_removed=0,
                    content_duplicates_removed=0,
                    no_content_removed=0,
                    news_agency_removed=0,
                    short_articles_removed=0,
                    final_articles=0,
                    processing_time=0,
                    success=True
                )
            
            # 2. 제목 중복 제거
            print("🔍 제목 중복 제거 중...")
            articles_after_title, title_duplicates = self.remove_duplicate_titles(articles)
            print(f"✅ 제목 중복 {title_duplicates}개 제거 완료")
            
            # 3. 하이브리드 본문 중복 제거
            print("🔍 하이브리드 본문 중복 제거 중...")
            articles_after_content, content_duplicates = self.remove_hybrid_duplicate_contents(articles_after_title)
            print(f"✅ 본문 중복 {content_duplicates}개 제거 완료")
            
            # 4. 기본 필터링 (본문 없는 기사, 뉴스통신사업자, 짧은 기사 제거)
            print("🔍 기본 필터링 시작...")
            basic_filter_result = self.basic_filter.process_basic_filtering(articles_after_content)
            
            if not basic_filter_result.success:
                raise Exception(f"기본 필터링 실패: {basic_filter_result.error_message}")
            
            print(f"✅ 기본 필터링 완료: {basic_filter_result.final_articles}개 기사 남음")
            
            # 5. 최종 기사 리스트 (기본 필터링 후 남은 기사들)
            # basic_filter.process_basic_filtering은 결과만 반환하므로, 실제 필터링된 기사를 다시 가져와야 함
            final_articles = self._apply_basic_filtering(articles_after_content)
            
            # 6. articles_cleaned에 저장
            print("💾 articles_cleaned에 저장 중...")
            save_success = self.save_to_articles_cleaned(final_articles)
            
            if not save_success:
                raise Exception("articles_cleaned 저장 실패")
            
            print(f"✅ {len(final_articles)}개 기사 저장 완료")
            
            # 7. 결과 반환
            processing_time = (datetime.now() - start_time).total_seconds()
            
            return PreprocessingResult(
                total_articles=len(articles),
                title_duplicates_removed=title_duplicates,
                content_duplicates_removed=content_duplicates,
                no_content_removed=basic_filter_result.no_content_removed,
                news_agency_removed=basic_filter_result.news_agency_removed,
                short_articles_removed=basic_filter_result.short_articles_removed,
                final_articles=len(final_articles),
                processing_time=processing_time,
                success=True
            )
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"통합 전처리 프로세스 실패: {str(e)}"
            print(f"❌ {error_msg}")
            
            return PreprocessingResult(
                total_articles=len(articles) if 'articles' in locals() else 0,
                title_duplicates_removed=0,
                content_duplicates_removed=0,
                no_content_removed=0,
                news_agency_removed=0,
                short_articles_removed=0,
                final_articles=0,
                processing_time=processing_time,
                success=False,
                error_message=error_msg
            )
    
    def _apply_basic_filtering(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        기본 필터링을 적용하여 실제 필터링된 기사 리스트 반환
        
        Args:
            articles: 기사 리스트
            
        Returns:
            필터링된 기사 리스트
        """
        # 1. 본문 없는 기사 제거
        articles_filtered, _ = self.basic_filter.filter_no_content_articles(articles)
        
        # 2. 뉴스통신사업자 기사 제거
        articles_filtered, _ = self.basic_filter.filter_news_agency_articles(articles_filtered)
        
        # 3. 짧은 기사 제거
        articles_filtered, _ = self.basic_filter.filter_short_articles(articles_filtered)
        
        return articles_filtered

# 사용 예시
if __name__ == "__main__":
    # 통합 전처리기 생성
    preprocessor = IntegratedPreprocessor()
    
    # 통합 전처리 실행 (테스트용으로 100개만)
    result = preprocessor.process_integrated_preprocessing(limit=100)
    
    # 결과 출력
    print("\n📊 통합 전처리 결과:")
    print(f"  총 기사 수: {result.total_articles}")
    print(f"  제목 중복 제거: {result.title_duplicates_removed}개")
    print(f"  본문 중복 제거: {result.content_duplicates_removed}개")
    print(f"  본문 없는 기사 제거: {result.no_content_removed}개")
    print(f"  뉴스통신사업자 기사 제거: {result.news_agency_removed}개")
    print(f"  짧은 기사 제거: {result.short_articles_removed}개")
    print(f"  최종 기사 수: {result.final_articles}")
    print(f"  처리 시간: {result.processing_time:.2f}초")
    print(f"  성공 여부: {'✅ 성공' if result.success else '❌ 실패'}")
    
    if result.error_message:
        print(f"  오류 메시지: {result.error_message}")

# 이전 버전 호환성을 위한 별칭
DuplicateRemover = IntegratedPreprocessor
DuplicateRemovalResult = PreprocessingResult
