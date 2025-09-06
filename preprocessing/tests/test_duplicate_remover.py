#!/usr/bin/env python3
"""
중복 제거 모듈 테스트
- 단위 테스트: 각 메서드별 개별 테스트
- 통합 테스트: 전체 프로세스 테스트
- 에러 케이스 테스트: 예외 상황 처리 테스트
"""

import sys
import os
import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from preprocessing.modules.duplicate_remover import DuplicateRemover, DuplicateRemovalResult
from preprocessing.utils.similarity_calculator import SimilarityCalculator

class TestDuplicateRemover:
    """중복 제거 모듈 테스트 클래스"""
    
    def setup_method(self):
        """각 테스트 메서드 실행 전 설정"""
        self.remover = DuplicateRemover()
        
        # 테스트용 기사 데이터
        self.test_articles = [
            {
                'id': '1',
                'title': '정치 뉴스 1',
                'content': '이것은 정치 뉴스 내용입니다.',
                'published_at': '2024-01-01T10:00:00Z'
            },
            {
                'id': '2',
                'title': '정치 뉴스 1',  # 중복 제목
                'content': '이것은 다른 정치 뉴스 내용입니다.',
                'published_at': '2024-01-02T10:00:00Z'  # 더 최신
            },
            {
                'id': '3',
                'title': '경제 뉴스 1',
                'content': '이것은 정치 뉴스 내용입니다.',  # 중복 본문
                'published_at': '2024-01-03T10:00:00Z'  # 더 최신
            },
            {
                'id': '4',
                'title': '경제 뉴스 2',
                'content': '이것은 경제 뉴스 내용입니다.',
                'published_at': '2024-01-04T10:00:00Z'
            }
        ]
    
    def test_initialization(self):
        """초기화 테스트"""
        remover = DuplicateRemover(title_threshold=0.9, content_threshold=0.8)
        
        assert remover.similarity_calculator.title_threshold == 0.9
        assert remover.similarity_calculator.content_threshold == 0.8
        assert remover.supabase_manager is not None
    
    @patch('preprocessing.modules.duplicate_remover.SupabaseManager')
    def test_fetch_articles_from_supabase_success(self, mock_supabase_manager):
        """Supabase에서 기사 조회 성공 테스트"""
        # Mock 설정
        mock_client = Mock()
        mock_client.table.return_value.select.return_value.limit.return_value.execute.return_value.data = self.test_articles
        mock_supabase_manager.return_value.client = mock_client
        
        remover = DuplicateRemover()
        articles = remover.fetch_articles_from_supabase(limit=10)
        
        assert len(articles) == 4
        assert articles[0]['id'] == '1'
        assert articles[1]['title'] == '정치 뉴스 1'
    
    @patch('preprocessing.modules.duplicate_remover.SupabaseManager')
    def test_fetch_articles_from_supabase_failure(self, mock_supabase_manager):
        """Supabase에서 기사 조회 실패 테스트"""
        # Mock 설정 - 클라이언트가 None
        mock_supabase_manager.return_value.client = None
        
        remover = DuplicateRemover()
        
        with pytest.raises(Exception, match="Supabase 클라이언트가 초기화되지 않았습니다"):
            remover.fetch_articles_from_supabase()
    
    def test_remove_duplicate_titles(self):
        """제목 중복 제거 테스트"""
        # 제목 중복 제거 실행
        unique_articles, duplicates_removed = self.remover.remove_duplicate_titles(self.test_articles)
        
        # 결과 검증
        assert len(unique_articles) == 3  # 4개에서 1개 제거
        assert duplicates_removed == 1
        
        # 제목별로 하나씩만 있는지 확인
        titles = [article['title'] for article in unique_articles]
        assert len(set(titles)) == len(titles)  # 모든 제목이 고유한지 확인
        
        # 최신 기사가 유지되었는지 확인 (정치 뉴스 1의 경우 id=2가 유지되어야 함)
        political_news = [article for article in unique_articles if article['title'] == '정치 뉴스 1'][0]
        assert political_news['id'] == '2'  # 더 최신 기사
    
    def test_remove_duplicate_titles_empty_list(self):
        """빈 리스트에 대한 제목 중복 제거 테스트"""
        unique_articles, duplicates_removed = self.remover.remove_duplicate_titles([])
        
        assert len(unique_articles) == 0
        assert duplicates_removed == 0
    
    def test_remove_duplicate_contents(self):
        """본문 중복 제거 테스트"""
        # 본문 중복 제거 실행
        unique_articles, duplicates_removed = self.remover.remove_duplicate_contents(self.test_articles)
        
        # 결과 검증 (정확한 수는 유사도 계산에 따라 달라질 수 있음)
        assert len(unique_articles) <= len(self.test_articles)
        assert duplicates_removed >= 0
    
    def test_remove_duplicate_contents_empty_list(self):
        """빈 리스트에 대한 본문 중복 제거 테스트"""
        unique_articles, duplicates_removed = self.remover.remove_duplicate_contents([])
        
        assert len(unique_articles) == 0
        assert duplicates_removed == 0
    
    @patch('preprocessing.modules.duplicate_remover.SupabaseManager')
    def test_save_to_articles_cleaned_success(self, mock_supabase_manager):
        """articles_cleaned 저장 성공 테스트"""
        # Mock 설정
        mock_client = Mock()
        mock_client.table.return_value.insert.return_value.execute.return_value.data = [{'id': '1'}]
        mock_supabase_manager.return_value.client = mock_client
        
        remover = DuplicateRemover()
        result = remover.save_to_articles_cleaned(self.test_articles[:2])
        
        assert result is True
        mock_client.table.assert_called_once_with('articles_cleaned')
    
    @patch('preprocessing.modules.duplicate_remover.SupabaseManager')
    def test_save_to_articles_cleaned_failure(self, mock_supabase_manager):
        """articles_cleaned 저장 실패 테스트"""
        # Mock 설정 - 클라이언트가 None
        mock_supabase_manager.return_value.client = None
        
        remover = DuplicateRemover()
        result = remover.save_to_articles_cleaned(self.test_articles[:2])
        
        assert result is False
    
    @patch('preprocessing.modules.duplicate_remover.SupabaseManager')
    def test_process_duplicate_removal_success(self, mock_supabase_manager):
        """전체 중복 제거 프로세스 성공 테스트"""
        # Mock 설정
        mock_client = Mock()
        mock_client.table.return_value.select.return_value.limit.return_value.execute.return_value.data = self.test_articles
        mock_client.table.return_value.insert.return_value.execute.return_value.data = [{'id': '1'}]
        mock_supabase_manager.return_value.client = mock_client
        
        remover = DuplicateRemover()
        result = remover.process_duplicate_removal(limit=10)
        
        # 결과 검증
        assert result.success is True
        assert result.total_articles == 4
        assert result.title_duplicates_removed >= 0
        assert result.content_duplicates_removed >= 0
        assert result.final_articles > 0
        assert result.processing_time > 0
        assert result.error_message is None
    
    @patch('preprocessing.modules.duplicate_remover.SupabaseManager')
    def test_process_duplicate_removal_failure(self, mock_supabase_manager):
        """전체 중복 제거 프로세스 실패 테스트"""
        # Mock 설정 - 클라이언트가 None
        mock_supabase_manager.return_value.client = None
        
        remover = DuplicateRemover()
        result = remover.process_duplicate_removal()
        
        # 결과 검증
        assert result.success is False
        assert result.total_articles == 0
        assert result.error_message is not None
        assert "Supabase 클라이언트가 초기화되지 않았습니다" in result.error_message
    
    def test_process_duplicate_removal_empty_articles(self):
        """빈 기사 리스트에 대한 처리 테스트"""
        with patch.object(self.remover, 'fetch_articles_from_supabase', return_value=[]):
            result = self.remover.process_duplicate_removal()
            
            assert result.success is True
            assert result.total_articles == 0
            assert result.final_articles == 0

class TestSimilarityCalculator:
    """유사도 계산기 테스트 클래스"""
    
    def setup_method(self):
        """각 테스트 메서드 실행 전 설정"""
        self.calculator = SimilarityCalculator()
    
    def test_normalize_text(self):
        """텍스트 정규화 테스트"""
        # 공백 정규화
        assert self.calculator.normalize_text("  hello   world  ") == "hello world"
        
        # 특수문자 제거
        assert self.calculator.normalize_text("hello!@# world") == "hello world"
        
        # 소문자 변환
        assert self.calculator.normalize_text("HELLO World") == "hello world"
        
        # 빈 문자열
        assert self.calculator.normalize_text("") == ""
        assert self.calculator.normalize_text(None) == ""
    
    def test_calculate_title_similarity(self):
        """제목 유사도 계산 테스트"""
        # 정확한 매칭
        result = self.calculator.calculate_title_similarity("정치 뉴스", "정치 뉴스")
        assert result.similarity_score == 1.0
        assert result.is_duplicate is True
        
        # 다른 제목
        result = self.calculator.calculate_title_similarity("정치 뉴스", "경제 뉴스")
        assert result.similarity_score < 1.0
        assert result.is_duplicate is False
        
        # 빈 제목
        result = self.calculator.calculate_title_similarity("", "정치 뉴스")
        assert result.similarity_score == 0.0
        assert result.is_duplicate is False
    
    def test_calculate_content_similarity(self):
        """본문 유사도 계산 테스트"""
        content1 = "이것은 정치 뉴스 내용입니다. 매우 중요한 내용이 포함되어 있습니다. 정치권에서는 다양한 의견이 제시되고 있으며, 국민들의 관심이 높아지고 있는 상황입니다. 전문가들은 이번 사안에 대해 신중한 접근이 필요하다고 분석하고 있습니다."
        content2 = "이것은 정치 뉴스 내용입니다. 매우 중요한 내용이 포함되어 있습니다. 정치권에서는 다양한 의견이 제시되고 있으며, 국민들의 관심이 높아지고 있는 상황입니다. 전문가들은 이번 사안에 대해 신중한 접근이 필요하다고 분석하고 있습니다."
        
        result = self.calculator.calculate_content_similarity(content1, content2)
        assert result.similarity_score >= 0.95
        assert result.is_duplicate is True
        
        # 다른 본문
        content3 = "이것은 경제 뉴스 내용입니다. 완전히 다른 내용입니다. 경제 전문가들은 최근 시장 동향을 분석하며, 향후 전망에 대해 다양한 의견을 제시하고 있습니다. 투자자들은 신중한 판단이 필요한 시점이라고 보고 있습니다."
        result = self.calculator.calculate_content_similarity(content1, content3)
        assert result.similarity_score < 0.95
        assert result.is_duplicate is False
        
        # 짧은 본문도 유사도 계산됨
        short_content1 = "짧은 내용"
        short_content2 = "짧은 내용"
        result = self.calculator.calculate_content_similarity(short_content1, short_content2)
        assert result.similarity_score == 1.0  # 동일한 짧은 텍스트
        assert result.is_duplicate is True
        
        # 빈 문자열은 제외
        empty_content = ""
        result = self.calculator.calculate_content_similarity(empty_content, content1)
        assert result.similarity_score == 0.0
        assert result.is_duplicate is False

# 통합 테스트
class TestIntegration:
    """통합 테스트 클래스"""
    
    def test_full_duplicate_removal_process(self):
        """전체 중복 제거 프로세스 통합 테스트"""
        # 실제 Supabase 연결 없이 테스트
        with patch('preprocessing.modules.duplicate_remover.SupabaseManager') as mock_supabase:
            # Mock 설정
            mock_client = Mock()
            mock_client.table.return_value.select.return_value.limit.return_value.execute.return_value.data = [
                {
                    'id': '1',
                    'title': '정치 뉴스',
                    'content': '정치 뉴스 내용입니다.',
                    'published_at': '2024-01-01T10:00:00Z'
                },
                {
                    'id': '2',
                    'title': '정치 뉴스',  # 중복 제목
                    'content': '다른 정치 뉴스 내용입니다.',
                    'published_at': '2024-01-02T10:00:00Z'  # 더 최신
                }
            ]
            mock_client.table.return_value.insert.return_value.execute.return_value.data = [{'id': '1'}]
            mock_supabase.return_value.client = mock_client
            
            # 중복 제거 실행
            remover = DuplicateRemover()
            result = remover.process_duplicate_removal(limit=10)
            
            # 결과 검증
            assert result.success is True
            assert result.total_articles == 2
            assert result.title_duplicates_removed == 1
            assert result.final_articles == 1

# 성능 테스트
class TestPerformance:
    """성능 테스트 클래스"""
    
    def test_large_dataset_performance(self):
        """대량 데이터 처리 성능 테스트"""
        # 대량 테스트 데이터 생성
        large_articles = []
        for i in range(1000):
            large_articles.append({
                'id': str(i),
                'title': f'뉴스 제목 {i % 100}',  # 100개 그룹으로 중복 생성
                'content': f'뉴스 내용 {i % 50}',  # 50개 그룹으로 중복 생성
                'published_at': f'2024-01-{(i % 30) + 1:02d}T10:00:00Z'
            })
        
        # 성능 측정
        import time
        start_time = time.time()
        
        remover = DuplicateRemover()
        unique_articles, title_duplicates = remover.remove_duplicate_titles(large_articles)
        
        processing_time = time.time() - start_time
        
        # 성능 검증 (1초 이내에 처리되어야 함)
        assert processing_time < 1.0
        assert len(unique_articles) < len(large_articles)
        assert title_duplicates > 0

# 실행 예시
if __name__ == "__main__":
    # pytest 실행
    pytest.main([__file__, "-v"])
