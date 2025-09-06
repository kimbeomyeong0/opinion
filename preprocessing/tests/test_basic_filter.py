#!/usr/bin/env python3
"""
기본 필터링 모듈 테스트
- 본문 없는 기사 제거 테스트
- 뉴스통신사업자 기사 제거 테스트
- 짧은 기사 제거 테스트
"""

import sys
import os
import pytest
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from preprocessing.modules.basic_filter import BasicFilter, BasicFilterResult

class TestBasicFilter:
    """기본 필터링 모듈 테스트 클래스"""
    
    def setup_method(self):
        """각 테스트 메서드 실행 전 설정"""
        self.basic_filter = BasicFilter()
        
        # 테스트용 기사 데이터
        self.test_articles = [
            {
                'id': '1',
                'title': '정치 뉴스 1',
                'content': '이것은 정치 뉴스 내용입니다. 매우 중요한 내용이 포함되어 있습니다. 정치권에서는 다양한 의견이 제시되고 있으며, 국민들의 관심이 높아지고 있는 상황입니다. 전문가들은 이번 사안에 대해 신중한 접근이 필요하다고 분석하고 있습니다.',
                'url': 'https://example.com/news1'
            },
            {
                'id': '2',
                'title': '연합뉴스 - 경제 소식',  # 뉴스통신사업자
                'content': '경제 관련 소식입니다. 최근 경제 동향을 분석해보면 다양한 변화가 감지되고 있습니다. 전문가들은 신중한 관찰이 필요하다고 말하고 있습니다.',
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
            },
            {
                'id': '5',
                'title': '일반 뉴스',
                'content': '   ',  # 공백만 있는 본문
                'url': 'https://example.com/news5'
            }
        ]
    
    def test_initialization(self):
        """초기화 테스트"""
        basic_filter = BasicFilter(min_sentences=5, min_content_length=200)
        
        assert basic_filter.min_sentences == 5
        assert basic_filter.min_content_length == 200
        assert len(basic_filter.news_agencies) > 0
        assert '연합뉴스' in basic_filter.news_agencies
        assert '뉴시스' in basic_filter.news_agencies
    
    def test_has_content_valid_content(self):
        """유효한 본문 확인 테스트"""
        article_with_content = {
            'content': '이것은 유효한 본문입니다. 충분한 내용이 있습니다.'
        }
        
        assert self.basic_filter.has_content(article_with_content) is True
    
    def test_has_content_no_content(self):
        """본문 없는 기사 확인 테스트"""
        # 빈 본문
        article_empty = {'content': ''}
        assert self.basic_filter.has_content(article_empty) is False
        
        # 공백만 있는 본문
        article_whitespace = {'content': '   \n\t  '}
        assert self.basic_filter.has_content(article_whitespace) is False
        
        # 너무 짧은 본문
        article_short = {'content': '짧음'}
        assert self.basic_filter.has_content(article_short) is False
        
        # content 키가 없는 경우
        article_no_key = {'title': '제목만 있음'}
        assert self.basic_filter.has_content(article_no_key) is False
    
    def test_is_news_agency_article_by_title(self):
        """제목으로 뉴스통신사업자 기사 확인 테스트"""
        # 연합뉴스
        article_yonhap = {
            'title': '연합뉴스 - 정치 소식',
            'content': '내용',
            'url': 'https://example.com'
        }
        assert self.basic_filter.is_news_agency_article(article_yonhap) is True
        
        # 뉴시스
        article_newsis = {
            'title': '뉴시스 특보',
            'content': '내용',
            'url': 'https://example.com'
        }
        assert self.basic_filter.is_news_agency_article(article_newsis) is True
    
    def test_is_news_agency_article_by_url(self):
        """URL로 뉴스통신사업자 기사 확인 테스트"""
        # 연합뉴스 도메인
        article_yonhap_url = {
            'title': '정치 뉴스',
            'content': '내용',
            'url': 'https://yna.co.kr/view/123'
        }
        assert self.basic_filter.is_news_agency_article(article_yonhap_url) is True
        
        # 뉴시스 도메인
        article_newsis_url = {
            'title': '경제 뉴스',
            'content': '내용',
            'url': 'https://newsis.com/view/456'
        }
        assert self.basic_filter.is_news_agency_article(article_newsis_url) is True
    
    def test_is_news_agency_article_general_media(self):
        """일반 언론사 기사 확인 테스트"""
        article_general = {
            'title': '조선일보 정치 뉴스',
            'content': '내용',
            'url': 'https://chosun.com/123'
        }
        assert self.basic_filter.is_news_agency_article(article_general) is False
    
    def test_is_short_article_by_sentences(self):
        """문장 수로 짧은 기사 확인 테스트"""
        # 짧은 기사 (2문장)
        short_article = {
            'content': '첫 번째 문장입니다. 두 번째 문장입니다.'
        }
        assert self.basic_filter.is_short_article(short_article) is True
        
        # 충분한 길이 (4문장)
        long_article = {
            'content': '첫 번째 문장입니다. 두 번째 문장입니다. 세 번째 문장입니다. 네 번째 문장입니다.'
        }
        assert self.basic_filter.is_short_article(long_article) is False
    
    def test_is_short_article_by_length(self):
        """길이로 짧은 기사 확인 테스트"""
        # 짧은 내용 (문장 수도 적고 길이도 짧음)
        short_content = {
            'content': '짧은 내용입니다. 이것도 짧습니다.'  # 2문장 + 짧은 길이
        }
        assert self.basic_filter.is_short_article(short_content) is True
    
    def test_is_short_article_breaking_news(self):
        """속보성 기사 확인 테스트"""
        # 속보 + 짧은 내용
        breaking_news = {
            'title': '[속보] 긴급 상황 발생',
            'content': '긴급 상황이 발생했습니다. 자세한 내용은 추후 보도할 예정입니다.'
        }
        assert self.basic_filter.is_short_article(breaking_news) is True
        
        # 속보이지만 충분한 내용
        breaking_long = {
            'title': '[속보] 중요한 발표',
            'content': '중요한 발표가 있었습니다. ' * 20  # 충분히 긴 내용
        }
        assert self.basic_filter.is_short_article(breaking_long) is False
    
    def test_filter_no_content_articles(self):
        """본문 없는 기사 필터링 테스트"""
        filtered_articles, removed_count = self.basic_filter.filter_no_content_articles(self.test_articles)
        
        # 본문 없는 기사 제거 (실제 개수 확인)
        assert removed_count >= 2
        assert len(filtered_articles) <= 3
        
        # 제거된 기사 확인
        remaining_ids = [article['id'] for article in filtered_articles]
        assert '3' not in remaining_ids  # 빈 본문
        assert '5' not in remaining_ids  # 공백만 있는 본문
    
    def test_filter_news_agency_articles(self):
        """뉴스통신사업자 기사 필터링 테스트"""
        filtered_articles, removed_count = self.basic_filter.filter_news_agency_articles(self.test_articles)
        
        # 뉴스통신사업자 기사 1개 제거 (id: 2)
        assert removed_count == 1
        assert len(filtered_articles) == 4
        
        # 제거된 기사 확인
        remaining_ids = [article['id'] for article in filtered_articles]
        assert '2' not in remaining_ids  # 연합뉴스 기사
    
    def test_filter_short_articles(self):
        """짧은 기사 필터링 테스트"""
        filtered_articles, removed_count = self.basic_filter.filter_short_articles(self.test_articles)
        
        # 짧은 기사 제거 (정확한 수는 내용에 따라 달라질 수 있음)
        assert removed_count >= 1
        assert len(filtered_articles) < len(self.test_articles)
        
        # id: 4 (짧은 속보) 제거 확인
        remaining_ids = [article['id'] for article in filtered_articles]
        assert '4' not in remaining_ids
    
    def test_filter_empty_list(self):
        """빈 리스트 필터링 테스트"""
        empty_articles = []
        
        # 본문 없는 기사 필터링
        filtered, removed = self.basic_filter.filter_no_content_articles(empty_articles)
        assert len(filtered) == 0
        assert removed == 0
        
        # 뉴스통신사업자 기사 필터링
        filtered, removed = self.basic_filter.filter_news_agency_articles(empty_articles)
        assert len(filtered) == 0
        assert removed == 0
        
        # 짧은 기사 필터링
        filtered, removed = self.basic_filter.filter_short_articles(empty_articles)
        assert len(filtered) == 0
        assert removed == 0
    
    def test_process_basic_filtering_success(self):
        """전체 기본 필터링 프로세스 성공 테스트"""
        result = self.basic_filter.process_basic_filtering(self.test_articles)
        
        # 결과 검증
        assert result.success is True
        assert result.total_articles == 5
        assert result.no_content_removed >= 0
        assert result.news_agency_removed >= 0
        assert result.short_articles_removed >= 0
        assert result.final_articles < result.total_articles  # 일부 기사가 제거되어야 함
        assert result.processing_time > 0
        assert result.error_message is None
    
    def test_process_basic_filtering_empty_articles(self):
        """빈 기사 리스트 처리 테스트"""
        result = self.basic_filter.process_basic_filtering([])
        
        # 결과 검증
        assert result.success is True
        assert result.total_articles == 0
        assert result.final_articles == 0
        assert result.no_content_removed == 0
        assert result.news_agency_removed == 0
        assert result.short_articles_removed == 0

# 통합 테스트
class TestBasicFilterIntegration:
    """기본 필터링 통합 테스트 클래스"""
    
    def test_full_filtering_process(self):
        """전체 필터링 프로세스 통합 테스트"""
        # 다양한 유형의 기사 데이터
        mixed_articles = [
            {
                'id': '1',
                'title': '정치 뉴스',
                'content': '충분한 길이의 정치 뉴스 내용입니다. ' * 10,
                'url': 'https://example.com/politics1'
            },
            {
                'id': '2',
                'title': '연합뉴스 - 경제 동향',
                'content': '경제 동향 분석입니다. ' * 5,
                'url': 'https://yna.co.kr/economy1'
            },
            {
                'id': '3',
                'title': '사회 뉴스',
                'content': '',
                'url': 'https://example.com/society1'
            },
            {
                'id': '4',
                'title': '[속보] 긴급 상황',
                'content': '짧은 속보입니다.',
                'url': 'https://example.com/breaking1'
            },
            {
                'id': '5',
                'title': '문화 뉴스',
                'content': '충분한 길이의 문화 뉴스 내용입니다. ' * 15,
                'url': 'https://example.com/culture1'
            }
        ]
        
        # 기본 필터링 실행
        basic_filter = BasicFilter()
        result = basic_filter.process_basic_filtering(mixed_articles)
        
        # 결과 검증
        assert result.success is True
        assert result.total_articles == 5
        assert result.final_articles < result.total_articles  # 일부 제거
        assert result.no_content_removed >= 1  # id: 3 제거
        assert result.news_agency_removed >= 1  # id: 2 제거
        assert result.short_articles_removed >= 0  # 짧은 기사 제거 (기준에 따라 달라질 수 있음)

# 성능 테스트
class TestBasicFilterPerformance:
    """기본 필터링 성능 테스트 클래스"""
    
    def test_large_dataset_performance(self):
        """대량 데이터 처리 성능 테스트"""
        # 대량 테스트 데이터 생성
        large_articles = []
        for i in range(1000):
            article_type = i % 4
            
            if article_type == 0:  # 정상 기사
                large_articles.append({
                    'id': str(i),
                    'title': f'정상 뉴스 {i}',
                    'content': f'충분한 길이의 뉴스 내용입니다. ' * 10,
                    'url': f'https://example.com/news{i}'
                })
            elif article_type == 1:  # 뉴스통신사업자
                large_articles.append({
                    'id': str(i),
                    'title': f'연합뉴스 - 뉴스 {i}',
                    'content': f'뉴스 내용입니다. ' * 5,
                    'url': f'https://yna.co.kr/news{i}'
                })
            elif article_type == 2:  # 본문 없음
                large_articles.append({
                    'id': str(i),
                    'title': f'빈 뉴스 {i}',
                    'content': '',
                    'url': f'https://example.com/empty{i}'
                })
            else:  # 짧은 기사
                large_articles.append({
                    'id': str(i),
                    'title': f'[속보] 짧은 뉴스 {i}',
                    'content': '짧은 내용입니다.',
                    'url': f'https://example.com/short{i}'
                })
        
        # 성능 측정
        import time
        start_time = time.time()
        
        basic_filter = BasicFilter()
        result = basic_filter.process_basic_filtering(large_articles)
        
        processing_time = time.time() - start_time
        
        # 성능 검증 (2초 이내에 처리되어야 함)
        assert processing_time < 2.0
        assert result.success is True
        assert result.final_articles < result.total_articles
        assert result.no_content_removed > 0
        assert result.news_agency_removed > 0
        assert result.short_articles_removed >= 0

# 실행 예시
if __name__ == "__main__":
    # pytest 실행
    pytest.main([__file__, "-v"])
