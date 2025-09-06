#!/usr/bin/env python3
"""
텍스트 정제 모듈 테스트
"""

import pytest
import sys
import os

# 상위 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from preprocessing.modules.text_cleaner import TextCleaner, TextCleaningResult

class TestTextCleaner:
    """텍스트 정제 메인 모듈 테스트"""
    
    def setup_method(self):
        """테스트 전 초기화"""
        self.cleaner = TextCleaner()
    
    def test_initialization(self):
        """초기화 테스트"""
        assert self.cleaner is not None
        assert 'yonhap' in self.cleaner.patterns
        assert 'common' in self.cleaner.patterns
        assert len(self.cleaner.patterns) >= 8  # 8개 언론사 + common
    
    def test_detect_media_outlet_yonhap(self):
        """연합뉴스 URL 식별 테스트"""
        url = "https://www.yonhapnews.co.kr/politics/2025/09/05/test"
        result = self.cleaner.detect_media_outlet(url)
        assert result == 'yonhap'
    
    def test_detect_media_outlet_newsis(self):
        """뉴시스 URL 식별 테스트"""
        url = "https://www.newsis.com/view/test"
        result = self.cleaner.detect_media_outlet(url)
        assert result == 'newsis'
    
    def test_detect_media_outlet_unknown(self):
        """알 수 없는 URL 식별 테스트"""
        url = "https://www.unknown.com/test"
        result = self.cleaner.detect_media_outlet(url)
        assert result == 'unknown'
    
    def test_clean_title_common_patterns(self):
        """제목 공통 패턴 정제 테스트"""
        title = "[속보] 이재명 대통령 발표"
        cleaned, patterns = self.cleaner.clean_title(title, 'unknown')
        assert cleaned == "이재명 대통령 발표"
        assert len(patterns) > 0
    
    def test_clean_title_newsis_patterns(self):
        """뉴시스 제목 패턴 정제 테스트"""
        title = "[속보]김선민 발언 [뉴시스Pic]"
        cleaned, patterns = self.cleaner.clean_title(title, 'newsis')
        assert cleaned == "김선민 발언"
        assert len(patterns) >= 2  # [속보]와 [뉴시스Pic] 패턴
    
    def test_clean_content_newsis_patterns(self):
        """뉴시스 본문 패턴 정제 테스트"""
        content = "[서울=뉴시스]김기자 이재명 대통령이 발표했다고 밝혔다."
        cleaned, patterns = self.cleaner.clean_content(content, 'newsis')
        assert cleaned == "이재명 대통령이 발표했다고 밝혔다."
        assert len(patterns) > 0
    
    def test_clean_content_joongang_patterns(self):
        """중앙일보 본문 패턴 정제 테스트"""
        content = "정치 뉴스입니다. 편집 김철수 PD. 이어지는 내용입니다."
        cleaned, patterns = self.cleaner.clean_content(content, 'joongang')
        assert "편집 김철수 PD" not in cleaned
        assert "정치 뉴스입니다" in cleaned
        assert "이어지는 내용입니다" in cleaned
        assert len(patterns) > 0
    
    def test_clean_content_common_email_removal(self):
        """공통 이메일 제거 패턴 테스트"""
        content = "문의사항은 test@example.com으로 연락주세요."
        cleaned, patterns = self.cleaner.clean_content(content, 'unknown')
        assert "test@example.com" not in cleaned
        assert "문의사항은" in cleaned
        assert len(patterns) > 0
    
    def test_clean_content_donga_ad_removal(self):
        """동아일보 광고 코드 제거 테스트"""
        content = "뉴스 내용입니다. googletag.cmd.push(function() { googletag.display('div-gpt-ad'); }); 계속되는 내용입니다."
        cleaned, patterns = self.cleaner.clean_content(content, 'donga')
        assert "googletag" not in cleaned
        assert "뉴스 내용입니다" in cleaned
        assert "계속되는 내용입니다" in cleaned
        assert len(patterns) > 0
    
    def test_clean_article_success(self):
        """단일 기사 정제 성공 테스트"""
        article = {
            'title': '[속보] 정치 뉴스',
            'content': '[서울=뉴시스]김기자 뉴스 내용입니다.',
            'url': 'https://www.newsis.com/test'
        }
        
        result = self.cleaner.clean_article(article)
        
        assert isinstance(result, TextCleaningResult)
        assert result.success is True
        assert result.cleaned_title == "정치 뉴스"
        assert "김기자" not in result.cleaned_content
        assert "뉴스 내용입니다" in result.cleaned_content
        assert result.cleaning_metadata['media_outlet'] == 'newsis'
        assert len(result.patterns_removed) > 0
    
    def test_clean_article_empty_content(self):
        """빈 내용 기사 정제 테스트"""
        article = {
            'title': '',
            'content': '',
            'url': 'https://www.test.com'
        }
        
        result = self.cleaner.clean_article(article)
        
        assert result.success is True
        assert result.cleaned_title == ""
        assert result.cleaned_content == ""
        assert len(result.patterns_removed) == 0
    
    def test_clean_articles_multiple(self):
        """여러 기사 일괄 정제 테스트"""
        articles = [
            {
                'title': '[속보] 뉴스 1',
                'content': '[서울=뉴시스]기자1 내용1',
                'url': 'https://www.newsis.com/test1'
            },
            {
                'title': '뉴스 2',
                'content': '내용2입니다. 편집 김PD 추가내용',
                'url': 'https://joongang.co.kr/test2'
            }
        ]
        
        results = self.cleaner.clean_articles(articles)
        
        assert len(results) == 2
        assert all(r.success for r in results)
        assert results[0].cleaning_metadata['media_outlet'] == 'newsis'
        assert results[1].cleaning_metadata['media_outlet'] == 'joongang'

class TestTextCleaningStatistics:
    """텍스트 정제 통계 테스트"""
    
    def setup_method(self):
        """테스트 전 초기화"""
        self.cleaner = TextCleaner()
    
    def test_get_cleaning_statistics_empty(self):
        """빈 결과 통계 테스트"""
        stats = self.cleaner.get_cleaning_statistics([])
        assert stats == {}
    
    def test_get_cleaning_statistics_success(self):
        """정상 통계 테스트"""
        articles = [
            {
                'title': '[속보] 뉴스 1',
                'content': '[서울=뉴시스]기자1 내용1',
                'url': 'https://www.newsis.com/test1'
            },
            {
                'title': '뉴스 2',
                'content': '내용2',
                'url': 'https://joongang.co.kr/test2'
            }
        ]
        
        results = self.cleaner.clean_articles(articles)
        stats = self.cleaner.get_cleaning_statistics(results)
        
        assert stats['total_articles'] == 2
        assert stats['successful_articles'] == 2
        assert stats['failed_articles'] == 0
        assert stats['success_rate'] == 1.0
        assert 'media_statistics' in stats
        assert 'newsis' in stats['media_statistics']
        assert 'joongang' in stats['media_statistics']

class TestMediaOutletSpecificPatterns:
    """언론사별 특화 패턴 테스트"""
    
    def setup_method(self):
        """테스트 전 초기화"""
        self.cleaner = TextCleaner()
    
    def test_yonhap_patterns(self):
        """연합뉴스 패턴 테스트"""
        # 기자 바이라인 패턴
        content = "(서울=연합뉴스) 김기자 기자 = 뉴스 내용입니다."
        cleaned, patterns = self.cleaner.clean_content(content, 'yonhap')
        assert "김기자 기자 =" not in cleaned
        assert "뉴스 내용입니다" in cleaned
        assert len(patterns) > 0
    
    def test_khan_patterns(self):
        """경향신문 패턴 테스트"""
        # 타 언론사 인용 패턴
        content = "교도통신이 5일 보도했다고 전했다. 추가 내용입니다."
        cleaned, patterns = self.cleaner.clean_content(content, 'khan')
        assert "교도통신이 5일 보도했다" not in cleaned
        assert "추가 내용입니다" in cleaned
        assert len(patterns) > 0
    
    def test_chosun_patterns(self):
        """조선일보 패턴 테스트"""
        # 제목 태그만 제거 (본문은 깔끔함)
        title = "[News&How] 정치 분석"
        cleaned, patterns = self.cleaner.clean_title(title, 'chosun')
        assert cleaned == "정치 분석"
        assert len(patterns) > 0
    
    def test_ohmynews_patterns(self):
        """오마이뉴스 패턴 테스트"""
        # 덧붙이는 글 패턴
        content = "뉴스 내용입니다. 덧붙이는 글 | 추가 정보입니다. 글쓴이 김기자는 시민기자입니다."
        cleaned, patterns = self.cleaner.clean_content(content, 'ohmynews')
        assert "덧붙이는 글" not in cleaned
        assert "글쓴이 김기자" not in cleaned
        assert "뉴스 내용입니다" in cleaned
        assert len(patterns) > 0
    
    def test_hani_patterns(self):
        """한겨레 패턴 테스트"""
        # 한겨레는 매우 깔끔하므로 기본 패턴만 적용
        title = "[단독] 정치 뉴스"
        cleaned, patterns = self.cleaner.clean_title(title, 'hani')
        assert cleaned == "정치 뉴스"
        assert len(patterns) > 0

class TestTextCleaningIntegration:
    """텍스트 정제 통합 테스트"""
    
    def setup_method(self):
        """테스트 전 초기화"""
        self.cleaner = TextCleaner()
    
    def test_full_cleaning_process(self):
        """전체 정제 프로세스 테스트"""
        articles = [
            # 뉴시스 기사
            {
                'title': '[속보]김선민 발언 [뉴시스Pic]',
                'content': '[서울=뉴시스]김기자 정치 뉴스 내용입니다. 연락처: test@newsis.com',
                'url': 'https://www.newsis.com/test'
            },
            # 중앙일보 기사
            {
                'title': '경제 뉴스',
                'content': '경제 상황입니다. 편집 이철수 PD 추가 분석입니다.',
                'url': 'https://joongang.co.kr/test'
            },
            # 동아일보 기사
            {
                'title': '사회 뉴스',
                'content': '사회 이슈입니다. googletag.cmd.push(function() { googletag.display("ad"); }); 계속 내용입니다. (서울=연합뉴스)',
                'url': 'https://www.donga.com/test'
            }
        ]
        
        results = self.cleaner.clean_articles(articles)
        stats = self.cleaner.get_cleaning_statistics(results)
        
        # 모든 기사가 성공적으로 처리되었는지 확인
        assert len(results) == 3
        assert all(r.success for r in results)
        assert stats['success_rate'] == 1.0
        
        # 각 기사별 정제 결과 확인
        # 뉴시스
        assert results[0].cleaned_title == "김선민 발언"
        assert "김기자" not in results[0].cleaned_content
        assert "test@newsis.com" not in results[0].cleaned_content
        
        # 중앙일보
        assert "편집 이철수 PD" not in results[1].cleaned_content
        assert "경제 상황입니다" in results[1].cleaned_content
        
        # 동아일보
        assert "googletag" not in results[2].cleaned_content
        assert "(서울=연합뉴스)" not in results[2].cleaned_content
        assert "사회 이슈입니다" in results[2].cleaned_content

class TestTextCleaningPerformance:
    """텍스트 정제 성능 테스트"""
    
    def setup_method(self):
        """테스트 전 초기화"""
        self.cleaner = TextCleaner()
    
    def test_large_dataset_performance(self):
        """대량 데이터 처리 성능 테스트"""
        # 100개 기사 생성
        articles = []
        for i in range(100):
            articles.append({
                'title': f'[속보] 뉴스 제목 {i}',
                'content': f'[서울=뉴시스]기자{i} 뉴스 내용 {i}입니다. 이메일: test{i}@example.com',
                'url': f'https://www.newsis.com/test{i}'
            })
        
        import time
        start_time = time.time()
        results = self.cleaner.clean_articles(articles)
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        assert len(results) == 100
        assert all(r.success for r in results)
        assert processing_time < 10.0  # 10초 이내 처리
        
        stats = self.cleaner.get_cleaning_statistics(results)
        assert stats['success_rate'] == 1.0
        assert stats['total_patterns_removed'] > 0

if __name__ == "__main__":
    # 직접 실행 시 간단한 테스트
    cleaner = TextCleaner()
    
    test_article = {
        'title': '[속보] 이재명 대통령 발표',
        'content': '[서울=뉴시스]김기자 대통령이 오늘 발표했습니다. 문의: news@test.com',
        'url': 'https://www.newsis.com/test'
    }
    
    result = cleaner.clean_article(test_article)
    
    print("=== 텍스트 정제 테스트 ===")
    print(f"원본 제목: {result.original_title}")
    print(f"정제 제목: {result.cleaned_title}")
    print(f"원본 본문: {result.original_content}")
    print(f"정제 본문: {result.cleaned_content}")
    print(f"제거된 패턴: {result.patterns_removed}")
    print(f"언론사: {result.cleaning_metadata['media_outlet']}")
    print(f"성공: {result.success}")
