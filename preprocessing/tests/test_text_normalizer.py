#!/usr/bin/env python3
"""
텍스트 정규화 모듈 테스트
"""

import pytest
import sys
import os

# 상위 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from preprocessing.modules.text_normalizer import TextNormalizer, NormalizationResult

class TestTextNormalizer:
    """텍스트 정규화 메인 모듈 테스트"""
    
    def setup_method(self):
        """테스트 전 초기화"""
        self.normalizer = TextNormalizer()
    
    def test_initialization(self):
        """초기화 테스트"""
        assert self.normalizer is not None
        assert len(self.normalizer.hanja_dict) > 0
        assert len(self.normalizer.abbrev_dict) > 0
        assert len(self.normalizer.case_normalization) > 0
    
    def test_dictionary_sizes(self):
        """사전 크기 테스트 (핵심 용어 100개 목표)"""
        total_size = (len(self.normalizer.hanja_dict) + 
                     len(self.normalizer.abbrev_dict) + 
                     len(self.normalizer.case_normalization))
        
        # 100개 내외 (±20% 허용)
        assert 80 <= total_size <= 200
        
        # 각 사전별 최소 크기
        assert len(self.normalizer.hanja_dict) >= 30
        assert len(self.normalizer.abbrev_dict) >= 30
        assert len(self.normalizer.case_normalization) >= 10

class TestHanjaNormalization:
    """한자 정규화 테스트"""
    
    def setup_method(self):
        """테스트 전 초기화"""
        self.normalizer = TextNormalizer()
    
    def test_basic_hanja_normalization(self):
        """기본 한자 정규화 테스트"""
        text = "李대통령이 中國과 美國의 관계를 논의했다."
        normalized, changes = self.normalizer.normalize_hanja(text)
        
        assert "이대통령이" in normalized
        assert "중국과" in normalized  # 中國 → 중국
        assert "미국의" in normalized
        assert len(changes) >= 2
    
    def test_political_hanja(self):
        """정치 관련 한자 테스트"""
        text = "與野 합의로 政府 정책을 추진한다."
        normalized, changes = self.normalizer.normalize_hanja(text)
        
        assert "여당야당" in normalized or "여당" in normalized
        assert "정부" in normalized
        assert len(changes) >= 2
    
    def test_no_hanja_text(self):
        """한자가 없는 텍스트 테스트"""
        text = "한글로만 작성된 텍스트입니다."
        normalized, changes = self.normalizer.normalize_hanja(text)
        
        assert normalized == text
        assert len(changes) == 0
    
    def test_mixed_hanja_hangul(self):
        """한자와 한글 혼합 텍스트 테스트"""
        text = "北韓과 남한의 관계"
        normalized, changes = self.normalizer.normalize_hanja(text)
        
        assert "북" in normalized
        assert "남한의 관계" in normalized
        assert len(changes) >= 1

class TestAbbreviationNormalization:
    """영문 약어 정규화 테스트"""
    
    def setup_method(self):
        """테스트 전 초기화"""
        self.normalizer = TextNormalizer()
    
    def test_basic_abbreviation_normalization(self):
        """기본 영문 약어 정규화 테스트"""
        text = "AI 기술과 IT 산업이 GDP 성장에 기여한다."
        normalized, changes = self.normalizer.normalize_abbreviations(text)
        
        assert "인공지능" in normalized
        assert "정보기술" in normalized
        assert "국내총생산" in normalized
        assert len(changes) >= 3
    
    def test_international_organizations(self):
        """국제기구 약어 테스트"""
        text = "UN과 WHO가 협력하여 정책을 발표했다."
        normalized, changes = self.normalizer.normalize_abbreviations(text)
        
        assert "유엔과" in normalized
        assert "세계보건기구가" in normalized
        assert len(changes) >= 2
    
    def test_technology_abbreviations(self):
        """기술 약어 테스트"""
        text = "IoT와 VR 기술이 발전하고 있다."
        normalized, changes = self.normalizer.normalize_abbreviations(text)
        
        assert "사물인터넷" in normalized
        assert "가상현실" in normalized
        assert len(changes) >= 2
    
    def test_no_abbreviation_text(self):
        """영문 약어가 없는 텍스트 테스트"""
        text = "순수 한글 텍스트입니다."
        normalized, changes = self.normalizer.normalize_abbreviations(text)
        
        assert normalized == text
        assert len(changes) == 0
    
    def test_partial_match_prevention(self):
        """부분 매칭 방지 테스트"""
        text = "ABCDE 회사의 AI 기술"
        normalized, changes = self.normalizer.normalize_abbreviations(text)
        
        # ABCDE는 변경되지 않아야 함 (사전에 없음)
        assert "ABCDE" in normalized
        # AI는 변경되어야 함
        assert "인공지능" in normalized
        assert len(changes) == 1

class TestCaseNormalization:
    """대소문자 정규화 테스트"""
    
    def setup_method(self):
        """테스트 전 초기화"""
        self.normalizer = TextNormalizer()
    
    def test_lowercase_to_uppercase(self):
        """소문자 → 대문자 변환 테스트"""
        text = "ai 기술과 gdp 성장률"
        normalized, changes = self.normalizer.normalize_case(text)
        
        assert "AI" in normalized
        assert "GDP" in normalized
        assert len(changes) >= 2
    
    def test_mixed_case_normalization(self):
        """혼합 대소문자 정규화 테스트"""
        text = "Api 인터페이스와 Url 주소"
        normalized, changes = self.normalizer.normalize_case(text)
        
        # 정규화 사전에 있는 경우만 변경
        expected_changes = 0
        for word in ['Api', 'Url']:
            if word.lower() in [k.lower() for k in self.normalizer.case_normalization.keys()]:
                expected_changes += 1
        
        assert len(changes) == expected_changes

class TestIntegratedNormalization:
    """통합 정규화 테스트"""
    
    def setup_method(self):
        """테스트 전 초기화"""
        self.normalizer = TextNormalizer()
    
    def test_full_normalization_process(self):
        """전체 정규화 프로세스 테스트"""
        text = "李대통령이 美中 관계에서 AI와 gdp 협력을 강조했다."
        result = self.normalizer.normalize_text(text)
        
        assert isinstance(result, NormalizationResult)
        assert result.success is True
        assert "이대통령이" in result.normalized_text
        assert "미국중국" in result.normalized_text or "미국" in result.normalized_text
        assert "인공지능" in result.normalized_text
        assert "GDP" in result.normalized_text
        assert len(result.changes_made) >= 4
    
    def test_complex_text_normalization(self):
        """복합 텍스트 정규화 테스트"""
        text = "與野 합의로 UN에서 AI 기술과 it 정책을 논의한다."
        result = self.normalizer.normalize_text(text)
        
        assert result.success is True
        
        # 한자 변경 확인
        hanja_changes = [c for c in result.changes_made if c['type'] == 'hanja']
        assert len(hanja_changes) >= 2  # 與, 野
        
        # 약어 변경 확인  
        abbrev_changes = [c for c in result.changes_made if c['type'] == 'abbreviation']
        assert len(abbrev_changes) >= 2  # UN, AI
        
        # 대소문자 변경 확인
        case_changes = [c for c in result.changes_made if c['type'] == 'case']
        assert len(case_changes) >= 1  # it → IT
    
    def test_empty_text_normalization(self):
        """빈 텍스트 정규화 테스트"""
        text = ""
        result = self.normalizer.normalize_text(text)
        
        assert result.success is True
        assert result.normalized_text == ""
        assert len(result.changes_made) == 0
    
    def test_normalization_metadata(self):
        """정규화 메타데이터 테스트"""
        text = "AI 기술이 GDP에 미치는 영향"
        result = self.normalizer.normalize_text(text)
        
        assert 'original_length' in result.normalization_metadata
        assert 'normalized_length' in result.normalization_metadata
        assert 'total_changes' in result.normalization_metadata
        assert 'change_ratio' in result.normalization_metadata
        assert result.normalization_metadata['original_length'] == len(text)

class TestNormalizationStatistics:
    """정규화 통계 테스트"""
    
    def setup_method(self):
        """테스트 전 초기화"""
        self.normalizer = TextNormalizer()
    
    def test_statistics_empty_results(self):
        """빈 결과 통계 테스트"""
        stats = self.normalizer.get_normalization_statistics([])
        assert stats == {}
    
    def test_statistics_calculation(self):
        """통계 계산 테스트"""
        texts = [
            "AI 기술과 GDP 성장",
            "李대통령의 政策 발표",
            "un과 who의 협력"
        ]
        
        results = [self.normalizer.normalize_text(text) for text in texts]
        stats = self.normalizer.get_normalization_statistics(results)
        
        assert stats['total_articles'] == 3
        assert stats['successful_articles'] == 3
        assert stats['success_rate'] == 1.0
        assert 'total_changes' in stats
        assert 'most_common_changes' in stats
        assert 'dictionary_sizes' in stats

class TestArticleNormalization:
    """기사 정규화 테스트"""
    
    def setup_method(self):
        """테스트 전 초기화"""
        self.normalizer = TextNormalizer()
    
    def test_single_article_normalization(self):
        """단일 기사 정규화 테스트"""
        articles = [{
            'title': 'AI 기술 발전',
            'content': '李대통령이 GDP 성장에 대해 발표했다.'
        }]
        
        results = self.normalizer.normalize_articles(articles)
        
        assert len(results) == 1
        assert results[0].success is True
        assert "인공지능" in results[0].normalized_text
        assert "이대통령이" in results[0].normalized_text
        assert "국내총생산" in results[0].normalized_text
    
    def test_multiple_articles_normalization(self):
        """여러 기사 정규화 테스트"""
        articles = [
            {'title': 'AI 뉴스', 'content': 'IT 기술 발전'},
            {'title': '與野 합의', 'content': 'UN 결의안 통과'},
            {'title': '', 'content': ''}  # 빈 기사
        ]
        
        results = self.normalizer.normalize_articles(articles)
        
        assert len(results) == 3
        assert all(r.success for r in results)
        
        # 첫 번째 기사
        assert "인공지능" in results[0].normalized_text
        assert "정보기술" in results[0].normalized_text
        
        # 두 번째 기사
        assert "여당야당" in results[1].normalized_text or "여당" in results[1].normalized_text
        assert "유엔" in results[1].normalized_text
        
        # 세 번째 기사 (빈 기사)
        assert results[2].normalized_text == ""

class TestNormalizationPerformance:
    """정규화 성능 테스트"""
    
    def setup_method(self):
        """테스트 전 초기화"""
        self.normalizer = TextNormalizer()
    
    def test_large_text_performance(self):
        """대용량 텍스트 성능 테스트"""
        # 긴 텍스트 생성
        text = "AI 기술과 IT 산업이 GDP에 미치는 영향을 분석한다. " * 100
        
        import time
        start_time = time.time()
        result = self.normalizer.normalize_text(text)
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        assert result.success is True
        assert processing_time < 5.0  # 5초 이내 처리
        assert len(result.changes_made) > 0
    
    def test_batch_processing_performance(self):
        """일괄 처리 성능 테스트"""
        # 100개 기사 생성
        articles = []
        for i in range(100):
            articles.append({
                'title': f'AI 기술 뉴스 {i}',
                'content': f'李대통령이 GDP 성장과 IT 정책에 대해 {i}번째 발표를 했다.'
            })
        
        import time
        start_time = time.time()
        results = self.normalizer.normalize_articles(articles)
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        assert len(results) == 100
        assert all(r.success for r in results)
        assert processing_time < 10.0  # 10초 이내 처리

if __name__ == "__main__":
    # 직접 실행 시 간단한 테스트
    normalizer = TextNormalizer()
    
    test_text = "李대통령이 AI 기술과 gdp 성장에 대해 UN에서 발표했다."
    result = normalizer.normalize_text(test_text)
    
    print("=== 텍스트 정규화 테스트 ===")
    print(f"원본: {result.original_text}")
    print(f"정규화: {result.normalized_text}")
    print(f"변경사항: {len(result.changes_made)}개")
    
    for change in result.changes_made:
        print(f"  - {change['pattern']} ({change['type']})")
    
    print(f"성공: {result.success}")
    print(f"사전 크기: 총 {len(normalizer.hanja_dict) + len(normalizer.abbrev_dict) + len(normalizer.case_normalization)}개")
