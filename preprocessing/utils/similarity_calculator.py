#!/usr/bin/env python3
"""
하이브리드 유사도 계산 유틸리티
- 단계별 중복 검사로 성능 최적화
- 해시 기반 정확한 중복 + 제목/리드 기반 빠른 중복
- O(n) 시간 복잡도로 대량 데이터 처리
"""

import re
import difflib
import hashlib
from typing import List, Tuple, Optional, Dict, Set
from dataclasses import dataclass

@dataclass
class SimilarityResult:
    """유사도 계산 결과"""
    similarity_score: float
    is_duplicate: bool
    similarity_type: str  # 'title' or 'content'
    threshold: float

class SimilarityCalculator:
    """유사도 계산 클래스"""
    
    def __init__(self, title_threshold: float = 1.0, content_threshold: float = 0.95):
        """
        초기화
        
        Args:
            title_threshold: 제목 유사도 임계값 (기본값: 1.0 = 정확한 매칭)
            content_threshold: 본문 유사도 임계값 (기본값: 0.95)
        """
        self.title_threshold = title_threshold
        self.content_threshold = content_threshold
    
    def normalize_text(self, text: str) -> str:
        """
        텍스트 정규화
        
        Args:
            text: 정규화할 텍스트
            
        Returns:
            정규화된 텍스트
        """
        if not text:
            return ""
        
        # 공백 정규화
        text = re.sub(r'\s+', ' ', text.strip())
        
        # 특수문자 정규화
        text = re.sub(r'[^\w\s가-힣]', '', text)
        
        # 소문자 변환 (영문의 경우)
        text = text.lower()
        
        return text
    
    def calculate_title_similarity(self, title1: str, title2: str) -> SimilarityResult:
        """
        제목 유사도 계산
        
        Args:
            title1: 첫 번째 제목
            title2: 두 번째 제목
            
        Returns:
            유사도 계산 결과
        """
        if not title1 or not title2:
            return SimilarityResult(0.0, False, 'title', self.title_threshold)
        
        # 텍스트 정규화
        norm_title1 = self.normalize_text(title1)
        norm_title2 = self.normalize_text(title2)
        
        # 정확한 매칭 확인
        if norm_title1 == norm_title2:
            return SimilarityResult(1.0, True, 'title', self.title_threshold)
        
        # difflib을 사용한 유사도 계산
        similarity = difflib.SequenceMatcher(None, norm_title1, norm_title2).ratio()
        
        return SimilarityResult(
            similarity_score=similarity,
            is_duplicate=similarity >= self.title_threshold,
            similarity_type='title',
            threshold=self.title_threshold
        )
    
    def calculate_content_similarity(self, content1: str, content2: str) -> SimilarityResult:
        """
        본문 유사도 계산
        
        Args:
            content1: 첫 번째 본문
            content2: 두 번째 본문
            
        Returns:
            유사도 계산 결과
        """
        if not content1 or not content2:
            return SimilarityResult(0.0, False, 'content', self.content_threshold)
        
        # 텍스트 정규화
        norm_content1 = self.normalize_text(content1)
        norm_content2 = self.normalize_text(content2)
        
        # 빈 문자열이나 공백만 있는 경우만 제외
        if not norm_content1.strip() or not norm_content2.strip():
            return SimilarityResult(0.0, False, 'content', self.content_threshold)
        
        # difflib을 사용한 유사도 계산
        similarity = difflib.SequenceMatcher(None, norm_content1, norm_content2).ratio()
        
        return SimilarityResult(
            similarity_score=similarity,
            is_duplicate=similarity >= self.content_threshold,
            similarity_type='content',
            threshold=self.content_threshold
        )
    
    def find_duplicate_titles(self, articles: List[dict]) -> List[Tuple[int, int, SimilarityResult]]:
        """
        중복 제목 찾기
        
        Args:
            articles: 기사 리스트
            
        Returns:
            중복 제목 쌍 리스트 [(인덱스1, 인덱스2, 유사도결과), ...]
        """
        duplicates = []
        
        for i in range(len(articles)):
            for j in range(i + 1, len(articles)):
                title1 = articles[i].get('title', '')
                title2 = articles[j].get('title', '')
                
                similarity_result = self.calculate_title_similarity(title1, title2)
                
                if similarity_result.is_duplicate:
                    duplicates.append((i, j, similarity_result))
        
        return duplicates
    
    def find_duplicate_contents(self, articles: List[dict]) -> List[Tuple[int, int, SimilarityResult]]:
        """
        중복 본문 찾기 (최적화됨)
        
        Args:
            articles: 기사 리스트
            
        Returns:
            중복 본문 쌍 리스트 [(인덱스1, 인덱스2, 유사도결과), ...]
        """
        duplicates = []
        total_comparisons = len(articles) * (len(articles) - 1) // 2
        current_comparison = 0
        
        # 진행 상황 출력을 위한 체크포인트
        progress_checkpoints = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
        checkpoint_index = 0
        
        # 본문이 비어있거나 너무 짧은 기사들은 미리 제외
        valid_articles = []
        for i, article in enumerate(articles):
            content = article.get('content', '')
            if content and len(content.strip()) > 50:  # 최소 길이 체크
                valid_articles.append((i, article, self.normalize_text(content)))
        
        print(f"📊 유사도 비교 대상: {len(valid_articles)}/{len(articles)}개 기사")
        
        for i in range(len(valid_articles)):
            for j in range(i + 1, len(valid_articles)):
                idx1, article1, norm_content1 = valid_articles[i]
                idx2, article2, norm_content2 = valid_articles[j]
                
                # 빠른 사전 필터링: 길이 차이가 너무 크면 스킵
                len_diff = abs(len(norm_content1) - len(norm_content2))
                if len_diff > max(len(norm_content1), len(norm_content2)) * 0.5:
                    current_comparison += 1
                    continue
                
                # difflib을 사용한 유사도 계산
                similarity = difflib.SequenceMatcher(None, norm_content1, norm_content2).ratio()
                
                similarity_result = SimilarityResult(
                    similarity_score=similarity,
                    is_duplicate=similarity >= self.content_threshold,
                    similarity_type='content',
                    threshold=self.content_threshold
                )
                
                if similarity_result.is_duplicate:
                    duplicates.append((idx1, idx2, similarity_result))
                
                current_comparison += 1
                
                # 진행 상황 출력
                if checkpoint_index < len(progress_checkpoints):
                    progress = current_comparison / total_comparisons
                    if progress >= progress_checkpoints[checkpoint_index]:
                        print(f"🔍 본문 유사도 계산 진행: {progress_checkpoints[checkpoint_index]*100:.0f}% ({current_comparison:,}/{total_comparisons:,})")
                        checkpoint_index += 1
        
        return duplicates
    
    def find_exact_duplicates(self, articles: List[dict]) -> List[Tuple[int, int, SimilarityResult]]:
        """
        해시 기반 정확한 중복 찾기 (O(n))
        
        Args:
            articles: 기사 리스트
            
        Returns:
            정확한 중복 쌍 리스트
        """
        content_hashes = {}
        duplicates = []
        
        print("🔍 1단계: 해시 기반 정확한 중복 검사...")
        
        for i, article in enumerate(articles):
            content = article.get('content', '')
            if not content.strip():
                continue
                
            # 정규화된 본문으로 해시 생성
            normalized_content = self.normalize_text(content)
            if len(normalized_content) < 50:  # 너무 짧은 본문 제외
                continue
                
            content_hash = hashlib.md5(normalized_content.encode('utf-8')).hexdigest()
            
            if content_hash in content_hashes:
                # 정확한 중복 발견
                original_idx = content_hashes[content_hash]
                similarity_result = SimilarityResult(
                    similarity_score=1.0,
                    is_duplicate=True,
                    similarity_type='exact_content',
                    threshold=1.0
                )
                duplicates.append((original_idx, i, similarity_result))
            else:
                content_hashes[content_hash] = i
        
        print(f"✅ 정확한 중복 {len(duplicates)}개 발견")
        return duplicates
    
    def find_signature_duplicates(self, articles: List[dict], excluded_indices: Set[int]) -> List[Tuple[int, int, SimilarityResult]]:
        """
        제목 + 리드 문단 기반 빠른 중복 찾기 (O(n))
        
        Args:
            articles: 기사 리스트
            excluded_indices: 이미 중복으로 판정된 인덱스들 (제외)
            
        Returns:
            시그니처 기반 중복 쌍 리스트
        """
        signatures = {}
        duplicates = []
        
        print("🔍 2단계: 제목+리드 기반 빠른 중복 검사...")
        
        for i, article in enumerate(articles):
            if i in excluded_indices:
                continue
                
            title = article.get('title', '').strip()
            content = article.get('content', '').strip()
            
            if not title and not content:
                continue
            
            # 시그니처 생성: 제목 + 본문 첫 200자
            lead_content = content[:200] if content else ""
            signature_text = f"{title}|{lead_content}"
            normalized_signature = self.normalize_text(signature_text)
            
            if len(normalized_signature) < 20:  # 너무 짧은 시그니처 제외
                continue
            
            # 시그니처 해시 생성
            signature_hash = hashlib.md5(normalized_signature.encode('utf-8')).hexdigest()
            
            if signature_hash in signatures:
                # 시그니처 중복 발견
                original_idx = signatures[signature_hash]
                
                # 실제 유사도 계산으로 검증
                similarity_result = self.calculate_content_similarity(
                    articles[original_idx].get('content', ''),
                    content
                )
                
                # 임계값 이상이면 중복으로 판정
                if similarity_result.similarity_score >= 0.8:  # 시그니처는 좀 더 관대하게
                    similarity_result.similarity_type = 'signature_content'
                    duplicates.append((original_idx, i, similarity_result))
            else:
                signatures[signature_hash] = i
        
        print(f"✅ 시그니처 중복 {len(duplicates)}개 발견")
        return duplicates
    
    def find_title_length_duplicates(self, articles: List[dict], excluded_indices: Set[int]) -> List[Tuple[int, int, SimilarityResult]]:
        """
        제목 길이 + 키워드 기반 의심 중복 찾기 (O(n))
        
        Args:
            articles: 기사 리스트
            excluded_indices: 이미 중복으로 판정된 인덱스들 (제외)
            
        Returns:
            길이 기반 의심 중복 쌍 리스트
        """
        length_groups = {}
        duplicates = []
        
        print("🔍 3단계: 제목 길이 기반 의심 중복 검사...")
        
        # 길이별 그룹화
        for i, article in enumerate(articles):
            if i in excluded_indices:
                continue
                
            title = article.get('title', '').strip()
            content = article.get('content', '').strip()
            
            if not content or len(content) < 100:
                continue
            
            # 본문 길이를 100자 단위로 그룹화
            content_length_bucket = len(content) // 100
            
            if content_length_bucket not in length_groups:
                length_groups[content_length_bucket] = []
            
            length_groups[content_length_bucket].append((i, article))
        
        # 각 길이 그룹 내에서 제목 유사도 검사
        total_suspicious = 0
        for length_bucket, group in length_groups.items():
            if len(group) < 2:
                continue
                
            # 그룹 내에서만 제목 비교 (O(n²)이지만 그룹 크기가 작음)
            for i in range(len(group)):
                for j in range(i + 1, len(group)):
                    idx1, article1 = group[i]
                    idx2, article2 = group[j]
                    
                    title1 = article1.get('title', '')
                    title2 = article2.get('title', '')
                    
                    # 제목 유사도 검사
                    title_similarity = self.calculate_title_similarity(title1, title2)
                    
                    # 제목이 매우 유사하면 본문도 검사
                    if title_similarity.similarity_score >= 0.7:
                        content_similarity = self.calculate_content_similarity(
                            article1.get('content', ''),
                            article2.get('content', '')
                        )
                        
                        if content_similarity.is_duplicate:
                            content_similarity.similarity_type = 'length_group_content'
                            duplicates.append((idx1, idx2, content_similarity))
                            total_suspicious += 1
        
        print(f"✅ 길이 그룹 기반 중복 {len(duplicates)}개 발견")
        return duplicates
    
    def find_hybrid_duplicates(self, articles: List[dict]) -> List[Tuple[int, int, SimilarityResult]]:
        """
        하이브리드 중복 검사 (단계별 접근)
        
        Args:
            articles: 기사 리스트
            
        Returns:
            모든 중복 쌍 리스트
        """
        print(f"🚀 하이브리드 중복 검사 시작 ({len(articles)}개 기사)")
        print("=" * 50)
        
        all_duplicates = []
        excluded_indices = set()
        
        # 1단계: 해시 기반 정확한 중복
        exact_duplicates = self.find_exact_duplicates(articles)
        all_duplicates.extend(exact_duplicates)
        
        # 중복으로 판정된 기사들을 제외 목록에 추가
        for _, duplicate_idx, _ in exact_duplicates:
            excluded_indices.add(duplicate_idx)
        
        # 2단계: 시그니처 기반 빠른 중복
        signature_duplicates = self.find_signature_duplicates(articles, excluded_indices)
        all_duplicates.extend(signature_duplicates)
        
        # 추가로 중복 판정된 기사들을 제외 목록에 추가
        for _, duplicate_idx, _ in signature_duplicates:
            excluded_indices.add(duplicate_idx)
        
        # 3단계: 길이 그룹 기반 의심 중복 (선택적)
        if len(articles) - len(excluded_indices) < 500:  # 남은 기사가 적으면 정밀 검사
            length_duplicates = self.find_title_length_duplicates(articles, excluded_indices)
            all_duplicates.extend(length_duplicates)
        
        print("=" * 50)
        print(f"🎯 총 {len(all_duplicates)}개 중복 발견")
        print(f"📊 제외된 기사: {len(excluded_indices)}개")
        print(f"📊 유지될 기사: {len(articles) - len(excluded_indices)}개")
        
        return all_duplicates
    
    def find_all_duplicates(self, articles: List[dict]) -> dict:
        """
        모든 중복 찾기
        
        Args:
            articles: 기사 리스트
            
        Returns:
            중복 정보 딕셔너리
        """
        title_duplicates = self.find_duplicate_titles(articles)
        content_duplicates = self.find_duplicate_contents(articles)
        
        return {
            'title_duplicates': title_duplicates,
            'content_duplicates': content_duplicates,
            'total_title_duplicates': len(title_duplicates),
            'total_content_duplicates': len(content_duplicates)
        }

# 사용 예시
if __name__ == "__main__":
    # 테스트 데이터
    test_articles = [
        {
            'id': 1,
            'title': '정치 뉴스 1',
            'content': '이것은 정치 뉴스 내용입니다.',
            'published_at': '2024-01-01'
        },
        {
            'id': 2,
            'title': '정치 뉴스 1',  # 중복 제목
            'content': '이것은 다른 정치 뉴스 내용입니다.',
            'published_at': '2024-01-02'
        },
        {
            'id': 3,
            'title': '경제 뉴스 1',
            'content': '이것은 정치 뉴스 내용입니다.',  # 중복 본문
            'published_at': '2024-01-03'
        }
    ]
    
    # 유사도 계산기 생성
    calculator = SimilarityCalculator()
    
    # 중복 찾기
    duplicates = calculator.find_all_duplicates(test_articles)
    
    print("중복 제목:", duplicates['title_duplicates'])
    print("중복 본문:", duplicates['content_duplicates'])
