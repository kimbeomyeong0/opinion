#!/usr/bin/env python3
"""
고급 클러스터링 파이프라인 스크립트
- HDBSCAN 클러스터링
- 키워드 기반 제목 생성
- 임베딩 기반 중복 통합
- 최종 이슈 저장
"""

import time
import numpy as np
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import warnings
import logging
warnings.filterwarnings('ignore')

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 필요한 라이브러리 import

try:
    import umap
    import hdbscan
    from sklearn.metrics.pairwise import cosine_similarity
    import psutil  # 성능 모니터링용
except ImportError as e:
    print("❌ 필요한 라이브러리가 설치되지 않았습니다.")
    print("pip install umap-learn hdbscan scikit-learn psutil")
    print(f"오류 세부사항: {e}")
    exit(1)

from utils.supabase_manager import SupabaseManager


class ClusteringError(Exception):
    """클러스터링 관련 예외"""
    pass


class ClusteringConfig:
    """클러스터링 설정 관리"""
    
    # 불용어 목록
    STOP_WORDS = {
        '관련', '이슈', '기사', '뉴스', '보도', '논란', '사태', '문제', '이야기', '소식', '전망', 
        '분석', '평가', '검토', '논의', '협의', '결정', '발표', '공개', '확인', '조사', '수사', 
        '재판', '판결', '기소', '구속', '체포', '조사', '확인', '발표', '공개', '결정', '논의', 
        '협의', '평가', '검토', '분석', '전망', '소식', '이야기', '이슈', '문제', '사태', '논란', 
        '뉴스', '기사', '보도'
    }
    
    # 성향별 분류 설정 (사용 중단 - 부정확한 데이터 생성 방지)
    # BIAS_DISTRIBUTION = {
    #     'left_ratio': 1/3,
    #     'center_ratio': 1/3, 
    #     'right_ratio': 1/3
    # }
    # → 실제 언론사 성향 데이터 없이 가짜 분류는 의미없음
    
    # 클러스터링 임계값 (고품질 이슈 생성 최적화)
    THRESHOLDS = {
        'min_cluster_size': 3,         # HDBSCAN과 일치: 최소 3개 기사
        'merge_threshold': 0.9,        # 매우 엄격한 통합 기준 (0.6→0.9)
        'separate_threshold': 0.8,     # 높은 분리 기준
        'title_similarity_threshold': 0.2,  # 제목 유사도 상향
        'max_cluster_size': 50,        # 대형 클러스터 분할 기준
        'noise_ratio_threshold': 0.7,  # 노이즈 비율 임계값
        'quality_threshold': 0.3,      # 클러스터 품질 최소 기준
        'top_clusters_limit': 3        # 상위 3개 클러스터만 저장
    }
    
    # 키워드 설정
    KEYWORD_SETTINGS = {
        'max_keywords': 15,
        'min_frequency': 2
    }


class AdvancedClusteringPipeline:
    """고급 클러스터링 파이프라인 클래스"""
    
    def __init__(self, batch_size: int = 100):
        """초기화"""
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise ClusteringError("Supabase 연결 실패")
        
        self.batch_size = batch_size
        
        # 성능 모니터링
        self.performance_stats = {
            'total_articles_processed': 0,
            'successful_clusters': 0,
            'processing_time': 0,
            'memory_usage_peak': 0
        }
        
        # UMAP 파라미터 (3072차원 전면 적용 최적화)
        self.umap_params = {
            'n_neighbors': 20,   # 더 많은 이웃으로 안정적 매니폴드 학습
            'n_components': 512, # 정보 손실 최소화 (83% 정보 보존)
            'min_dist': 0.0001, # 매우 조밀한 클러스터 형성
            'metric': 'cosine',  # OpenAI 임베딩에 최적화
            'random_state': 42,
            'n_jobs': -1,       # 병렬 처리 활성화
            'verbose': False,   # 로그 출력 제어
            'low_memory': True  # 메모리 효율성 향상
        }
        
        # HDBSCAN 파라미터 (512차원 최적화 + 최소 클러스터 크기 3)
        self.hdbscan_params = {
            'min_cluster_size': 3,     # 최소 클러스터 크기: 3개 기사 이상
            'min_samples': 2,          # 코어 포인트 기준: 더 민감한 탐지
            'metric': 'euclidean',     # UMAP 축소 후 유클리드 거리
            'cluster_selection_epsilon': 0.05,  # 더 세밀한 클러스터 분리
            'cluster_selection_method': 'eom',   # Excess of Mass 방법
            'core_dist_n_jobs': -1,    # 병렬 처리 활성화
            'algorithm': 'best'        # 최적 알고리즘 자동 선택
        }
        
        # 설정 관리
        self.config = ClusteringConfig()
        
        # 성능 최적화 설정
        import os
        self.n_jobs = min(os.cpu_count() or 1, 8)  # CPU 코어 수 제한
        print(f"💻 병렬 처리 코어 수: {self.n_jobs}")
    
    def optimize_embedding_processing(self, articles: List[Dict[str, Any]]) -> Tuple[np.ndarray, List[Dict[str, Any]]]:
        """pgvector 네이티브 벡터 처리 최적화"""
        embeddings = []
        valid_articles = []
        
        for article in articles:
            embedding_data = article.get('embedding')
            if not embedding_data:
                continue
                
            # pgvector는 자동으로 리스트 형태로 반환 (네이티브 처리)
            if isinstance(embedding_data, list):
                # 3072차원 벡터 검증
                if len(embedding_data) == 3072:
                    embeddings.append(embedding_data)
                    valid_articles.append(article)
                else:
                    print(f"⚠️ 예상치 못한 벡터 차원: {len(embedding_data)}차원 - {article['id']}")
                    continue
            elif isinstance(embedding_data, str):
                # 레거시 JSON 문자열 지원 (하위 호환성)
                try:
                    import json
                    embedding_list = json.loads(embedding_data)
                    if isinstance(embedding_list, list) and len(embedding_list) == 3072:
                        embeddings.append(embedding_list)
                        valid_articles.append(article)
                    else:
                        print(f"⚠️ 잘못된 JSON 벡터 형식: {article['id']}")
                        continue
                except (json.JSONDecodeError, ValueError) as e:
                    print(f"⚠️ JSON 파싱 실패: {article['id']} - {str(e)}")
                    continue
                except Exception as e:
                    print(f"⚠️ 임베딩 파싱 실패: {article['id']} - {str(e)}")
                    continue
            else:
                print(f"⚠️ 알 수 없는 임베딩 타입: {type(embedding_data)} - {article['id']}")
                continue
        
        if len(embeddings) > 0:
            # 메모리 효율적인 numpy 배열 생성 (float32 사용)
            return np.array(embeddings, dtype=np.float32), valid_articles
        else:
            return np.array([]), []

    def fetch_articles_by_category(self, category: str) -> List[Dict[str, Any]]:
        """카테고리별 기사 조회 (임베딩 + 언론사 성향 정보 포함)"""
        try:
            result = self.supabase_manager.client.table('articles').select(
                'id, title, lead_paragraph, political_category, embedding, media_id'
            ).eq('political_category', category).eq('is_preprocessed', True).execute()
            
            return result.data
        except Exception as e:
            print(f"❌ {category} 카테고리 기사 조회 실패: {str(e)}")
            return []
    
    
    def reduce_dimensions(self, embeddings: np.ndarray) -> np.ndarray:
        """UMAP 차원 축소"""
        try:
            reducer = umap.UMAP(**self.umap_params)
            reduced_embeddings = reducer.fit_transform(embeddings)
            return reduced_embeddings
        except Exception as e:
            print(f"❌ 차원 축소 실패: {str(e)}")
            return embeddings
    
    def perform_clustering(self, embeddings: np.ndarray) -> np.ndarray:
        """HDBSCAN 군집화"""
        try:
            clusterer = hdbscan.HDBSCAN(**self.hdbscan_params)
            cluster_labels = clusterer.fit_predict(embeddings)
            return cluster_labels
        except Exception as e:
            print(f"❌ 군집화 실패: {str(e)}")
            return np.array([-1] * len(embeddings))
    
    def perform_smart_clustering(self, embeddings: np.ndarray) -> np.ndarray:
        """스마트 클러스터링: UMAP 우선 적용 전략"""
        try:
            n_samples, n_features = embeddings.shape
            print(f"    📋 입력 데이터: {n_samples:,}개 샘플, {n_features}차원")
            
            # 메모리 사용량 예측 (정보 제공용)
            memory_usage_gb = (n_samples * n_features * 4) / (1024**3)  # float32 기준
            print(f"    💾 원본 메모리 사용량: {memory_usage_gb:.2f}GB")
            
            # 3072차원: 항상 UMAP 적용 (일관성 및 성능 최적화)
            if n_features == 3072:
                print(f"    🎯 3072차원 감지 → UMAP으로 512차원 축소 적용")
                print(f"    📊 예상 효과: 메모리 83% 절약, 처리속도 10-50배 향상")
                
                # UMAP 차원 축소 적용
                reduced_embeddings = self.reduce_dimensions(embeddings)
                reduced_memory_gb = (n_samples * 512 * 4) / (1024**3)
                print(f"    💾 축소 후 메모리: {reduced_memory_gb:.2f}GB (절약: {memory_usage_gb - reduced_memory_gb:.2f}GB)")
                
                # 축소된 차원으로 클러스터링
                return self.perform_clustering(reduced_embeddings)
                
            # 512차원 이하: 직접 클러스터링 (이미 최적 차원)
            elif n_features <= 512:
                print(f"    ✅ 최적 차원 범위 ({n_features}차원), 직접 HDBSCAN 적용")
                clusterer = hdbscan.HDBSCAN(**self.hdbscan_params)
                return clusterer.fit_predict(embeddings)
                
            # 512~3072차원: UMAP으로 512차원 축소
            else:
                print(f"    📉 고차원 ({n_features}차원) → 512차원 축소 후 클러스터링")
                reduced_embeddings = self.reduce_dimensions(embeddings)
                return self.perform_clustering(reduced_embeddings)
                    
        except Exception as e:
            print(f"❌ 스마트 클러스터링 실패: {str(e)}")
            import traceback
            traceback.print_exc()
            return np.array([-1] * len(embeddings))
    
    def extract_keywords_from_articles(self, articles: List[Dict[str, Any]]) -> List[str]:
        """기사들에서 핵심 키워드 추출"""
        if not articles:
            return []
        
        # 모든 제목과 리드문단 수집
        all_texts = []
        for article in articles:
            all_texts.append(article['title'])
            if article.get('lead_paragraph'):
                all_texts.append(article['lead_paragraph'])
        
        # 단어 추출 및 정제
        words = []
        for text in all_texts:
            text_words = text.replace('"', '').replace("'", '').split()
            words.extend(text_words)
        
        # 빈도수 계산
        word_freq = {}
        for word in words:
            word = word.strip('.,!?()[]{}"\'')
            if len(word) > 1 and word not in self.config.STOP_WORDS and not word.isdigit():
                word_freq[word] = word_freq.get(word, 0) + 1
        
        # 상위 키워드 반환 (설정 기반)
        max_keywords = self.config.KEYWORD_SETTINGS['max_keywords']
        min_frequency = self.config.KEYWORD_SETTINGS['min_frequency']
        top_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:max_keywords]
        return [word for word, freq in top_words if freq >= min_frequency]
    
    def create_keyword_based_title(self, articles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """키워드 기반 클러스터 정보 생성 (키워드 배열 + 표시용 제목)"""
        if not articles:
            return {
                'keywords': [],
                'display_title': "미분류 이슈",
                'keyword_count': 0
            }
        
        keywords = self.extract_keywords_from_articles(articles)
        
        # 핵심 키워드 추출 (최대 8개, 최소 빈도 2 이상)
        top_keywords = keywords[:8] if len(keywords) >= 8 else keywords
        
        # 키워드 수에 따른 처리
        if len(top_keywords) >= 5:
            # 5개 이상: 상위 5개 키워드 사용
            selected_keywords = top_keywords[:5]
            display_title = " ".join(selected_keywords)
        elif len(top_keywords) >= 2:
            # 2-4개: 모든 키워드 사용
            selected_keywords = top_keywords
            display_title = " ".join(selected_keywords)
        elif len(top_keywords) == 1:
            # 1개: 단일 키워드
            selected_keywords = top_keywords
            display_title = top_keywords[0]
        else:
            # 키워드 없음: 기사 수 표시
            selected_keywords = []
            display_title = f"{len(articles)}개_기사_클러스터"
        
        return {
            'keywords': selected_keywords,           # 순수 키워드 배열
            'display_title': display_title,         # 표시용 제목 (간결)
            'keyword_count': len(selected_keywords), # 키워드 개수
            'total_articles': len(articles)          # 기사 수
        }
    
    def calculate_keyword_similarity(self, keywords1: List[str], keywords2: List[str]) -> float:
        """키워드 배열 기반 유사도 계산 (Jaccard 유사도)"""
        if not keywords1 or not keywords2:
            return 0.0
            
        set1, set2 = set(keywords1), set(keywords2)
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        if union == 0:
            return 0.0
            
        jaccard_similarity = intersection / union
        
        # 가중치 추가: 키워드 수가 비슷할수록 더 유사한 것으로 판단
        size_similarity = 1 - abs(len(keywords1) - len(keywords2)) / max(len(keywords1), len(keywords2))
        
        # 최종 유사도: Jaccard(80%) + 크기 유사도(20%)
        final_similarity = jaccard_similarity * 0.8 + size_similarity * 0.2
        
        return final_similarity
    
    def calculate_title_similarity(self, title1: str, title2: str) -> float:
        """레거시 지원: 기존 제목 기반 유사도 계산"""
        # 제목에서 키워드 추출
        keywords1 = title1.replace('관련 이슈', '').replace('클러스터', '').replace('_', ' ').split()
        keywords2 = title2.replace('관련 이슈', '').replace('클러스터', '').replace('_', ' ').split()
        
        return self.calculate_keyword_similarity(keywords1, keywords2)
    
    def group_similar_clusters(self, clusters: List[Dict[str, Any]]) -> List[List[int]]:
        """비슷한 키워드 클러스터끼리 그룹핑 (최적화된 버전)"""
        groups = []
        used_indices = set()
        
        for i, cluster in enumerate(clusters):
            if i in used_indices:
                continue
            
            # 현재 클러스터와 유사한 클러스터들 찾기
            similar_group = [i]
            used_indices.add(i)
            
            current_keywords = cluster.get('keywords', [])
            
            for j, other_cluster in enumerate(clusters):
                if j in used_indices:
                    continue
                
                other_keywords = other_cluster.get('keywords', [])
                
                # 키워드 배열 기반 유사도 계산
                if current_keywords and other_keywords:
                    similarity = self.calculate_keyword_similarity(current_keywords, other_keywords)
                else:
                    # 키워드가 없는 경우 레거시 방식 사용
                    similarity = self.calculate_title_similarity(
                        cluster['title'], other_cluster['title']
                    )
                
                if similarity >= self.config.THRESHOLDS['title_similarity_threshold']:
                    similar_group.append(j)
                    used_indices.add(j)
            
            groups.append(similar_group)
        
        return groups
    
    def group_similar_titles(self, clusters: List[Dict[str, Any]]) -> List[List[int]]:
        """레거시 지원: 기존 제목 기반 그룹핑"""
        return self.group_similar_clusters(clusters)
    
    def calculate_embedding_similarity(self, articles1: List[Dict[str, Any]], articles2: List[Dict[str, Any]]) -> float:
        """두 클러스터의 기사들 간 임베딩 유사도 계산 (최적화된 임베딩 처리 사용)"""
        try:
            # 최적화된 임베딩 처리 메서드 사용
            embeddings1, _ = self.optimize_embedding_processing(articles1)
            embeddings2, _ = self.optimize_embedding_processing(articles2)
            
            if len(embeddings1) == 0 or len(embeddings2) == 0:
                return 0.0
            
            # 각 클러스터의 평균 임베딩 계산
            avg_embedding1 = np.mean(embeddings1, axis=0)
            avg_embedding2 = np.mean(embeddings2, axis=0)
            
            # 코사인 유사도 계산
            similarity = cosine_similarity([avg_embedding1], [avg_embedding2])[0][0]
            
            return float(similarity)
            
        except Exception as e:
            print(f"❌ 임베딩 유사도 계산 실패: {str(e)}")
            return 0.0
    
    def merge_similar_clusters(self, clusters: List[Dict[str, Any]], groups: List[List[int]]) -> List[Dict[str, Any]]:
        """유사한 클러스터들 통합"""
        merged_clusters = []
        
        for group in groups:
            if len(group) == 1:
                # 그룹에 클러스터가 하나만 있으면 그대로 유지
                merged_clusters.append(clusters[group[0]])
            else:
                # 여러 클러스터가 있으면 임베딩 유사도로 통합 여부 결정
                print(f"  🔍 {len(group)}개 클러스터 그룹 검토 중...")
                
                # 첫 번째 클러스터를 기준으로 시작
                merged_cluster = clusters[group[0]].copy()
                merged_articles = merged_cluster['articles'].copy()
                
                # 나머지 클러스터들과 유사도 계산
                for i in range(1, len(group)):
                    current_cluster = clusters[group[i]]
                    
                    # 임베딩 유사도 계산
                    similarity = self.calculate_embedding_similarity(
                        merged_articles, current_cluster['articles']
                    )
                    
                    print(f"    📊 유사도: {similarity:.3f}")
                    
                    if similarity >= self.config.THRESHOLDS['merge_threshold']:
                        # 통합
                        merged_articles.extend(current_cluster['articles'])
                        print(f"    ✅ 통합: {current_cluster['title']}")
                    else:
                        # 분리 (별도 클러스터로 유지)
                        merged_clusters.append(current_cluster)
                        print(f"    ❌ 분리: {current_cluster['title']}")
                
                # 통합된 클러스터 업데이트
                merged_cluster['articles'] = merged_articles
                merged_cluster['title'] = self.create_keyword_based_title(merged_articles)
                merged_clusters.append(merged_cluster)
        
        return merged_clusters
    
    def get_real_bias_distribution(self, articles: List[Dict[str, Any]]) -> Tuple[int, int, int]:
        """실제 언론사 성향 기반 분류 (media_outlets 테이블 참조)"""
        left_count = center_count = right_count = 0
        
        try:
            for article in articles:
                media_id = article.get('media_id')
                if not media_id:
                    continue
                    
                # media_outlets 테이블에서 성향 정보 조회
                media_info = self.supabase_manager.client.table('media_outlets').select(
                    'bias'
                ).eq('id', media_id).execute()
                
                if media_info.data:
                    bias = media_info.data[0].get('bias', 'center')
                    if bias == 'left':
                        left_count += 1
                    elif bias == 'right':
                        right_count += 1
                    else:  # center 또는 기타
                        center_count += 1
                else:
                    # 언론사 정보 없으면 중도로 처리
                    center_count += 1
                    
        except Exception as e:
            print(f"    ⚠️ 성향 분류 오류: {str(e)}")
            # 오류 시 전체를 중도로 처리
            center_count = len(articles)
            left_count = right_count = 0
            
        return left_count, center_count, right_count
    
    def get_cluster_statistics(self, articles: List[Dict[str, Any]]) -> Dict[str, int]:
        """클러스터 통계 정보 계산"""
        total_articles = len(articles)
        
        return {
            'total_count': total_articles,
            'avg_per_day': total_articles // 7 if total_articles >= 7 else total_articles,
            'cluster_density': min(1.0, total_articles / 10.0)
        }
    
    def save_cluster_to_issues(self, cluster_articles: List[Dict[str, Any]], cluster_id: int) -> Optional[str]:
        """클러스터를 issues 테이블에 저장 (실제 언론사 성향 기반)"""
        try:
            # 이슈 정보 생성
            issue_info = self.create_keyword_based_title(cluster_articles)
            
            # 실제 언론사 성향 기반 분류
            left_count, center_count, right_count = self.get_real_bias_distribution(cluster_articles)
            total_count = len(cluster_articles)
            
            print(f"      📈 성향 분포: 좌({left_count}) 중({center_count}) 우({right_count}) = 총 {total_count}개")
            
            # issues 테이블에 저장
            issue_data = {
                'title': issue_info['display_title'],  # 키워드 기반 제목
                'source': str(total_count),            # 전체 기사 수
                'left_source': str(left_count),        # 실제 좌편향 기사 수
                'center_source': str(center_count),    # 실제 중도 기사 수
                'right_source': str(right_count),      # 실제 우편향 기사 수
                'created_at': datetime.now().isoformat()
            }
            
            result = self.supabase_manager.client.table('issues').insert(issue_data).execute()
            issue_id = result.data[0]['id']
            
            # issue_articles 테이블에 저장
            issue_articles_data = []
            for article in cluster_articles:
                issue_articles_data.append({
                    'issue_id': issue_id,
                    'article_id': article['id']
                })
            
            if issue_articles_data:
                self.supabase_manager.client.table('issue_articles').insert(issue_articles_data).execute()
            
            return issue_id
            
        except Exception as e:
            print(f"❌ 클러스터 저장 실패: {str(e)}")
            return None
    
    def _prepare_embeddings_for_category(self, category: str) -> Tuple[np.ndarray, List[Dict[str, Any]]]:
        """카테고리별 임베딩 데이터 준비"""
        print(f"  🔄 임베딩 처리 중...")
        
        # 기사 조회
        articles = self.fetch_articles_by_category(category)
        if not articles:
            print(f"❌ {category} 카테고리에 처리할 기사가 없습니다.")
            return np.array([]), []
        
        print(f"    📰 조회된 기사: {len(articles):,}개")
        
        # 최적화된 임베딩 처리
        embeddings, valid_articles = self.optimize_embedding_processing(articles)
        
        if len(embeddings) == 0:
            print(f"❌ {category} 유효한 임베딩이 없습니다. 먼저 embeddings.py를 실행하세요.")
            return np.array([]), []
        
        print(f"    📊 임베딩 배열 형태: {embeddings.shape}")
        print(f"    📊 임베딩 차원: {embeddings.shape[1] if len(embeddings.shape) > 1 else 'N/A'}")
        print(f"    📊 처리된 기사: {len(valid_articles)}개")
        
        return embeddings, valid_articles
    
    def _create_clusters_from_labels(self, cluster_labels: np.ndarray, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """클러스터 라벨로부터 클러스터 객체 생성 (최소 크기 3 검증)"""
        unique_clusters = np.unique(cluster_labels)
        clusters = []
        noise_count = np.sum(cluster_labels == -1)  # 노이즈 개수
        
        print(f"    📈 HDBSCAN 결과: {len(unique_clusters)-1}개 클러스터, 노이즈 {noise_count}개")
        
        for cluster_id in unique_clusters:
            if cluster_id == -1:  # 노이즈 스킵
                continue
            
            cluster_mask = cluster_labels == cluster_id
            cluster_articles = [articles[i] for i in range(len(articles)) if cluster_mask[i]]
            cluster_size = len(cluster_articles)
            
            # 최소 크기 3 검증
            if cluster_size >= self.config.THRESHOLDS['min_cluster_size']:
                cluster_info = self.create_keyword_based_title(cluster_articles)
                
                # 클러스터 품질 평가
                quality_score = min(1.0, cluster_size / 10.0)  # 10개 이상이면 최고 품질
                
                clusters.append({
                    'id': cluster_id,
                    'title': cluster_info['display_title'],      # 표시용 제목
                    'keywords': cluster_info['keywords'],        # 순수 키워드 배열
                    'keyword_count': cluster_info['keyword_count'],
                    'articles': cluster_articles,
                    'size': cluster_size,
                    'quality_score': quality_score,              # 품질 점수
                    'density': cluster_size / (noise_count + len(articles))  # 밀도
                })
                print(f"      ✅ 클러스터 {cluster_id}: {cluster_size}개 기사, 키워드 {cluster_info['keyword_count']}개")
            else:
                print(f"      ❌ 클러스터 {cluster_id}: {cluster_size}개 기사 (최소 3개 미달로 제외)")
        
        print(f"    🏆 유효 클러스터: {len(clusters)}개 (최소 크기 3 이상)")
        return clusters
    
    def _group_and_merge_clusters(self, clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """클러스터 그룹핑 및 통합 (키워드 기반 최적화)"""
        print(f"  🔍 비슷한 키워드 그룹핑 중...")
        groups = self.group_similar_clusters(clusters)
        print(f"    📊 키워드 그룹: {len(groups)}개")
        
        print(f"  🔄 임베딩 기반 통합 중...")
        merged_clusters = self.merge_similar_clusters(clusters, groups)
        print(f"    📊 최종 클러스터: {len(merged_clusters)}개")
        
        return merged_clusters
    
    def _save_clusters_to_database(self, clusters: List[Dict[str, Any]]) -> int:
        """클러스터들을 데이터베이스에 저장 (상위 3개만 선별 저장)"""
        if not clusters:
            return 0
            
        # 기사 수(source) 기준으로 내림차순 정렬
        sorted_clusters = sorted(clusters, key=lambda x: len(x['articles']), reverse=True)
        
        print(f"  📈 전체 클러스터: {len(sorted_clusters)}개")
        for i, cluster in enumerate(sorted_clusters[:5], 1):  # 상위 5개만 미리보기
            print(f"    {i}위: {len(cluster['articles'])}개 기사 - '{cluster['title'][:40]}...'")
        
        # 상위 3개만 선별
        top_clusters = sorted_clusters[:self.config.THRESHOLDS['top_clusters_limit']]
        
        if len(sorted_clusters) > 3:
            excluded_count = len(sorted_clusters) - 3
            excluded_articles = sum(len(cluster['articles']) for cluster in sorted_clusters[3:])
            print(f"  ✂️ 하위 {excluded_count}개 클러스터 제외 ({excluded_articles}개 기사)")
        
        print(f"  🏆 상위 3개 클러스터만 저장:")
        
        # 상위 3개만 저장
        saved_clusters = 0
        for i, cluster in enumerate(top_clusters, 1):
            print(f"    {i}위 저장 중: {len(cluster['articles'])}개 기사 - '{cluster['title']}'")
            issue_id = self.save_cluster_to_issues(cluster['articles'], cluster['id'])
            if issue_id:
                saved_clusters += 1
                print(f"      ✅ 저장 성공: issue_id {issue_id}")
            else:
                print(f"      ❌ 저장 실패")
                
        return saved_clusters
    
    def process_category(self, category: str) -> Dict[str, Any]:
        """카테고리별 고급 클러스터링 처리"""
        print(f"📊 {category} 카테고리 처리 시작...")
        
        # 1. 임베딩 데이터 준비
        embeddings, articles = self._prepare_embeddings_for_category(category)
        if len(embeddings) == 0:
            return {'success': False, 'clusters': 0}
        
        # 2. 스마트 군집화
        print(f"  🎯 스마트 군집화 중...")
        cluster_labels = self.perform_smart_clustering(embeddings)
        
        # 3. 클러스터 생성
        clusters = self._create_clusters_from_labels(cluster_labels, articles)
        print(f"  📊 초기 클러스터: {len(clusters)}개")
        
        # 4. 클러스터 그룹핑 및 통합
        merged_clusters = self._group_and_merge_clusters(clusters)
        
        # 5. 데이터베이스 저장
        saved_clusters = self._save_clusters_to_database(merged_clusters)
        
        print(f"  ✅ {category} 완료: {saved_clusters}개 이슈 생성")
        return {'success': True, 'clusters': saved_clusters}
    
    def run_full_pipeline(self, categories: Optional[List[str]] = None) -> bool:
        """전체 고급 클러스터링 파이프라인 실행 (성능 모니터링 포함)"""
        try:
            print("=" * 60)
            print("🎯 고급 클러스터링 파이프라인 시작 (3072차원 최적화)")
            print("=" * 60)
            
            # 시스템 정보 출력
            memory_info = psutil.virtual_memory()
            print(f"💻 시스템 메모리: {memory_info.total / (1024**3):.1f}GB (사용가능: {memory_info.available / (1024**3):.1f}GB)")
            print(f"💻 CPU 코어: {self.n_jobs}개 병렬 처리")
            
            # 처리할 카테고리 결정
            if categories is None:
                categories = ['국회/정당', '행정부', '사법/검찰', '외교/안보', '정책/경제사회', '선거', '지역정치']
            
            total_clusters = 0
            start_time = time.time()
            peak_memory = 0
            
            for i, category in enumerate(categories, 1):
                print(f"\n📋 [{i}/{len(categories)}] {category} 처리 중...")
                
                # 메모리 사용량 모니터링
                memory_before = psutil.virtual_memory().used / (1024**3)
                
                result = self.process_category(category)
                
                memory_after = psutil.virtual_memory().used / (1024**3)
                memory_used = memory_after - memory_before
                peak_memory = max(peak_memory, memory_after)
                
                if result['success']:
                    total_clusters += result['clusters']
                    print(f"  📋 {category}: {result['clusters']}개 이슈 생성 (메모리: +{memory_used:.2f}GB)")
                else:
                    print(f"  ⚠️ {category}: 처리 실패")
            
            # 최종 결과 및 성능 통계
            total_time = time.time() - start_time
            avg_time_per_category = total_time / len(categories)
            
            print(f"\n{'='*60}")
            print(f"🎉 고급 클러스터링 완료!")
            print(f"✅ 총 생성된 이슈: {total_clusters}개")
            print(f"⏱️  총 소요시간: {total_time/60:.1f}분 (카테고리당 {avg_time_per_category/60:.1f}분)")
            print(f"💾 최대 메모리 사용량: {peak_memory:.2f}GB")
            print(f"📈 평균 이슈 생성률: {total_clusters/len(categories):.1f}개/카테고리")
            print(f"{'='*60}")
            
            # 성능 통계 업데이트
            self.performance_stats.update({
                'total_articles_processed': sum(result.get('articles_count', 0) for result in [self.process_category(cat) for cat in categories]),
                'successful_clusters': total_clusters,
                'processing_time': total_time,
                'memory_usage_peak': peak_memory
            })
            
            return total_clusters > 0
            
        except Exception as e:
            print(f"❌ 고급 클러스터링 파이프라인 실패: {str(e)}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """메인 함수 - 3072차원 pgvector 최적화 버전"""
    try:
        print("🚀 고급 클러스터링 파이프라인 v2.0")
        print("🔋 pgvector 네이티브 + 3072차원 최적화")
        print("💾 스마트 메모리 관리 + 성능 모니터링")
        
        # 고급 클러스터링 파이프라인 실행
        pipeline = AdvancedClusteringPipeline(batch_size=50)
        success = pipeline.run_full_pipeline()
        
        if success:
            print(f"\n✅ 고급 클러스터링 완료!")
            
            # 성능 통계 출력
            stats = pipeline.performance_stats
            if stats['successful_clusters'] > 0:
                print(f"📈 성능 요약:")
                print(f"  - 처리 시간: {stats['processing_time']/60:.1f}분")
                print(f"  - 생성 이슈: {stats['successful_clusters']}개")
                print(f"  - 최대 메모리: {stats['memory_usage_peak']:.2f}GB")
        else:
            print(f"\n❌ 고급 클러스터링 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()