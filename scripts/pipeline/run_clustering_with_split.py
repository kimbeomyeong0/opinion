#!/usr/bin/env python3
"""
대형 클러스터 분할 기능이 추가된 클러스터링 스크립트
- 100개 이상의 대형 클러스터를 하위 클러스터로 분할
- 클러스터 크기 제한 및 계층적 분할 지원
"""

import sys
import os
import json
import numpy as np
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Tuple
from collections import Counter
from sklearn.metrics import silhouette_score
from sklearn.cluster import KMeans
from sklearn.cluster import AgglomerativeClustering

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utils.supabase_manager import SupabaseManager

# HDBSCAN 설치 확인 및 import
try:
    import hdbscan
except ImportError:
    print("❌ HDBSCAN이 설치되지 않았습니다.")
    print("설치 명령: pip install hdbscan")
    sys.exit(1)

class ClusterSplitter:
    """클러스터 분할 기능이 추가된 클러스터링 클래스"""
    
    def __init__(self):
        """초기화"""
        # 기본 설정
        self.MIN_CLUSTER_SIZE = 5
        self.MIN_SAMPLES = 3
        self.CLUSTER_SELECTION_EPSILON = 0.2
        self.METRIC = 'euclidean'
        
        # 클러스터 크기 제한
        self.MAX_CLUSTER_SIZE = 50  # 최대 클러스터 크기
        self.SPLIT_THRESHOLD = 100  # 분할 임계값
        
        # 품질 검증 임계값
        self.MIN_SILHOUETTE_SCORE = 0.3
        self.MIN_CLUSTER_COHERENCE = 0.6
        
        # Supabase 연결
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
    
    def fetch_embeddings_data(self) -> tuple:
        """임베딩 데이터 조회 및 품질 필터링"""
        try:
            print("📡 임베딩 데이터 조회 중...")
            
            result = self.supabase_manager.client.table('articles_embeddings').select(
                'id, cleaned_article_id, article_id, media_id, embedding_vector, model_name'
            ).execute()
            
            if not result.data:
                print("❌ 임베딩 데이터가 없습니다.")
                return None, None, None
            
            print(f"✅ {len(result.data)}개 임베딩 데이터 조회 완료")
            
            # 벡터 파싱 및 품질 필터링
            embeddings = []
            article_ids = []
            article_metadata = []
            
            for item in result.data:
                try:
                    vector = json.loads(item['embedding_vector'])
                    
                    if len(vector) == 1536:
                        vector_norm = np.linalg.norm(vector)
                        if vector_norm > 0.1:
                            embeddings.append(vector)
                            article_ids.append(item['article_id'])
                            article_metadata.append({
                                'id': item['id'],
                                'cleaned_article_id': item['cleaned_article_id'],
                                'article_id': item['article_id'],
                                'media_id': item['media_id']
                            })
                        else:
                            print(f"⚠️ 품질 낮은 벡터 제거 (기사 ID: {item['article_id']})")
                    else:
                        print(f"⚠️ 잘못된 벡터 차원: {len(vector)}차원 (기사 ID: {item['article_id']})")
                        
                except Exception as e:
                    print(f"⚠️ 벡터 파싱 실패 (기사 ID: {item['article_id']}): {str(e)}")
                    continue
            
            print(f"✅ {len(embeddings)}개 고품질 벡터 파싱 완료")
            
            # 벡터 정규화
            embeddings_array = np.array(embeddings)
            norms = np.linalg.norm(embeddings_array, axis=1, keepdims=True)
            normalized_embeddings = embeddings_array / norms
            
            return normalized_embeddings, article_ids, article_metadata
            
        except Exception as e:
            print(f"❌ 임베딩 데이터 조회 실패: {str(e)}")
            return None, None, None
    
    def perform_hdbscan_clustering(self, embeddings: np.ndarray) -> tuple:
        """HDBSCAN 클러스터링 수행"""
        try:
            print("🔄 HDBSCAN 클러스터링 수행 중...")
            
            clusterer = hdbscan.HDBSCAN(
                min_cluster_size=self.MIN_CLUSTER_SIZE,
                min_samples=self.MIN_SAMPLES,
                cluster_selection_epsilon=self.CLUSTER_SELECTION_EPSILON,
                metric=self.METRIC
            )
            
            cluster_labels = clusterer.fit_predict(embeddings)
            
            # 결과 분석
            unique_labels = np.unique(cluster_labels)
            n_clusters = len(unique_labels) - (1 if -1 in unique_labels else 0)
            n_noise = list(cluster_labels).count(-1)
            
            print(f"✅ HDBSCAN 클러스터링 완료:")
            print(f"  - 클러스터 수: {n_clusters}개")
            print(f"  - 노이즈 포인트: {n_noise}개")
            print(f"  - 클러스터링 비율: {((len(cluster_labels) - n_noise) / len(cluster_labels) * 100):.1f}%")
            
            return cluster_labels, clusterer
            
        except Exception as e:
            print(f"❌ HDBSCAN 클러스터링 실패: {str(e)}")
            return None, None
    
    def split_large_cluster(self, embeddings: np.ndarray, cluster_labels: np.ndarray, 
                           article_ids: List[str], article_metadata: List[Dict], 
                           cluster_id: int) -> tuple:
        """
        대형 클러스터를 하위 클러스터로 분할
        
        Args:
            embeddings: 전체 임베딩 벡터
            cluster_labels: 클러스터 라벨
            article_ids: 기사 ID 리스트
            article_metadata: 기사 메타데이터
            cluster_id: 분할할 클러스터 ID
            
        Returns:
            tuple: (새로운 라벨, 새로운 기사 ID, 새로운 메타데이터)
        """
        try:
            print(f"🔀 클러스터 {cluster_id} 분할 중... (크기: {list(cluster_labels).count(cluster_id)}개)")
            
            # 해당 클러스터의 데이터 추출
            cluster_mask = cluster_labels == cluster_id
            cluster_embeddings = embeddings[cluster_mask]
            cluster_article_ids = [article_ids[i] for i in range(len(article_ids)) if cluster_mask[i]]
            cluster_metadata = [article_metadata[i] for i in range(len(article_metadata)) if cluster_mask[i]]
            
            # 최적의 하위 클러스터 수 결정
            n_samples = len(cluster_embeddings)
            optimal_clusters = min(max(2, n_samples // self.MAX_CLUSTER_SIZE), 10)  # 2~10개 사이
            
            print(f"  - {n_samples}개 샘플을 {optimal_clusters}개 하위 클러스터로 분할")
            
            # K-means로 하위 클러스터링
            kmeans = KMeans(n_clusters=optimal_clusters, random_state=42, n_init=10)
            sub_labels = kmeans.fit_predict(cluster_embeddings)
            
            # 새로운 라벨 생성 (기존 클러스터 ID를 하위 클러스터 ID로 변경)
            new_labels = cluster_labels.copy()
            new_article_ids = article_ids.copy()
            new_metadata = article_metadata.copy()
            
            # 최대 클러스터 ID 찾기
            max_existing_label = np.max(cluster_labels[cluster_labels != -1])
            
            # 하위 클러스터 라벨 할당
            for i, (original_idx, sub_label) in enumerate(zip(np.where(cluster_mask)[0], sub_labels)):
                new_cluster_id = max_existing_label + 1 + sub_label
                new_labels[original_idx] = new_cluster_id
            
            print(f"  - 클러스터 {cluster_id} → {optimal_clusters}개 하위 클러스터로 분할 완료")
            
            return new_labels, new_article_ids, new_metadata
            
        except Exception as e:
            print(f"❌ 클러스터 분할 실패: {str(e)}")
            return cluster_labels, article_ids, article_metadata
    
    def split_all_large_clusters(self, embeddings: np.ndarray, cluster_labels: np.ndarray,
                                article_ids: List[str], article_metadata: List[Dict]) -> tuple:
        """
        모든 대형 클러스터를 분할
        """
        try:
            print("🔍 대형 클러스터 검사 및 분할 중...")
            
            current_labels = cluster_labels.copy()
            current_article_ids = article_ids.copy()
            current_metadata = article_metadata.copy()
            
            split_count = 0
            
            while True:
                # 대형 클러스터 찾기
                large_clusters = []
                for label in np.unique(current_labels):
                    if label == -1:
                        continue
                    
                    cluster_size = list(current_labels).count(label)
                    if cluster_size >= self.SPLIT_THRESHOLD:
                        large_clusters.append((label, cluster_size))
                
                if not large_clusters:
                    break
                
                # 가장 큰 클러스터부터 분할
                large_clusters.sort(key=lambda x: x[1], reverse=True)
                largest_cluster_id, largest_size = large_clusters[0]
                
                print(f"  - 대형 클러스터 발견: ID {largest_cluster_id} (크기: {largest_size}개)")
                
                # 분할 수행
                current_labels, current_article_ids, current_metadata = self.split_large_cluster(
                    embeddings, current_labels, current_article_ids, current_metadata, largest_cluster_id
                )
                
                split_count += 1
                
                # 무한 루프 방지
                if split_count > 10:
                    print("⚠️ 분할 횟수 제한에 도달했습니다.")
                    break
            
            print(f"✅ 대형 클러스터 분할 완료: {split_count}개 클러스터 분할됨")
            
            return current_labels, current_article_ids, current_metadata
            
        except Exception as e:
            print(f"❌ 대형 클러스터 분할 실패: {str(e)}")
            return cluster_labels, article_ids, article_metadata
    
    def evaluate_cluster_quality(self, embeddings: np.ndarray, cluster_labels: np.ndarray) -> Dict[str, float]:
        """클러스터 품질 평가"""
        try:
            # 노이즈 제거
            valid_mask = cluster_labels != -1
            if np.sum(valid_mask) < 2:
                return {'silhouette_score': 0.0, 'avg_coherence': 0.0}
            
            valid_embeddings = embeddings[valid_mask]
            valid_labels = cluster_labels[valid_mask]
            
            # 실루엣 점수 계산
            if len(np.unique(valid_labels)) > 1:
                silhouette = silhouette_score(valid_embeddings, valid_labels)
            else:
                silhouette = 0.0
            
            # 클러스터 내 일관성 계산
            coherence_scores = []
            for label in np.unique(valid_labels):
                if label == -1:
                    continue
                
                cluster_mask = valid_labels == label
                cluster_embeddings = valid_embeddings[cluster_mask]
                
                if len(cluster_embeddings) > 1:
                    centroid = np.mean(cluster_embeddings, axis=0)
                    distances = np.linalg.norm(cluster_embeddings - centroid, axis=1)
                    avg_distance = np.mean(distances)
                    coherence = max(0, 1 - avg_distance)
                    coherence_scores.append(coherence)
            
            avg_coherence = np.mean(coherence_scores) if coherence_scores else 0.0
            
            return {
                'silhouette_score': silhouette,
                'avg_coherence': avg_coherence
            }
            
        except Exception as e:
            print(f"⚠️ 품질 평가 실패: {str(e)}")
            return {'silhouette_score': 0.0, 'avg_coherence': 0.0}
    
    def filter_low_quality_clusters(self, embeddings: np.ndarray, cluster_labels: np.ndarray, 
                                  article_ids: List[str], article_metadata: List[Dict]) -> tuple:
        """저품질 클러스터 필터링"""
        try:
            print("🔍 저품질 클러스터 필터링 중...")
            
            filtered_labels = cluster_labels.copy()
            
            # 각 클러스터 품질 평가
            for label in np.unique(cluster_labels):
                if label == -1:  # 노이즈는 그대로 유지
                    continue
                
                cluster_mask = cluster_labels == label
                cluster_embeddings = embeddings[cluster_mask]
                
                if len(cluster_embeddings) < 3:  # 너무 작은 클러스터는 노이즈로 변경
                    filtered_labels[cluster_mask] = -1
                    continue
                
                # 클러스터 내 일관성 검사
                centroid = np.mean(cluster_embeddings, axis=0)
                distances = np.linalg.norm(cluster_embeddings - centroid, axis=1)
                max_distance = np.max(distances)
                
                if max_distance > 0.8:  # 너무 분산된 클러스터는 노이즈로 변경
                    filtered_labels[cluster_mask] = -1
                    print(f"⚠️ 클러스터 {label} 제거 (일관성 부족)")
            
            # 필터링 결과 분석
            original_clusters = len(np.unique(cluster_labels)) - (1 if -1 in cluster_labels else 0)
            filtered_clusters = len(np.unique(filtered_labels)) - (1 if -1 in filtered_labels else 0)
            original_noise = list(cluster_labels).count(-1)
            filtered_noise = list(filtered_labels).count(-1)
            
            print(f"✅ 필터링 완료:")
            print(f"  - 원본 클러스터: {original_clusters}개 → 필터링 후: {filtered_clusters}개")
            print(f"  - 원본 노이즈: {original_noise}개 → 필터링 후: {filtered_noise}개")
            
            return filtered_labels, article_ids, article_metadata
            
        except Exception as e:
            print(f"❌ 필터링 실패: {str(e)}")
            return cluster_labels, article_ids, article_metadata
    
    def clear_existing_data(self) -> bool:
        """기존 이슈 데이터 초기화"""
        try:
            print("🗑️ 기존 이슈 데이터 초기화 중...")
            
            self.supabase_manager.client.table('issue_articles').delete().neq('issue_id', '00000000-0000-0000-0000-000000000000').execute()
            self.supabase_manager.client.table('issues').delete().neq('id', '00000000-0000-0000-0000-000000000000').execute()
            
            print("✅ 기존 이슈 데이터 초기화 완료")
            return True
            
        except Exception as e:
            print(f"❌ 데이터 초기화 실패: {str(e)}")
            return False
    
    def analyze_media_bias(self, article_metadata: List[Dict]) -> Dict[str, str]:
        """성향별 언론사 정보 분석"""
        try:
            media_bias_map = {
                '755f5a31-507f-42d7-aa8c-587a9459896c': 'right',  # 조선일보
                '629d6050-76e2-466a-ae66-f2532d1f359c': 'right',  # 중앙일보
                'afcd9fc8-e4fd-44c7-8d9d-59722bb21b26': 'right',  # 동아일보
                'ea42a075-88e0-4c6e-a21b-8854ec10dec9': 'left',   # 한겨레
                '81324c3e-5f68-4356-bc91-bd5c7719f5c9': 'left',   # 오마이뉴스
                '3847c39d-bc90-44e5-8650-331e67cbe140': 'left',   # 경향신문
                '33e32516-abb5-46ca-b87d-306304a61c34': 'center', # 연합뉴스
                'a29afc6a-1764-43fe-8b0e-3a5962d90402': 'center', # 뉴시스
                'a8fecf98-41ac-4018-85e8-19bfeb702fe5': 'center'  # 뉴스원
            }
            
            media_counts = {}
            for article in article_metadata:
                media_id = article['media_id']
                if media_id in media_bias_map:
                    bias = media_bias_map[media_id]
                    if bias not in media_counts:
                        media_counts[bias] = 0
                    media_counts[bias] += 1
            
            left_count = media_counts.get('left', 0)
            center_count = media_counts.get('center', 0)
            right_count = media_counts.get('right', 0)
            total_count = left_count + center_count + right_count
            
            return {
                'total_source': str(total_count),
                'left_source': str(left_count),
                'center_source': str(center_count),
                'right_source': str(right_count)
            }
            
        except Exception as e:
            print(f"⚠️ 성향 분석 실패: {str(e)}")
            return {
                'total_source': '0',
                'left_source': '0',
                'center_source': '0',
                'right_source': '0'
            }
    
    def create_issue(self, cluster_id: int, article_ids: List[str], article_metadata: List[Dict]) -> Optional[str]:
        """이슈 생성 및 저장"""
        try:
            source_info = self.analyze_media_bias(article_metadata)
            
            issue_data = {
                'date': date.today().isoformat(),
                'title': f'이슈 {cluster_id + 1}',
                'summary': f'{len(article_ids)}개 기사로 구성된 이슈',
                'subtitle': f'클러스터 {cluster_id + 1}',
                'source': source_info['total_source'],
                'left_source': source_info['left_source'],
                'center_source': source_info['center_source'],
                'right_source': source_info['right_source'],
                'created_at': datetime.now().isoformat()
            }
            
            result = self.supabase_manager.client.table('issues').insert(issue_data).execute()
            
            if result.data:
                issue_id = result.data[0]['id']
                print(f"✅ 이슈 {cluster_id + 1} 생성 완료 (ID: {issue_id}) - {len(article_ids)}개 기사")
                return issue_id
            else:
                print(f"❌ 이슈 {cluster_id + 1} 생성 실패")
                return None
                
        except Exception as e:
            print(f"❌ 이슈 생성 실패: {str(e)}")
            return None
    
    def create_issue_articles(self, issue_id: str, article_ids: List[str], article_metadata: List[Dict]) -> bool:
        """이슈-기사 연결 생성"""
        try:
            connections = []
            for i, article_id in enumerate(article_ids):
                cleaned_article_id = None
                for metadata in article_metadata:
                    if metadata['article_id'] == article_id:
                        cleaned_article_id = metadata['cleaned_article_id']
                        break
                
                connections.append({
                    'issue_id': issue_id,
                    'article_id': article_id,
                    'cleaned_article_id': cleaned_article_id,
                    'stance': 'center'
                })
            
            result = self.supabase_manager.client.table('issue_articles').insert(connections).execute()
            
            if result.data:
                print(f"✅ {len(connections)}개 연결 생성 완료")
                return True
            else:
                print(f"❌ 연결 생성 실패")
                return False
                
        except Exception as e:
            print(f"❌ 연결 생성 실패: {str(e)}")
            return False
    
    def run_clustering_with_split(self) -> bool:
        """대형 클러스터 분할 기능이 포함된 클러스터링"""
        try:
            print("🚀 대형 클러스터 분할 기능이 포함된 클러스터링 시작...")
            
            # 1. 기존 데이터 초기화
            if not self.clear_existing_data():
                return False
            
            # 2. 임베딩 데이터 조회
            embeddings, article_ids, article_metadata = self.fetch_embeddings_data()
            if embeddings is None:
                return False
            
            # 3. 초기 클러스터링 수행
            cluster_labels, clusterer = self.perform_hdbscan_clustering(embeddings)
            if cluster_labels is None:
                return False
            
            # 4. 대형 클러스터 분할
            cluster_labels, article_ids, article_metadata = self.split_all_large_clusters(
                embeddings, cluster_labels, article_ids, article_metadata
            )
            
            # 5. 클러스터 품질 평가
            quality_metrics = self.evaluate_cluster_quality(embeddings, cluster_labels)
            print(f"📊 클러스터 품질:")
            print(f"  - 실루엣 점수: {quality_metrics['silhouette_score']:.3f}")
            print(f"  - 평균 일관성: {quality_metrics['avg_coherence']:.3f}")
            
            # 6. 저품질 클러스터 필터링
            cluster_labels, article_ids, article_metadata = self.filter_low_quality_clusters(
                embeddings, cluster_labels, article_ids, article_metadata
            )
            
            # 7. 클러스터별 이슈 생성
            unique_labels = np.unique(cluster_labels)
            created_issues = 0
            failed_issues = 0
            
            # 클러스터 크기 통계
            cluster_sizes = []
            
            for label in unique_labels:
                if label == -1:  # 노이즈 포인트 건너뛰기
                    continue
                
                cluster_mask = cluster_labels == label
                cluster_article_ids = [article_ids[i] for i in range(len(article_ids)) if cluster_mask[i]]
                cluster_metadata = [article_metadata[i] for i in range(len(article_metadata)) if cluster_mask[i]]
                
                cluster_sizes.append(len(cluster_article_ids))
                
                issue_id = self.create_issue(label, cluster_article_ids, cluster_metadata)
                
                if issue_id:
                    if self.create_issue_articles(issue_id, cluster_article_ids, cluster_metadata):
                        created_issues += 1
                    else:
                        failed_issues += 1
                else:
                    failed_issues += 1
            
            # 최종 통계
            print(f"\n📊 최종 클러스터링 결과:")
            print(f"  - 생성된 이슈: {created_issues}개")
            print(f"  - 실패한 이슈: {failed_issues}개")
            print(f"  - 노이즈 포인트: {list(cluster_labels).count(-1)}개")
            print(f"  - 클러스터링 비율: {((len(cluster_labels) - list(cluster_labels).count(-1)) / len(cluster_labels) * 100):.1f}%")
            
            if cluster_sizes:
                print(f"  - 클러스터 크기 통계:")
                print(f"    * 평균 크기: {np.mean(cluster_sizes):.1f}개")
                print(f"    * 최대 크기: {np.max(cluster_sizes)}개")
                print(f"    * 최소 크기: {np.min(cluster_sizes)}개")
                print(f"    * 50개 이상 클러스터: {sum(1 for size in cluster_sizes if size >= 50)}개")
            
            return created_issues > 0
            
        except Exception as e:
            print(f"❌ 클러스터링 프로세스 실패: {str(e)}")
            return False

def main():
    """메인 함수"""
    print("=" * 60)
    print("🔮 대형 클러스터 분할 기능이 포함된 클러스터링 스크립트")
    print("=" * 60)
    
    try:
        # 클러스터링 실행
        clusterer = ClusterSplitter()
        success = clusterer.run_clustering_with_split()
        
        if success:
            print("\n✅ 대형 클러스터 분할 클러스터링 완료!")
        else:
            print("\n❌ 클러스터링 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()

