#!/usr/bin/env python3
"""
HDBSCAN 클러스터링 스크립트
- articles_embeddings 테이블의 벡터를 기반으로 클러스터링
- issues 테이블에 이슈 저장
- issue_articles 테이블에 연결 저장
"""

import sys
import os
import json
import numpy as np
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from collections import Counter

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

class HDBSCANClusterer:
    """HDBSCAN 클러스터링 클래스"""
    
    def __init__(self):
        """초기화"""
        # 설정 (하드코딩) - 더 큰 클러스터를 위해 완화
        self.MIN_CLUSTER_SIZE = 8        # 최소 클러스터 크기 (3 → 8)
        self.MIN_SAMPLES = 4             # 최소 샘플 수 (2 → 4)
        self.CLUSTER_SELECTION_EPSILON = 0.3  # 클러스터 선택 임계값 (0.1 → 0.3)
        self.METRIC = 'euclidean'        # 유클리드 거리 (정규화된 벡터에서 코사인과 유사)
        
        # Supabase 연결
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
    
    def fetch_embeddings_data(self) -> tuple:
        """
        임베딩 데이터 조회
        
        Returns:
            tuple: (embeddings, article_ids, article_metadata)
        """
        try:
            print("📡 임베딩 데이터 조회 중...")
            
            # articles_embeddings와 articles_cleaned 조인하여 데이터 조회
            result = self.supabase_manager.client.table('articles_embeddings').select(
                'id, cleaned_article_id, article_id, media_id, embedding_vector, model_name'
            ).execute()
            
            if not result.data:
                print("❌ 임베딩 데이터가 없습니다.")
                return None, None, None
            
            print(f"✅ {len(result.data)}개 임베딩 데이터 조회 완료")
            
            # 벡터 파싱 및 정리
            embeddings = []
            article_ids = []
            article_metadata = []
            
            for item in result.data:
                try:
                    # JSON 문자열을 파싱
                    vector = json.loads(item['embedding_vector'])
                    
                    if len(vector) == 1536:  # text-embedding-3-small 차원 확인
                        embeddings.append(vector)
                        article_ids.append(item['article_id'])
                        article_metadata.append({
                            'id': item['id'],
                            'cleaned_article_id': item['cleaned_article_id'],
                            'article_id': item['article_id'],
                            'media_id': item['media_id']
                        })
                    else:
                        print(f"⚠️ 잘못된 벡터 차원: {len(vector)}차원 (기사 ID: {item['article_id']})")
                        
                except Exception as e:
                    print(f"⚠️ 벡터 파싱 실패 (기사 ID: {item['article_id']}): {str(e)}")
                    continue
            
            print(f"✅ {len(embeddings)}개 벡터 파싱 완료")
            
            # 벡터 정규화 (코사인 유사도 효과)
            embeddings_array = np.array(embeddings)
            norms = np.linalg.norm(embeddings_array, axis=1, keepdims=True)
            normalized_embeddings = embeddings_array / norms
            
            return normalized_embeddings, article_ids, article_metadata
            
        except Exception as e:
            print(f"❌ 임베딩 데이터 조회 실패: {str(e)}")
            return None, None, None
    
    def perform_clustering(self, embeddings: np.ndarray) -> tuple:
        """
        HDBSCAN 클러스터링 수행
        
        Args:
            embeddings: 임베딩 벡터 배열
            
        Returns:
            tuple: (cluster_labels, clusterer)
        """
        try:
            print("🔄 HDBSCAN 클러스터링 수행 중...")
            
            # HDBSCAN 클러스터러 초기화
            clusterer = hdbscan.HDBSCAN(
                min_cluster_size=self.MIN_CLUSTER_SIZE,
                min_samples=self.MIN_SAMPLES,
                cluster_selection_epsilon=self.CLUSTER_SELECTION_EPSILON,
                metric=self.METRIC
            )
            
            # 클러스터링 수행
            cluster_labels = clusterer.fit_predict(embeddings)
            
            # 결과 분석
            unique_labels = np.unique(cluster_labels)
            n_clusters = len(unique_labels) - (1 if -1 in unique_labels else 0)
            n_noise = list(cluster_labels).count(-1)
            
            print(f"✅ 클러스터링 완료:")
            print(f"  - 클러스터 수: {n_clusters}개")
            print(f"  - 노이즈 포인트: {n_noise}개")
            print(f"  - 클러스터링 비율: {((len(cluster_labels) - n_noise) / len(cluster_labels) * 100):.1f}%")
            
            return cluster_labels, clusterer
            
        except Exception as e:
            print(f"❌ 클러스터링 실패: {str(e)}")
            return None, None
    
    def clear_existing_data(self) -> bool:
        """
        기존 이슈 데이터 초기화
        
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            print("🗑️ 기존 이슈 데이터 초기화 중...")
            
            # issue_articles 테이블 초기화
            self.supabase_manager.client.table('issue_articles').delete().neq('issue_id', '00000000-0000-0000-0000-000000000000').execute()
            
            # issues 테이블 초기화
            self.supabase_manager.client.table('issues').delete().neq('id', '00000000-0000-0000-0000-000000000000').execute()
            
            print("✅ 기존 이슈 데이터 초기화 완료")
            return True
            
        except Exception as e:
            print(f"❌ 데이터 초기화 실패: {str(e)}")
            return False
    
    def analyze_media_bias(self, article_metadata: List[Dict]) -> Dict[str, str]:
        """
        기사 메타데이터에서 성향별 언론사 정보 분석
        
        Args:
            article_metadata: 기사 메타데이터 리스트
            
        Returns:
            Dict: 성향별 언론사 정보
        """
        try:
            # 언론사 ID별 성향 매핑 (하드코딩)
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
            
            # 언론사별 기사 수 집계
            media_counts = {}
            for article in article_metadata:
                media_id = article['media_id']
                if media_id in media_bias_map:
                    bias = media_bias_map[media_id]
                    if bias not in media_counts:
                        media_counts[bias] = 0
                    media_counts[bias] += 1
            
            # 성향별 언론사 정보 생성
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
        """
        이슈 생성 및 저장
        
        Args:
            cluster_id: 클러스터 ID
            article_ids: 해당 클러스터의 기사 ID들
            article_metadata: 기사 메타데이터
            
        Returns:
            str: 생성된 이슈 ID 또는 None
        """
        try:
            # 성향별 언론사 정보 분석
            source_info = self.analyze_media_bias(article_metadata)
            
            # 기본 이슈 데이터 생성
            issue_data = {
                'date': date.today().isoformat(),
                'title': f'이슈 {cluster_id + 1}',
                'summary': f'{len(article_ids)}개 기사로 구성된 이슈',
                'subtitle': f'클러스터 {cluster_id + 1}',
                'importance': 'medium',
                'source': source_info['total_source'],
                'left_source': source_info['left_source'],
                'center_source': source_info['center_source'],
                'right_source': source_info['right_source'],
                'created_at': datetime.now().isoformat()
            }
            
            # 이슈 저장
            result = self.supabase_manager.client.table('issues').insert(issue_data).execute()
            
            if result.data:
                issue_id = result.data[0]['id']
                print(f"✅ 이슈 {cluster_id + 1} 생성 완료 (ID: {issue_id})")
                return issue_id
            else:
                print(f"❌ 이슈 {cluster_id + 1} 생성 실패")
                return None
                
        except Exception as e:
            print(f"❌ 이슈 생성 실패: {str(e)}")
            return None
    
    def create_issue_articles(self, issue_id: str, article_ids: List[str], article_metadata: List[Dict]) -> bool:
        """
        이슈-기사 연결 생성
        
        Args:
            issue_id: 이슈 ID
            article_ids: 기사 ID들
            article_metadata: 기사 메타데이터
            
        Returns:
            bool: 생성 성공 여부
        """
        try:
            # 연결 데이터 생성
            connections = []
            for i, article_id in enumerate(article_ids):
                # 해당 article_id의 cleaned_article_id 찾기
                cleaned_article_id = None
                for metadata in article_metadata:
                    if metadata['article_id'] == article_id:
                        cleaned_article_id = metadata['cleaned_article_id']
                        break
                
                connections.append({
                    'issue_id': issue_id,
                    'article_id': article_id,
                    'cleaned_article_id': cleaned_article_id,
                    'stance': 'center'  # 기본값
                })
            
            # 일괄 저장
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
    
    def run_clustering(self) -> bool:
        """
        클러스터링 메인 프로세스
        
        Returns:
            bool: 처리 성공 여부
        """
        try:
            print("🚀 HDBSCAN 클러스터링 시작...")
            
            # 1. 기존 데이터 초기화
            if not self.clear_existing_data():
                return False
            
            # 2. 임베딩 데이터 조회
            embeddings, article_ids, article_metadata = self.fetch_embeddings_data()
            if embeddings is None:
                return False
            
            # 3. 클러스터링 수행
            cluster_labels, clusterer = self.perform_clustering(embeddings)
            if cluster_labels is None:
                return False
            
            # 4. 클러스터별 이슈 생성
            unique_labels = np.unique(cluster_labels)
            created_issues = 0
            failed_issues = 0
            
            for label in unique_labels:
                if label == -1:  # 노이즈 포인트 건너뛰기
                    continue
                
                # 해당 클러스터의 기사들 찾기
                cluster_mask = cluster_labels == label
                cluster_article_ids = [article_ids[i] for i in range(len(article_ids)) if cluster_mask[i]]
                cluster_metadata = [article_metadata[i] for i in range(len(article_metadata)) if cluster_mask[i]]
                
                # 이슈 생성
                issue_id = self.create_issue(label, cluster_article_ids, cluster_metadata)
                
                if issue_id:
                    # 이슈-기사 연결 생성
                    if self.create_issue_articles(issue_id, cluster_article_ids, cluster_metadata):
                        created_issues += 1
                    else:
                        failed_issues += 1
                else:
                    failed_issues += 1
            
            print(f"\n📊 클러스터링 결과:")
            print(f"  - 생성된 이슈: {created_issues}개")
            print(f"  - 실패한 이슈: {failed_issues}개")
            print(f"  - 노이즈 포인트: {list(cluster_labels).count(-1)}개")
            
            return created_issues > 0
            
        except Exception as e:
            print(f"❌ 클러스터링 프로세스 실패: {str(e)}")
            return False

def main():
    """메인 함수"""
    print("=" * 60)
    print("🔮 HDBSCAN 클러스터링 스크립트")
    print("=" * 60)
    
    try:
        # 클러스터링 실행
        clusterer = HDBSCANClusterer()
        success = clusterer.run_clustering()
        
        if success:
            print("\n✅ 클러스터링 완료!")
        else:
            print("\n❌ 클러스터링 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()
