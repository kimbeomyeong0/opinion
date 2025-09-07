#!/usr/bin/env python3
"""
UMAP + HDBSCAN을 이용한 정치 뉴스 클러스터링
"""

import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Tuple
import json

# 클러스터링 라이브러리
import umap
import hdbscan
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

# 시각화 라이브러리
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go

# 프로젝트 모듈
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_manager import get_supabase_client
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.table import Table
from rich.panel import Panel

console = Console()

class PoliticalNewsClusterer:
    """정치 뉴스 클러스터링 클래스"""
    
    def __init__(self):
        """클러스터러 초기화"""
        self.supabase = get_supabase_client()
        self.embeddings = None
        self.articles_data = None
        self.umap_reducer = None
        self.hdbscan_clusterer = None
        self.cluster_labels = None
        self.umap_embedding = None
        
        # 결과 저장
        self.clusters_info = []
        self.cluster_stats = {}
        
    def load_embeddings_data(self) -> bool:
        """임베딩 데이터 로드"""
        try:
            console.print("📊 임베딩 데이터 로드 중...")
            
            # 임베딩 데이터 조회
            result = self.supabase.client.table('articles_embeddings').select(
                'cleaned_article_id, embedding_vector, model_name'
            ).eq('embedding_type', 'combined').execute()
            
            if not result.data:
                console.print("❌ 임베딩 데이터가 없습니다.")
                return False
            
            # 기사 메타데이터 조회 (배치 처리)
            article_ids = [item['cleaned_article_id'] for item in result.data]
            
            # 배치 크기로 나누어서 조회
            batch_size = 100
            articles_data_list = []
            
            for i in range(0, len(article_ids), batch_size):
                batch_ids = article_ids[i:i+batch_size]
                articles_result = self.supabase.client.table('articles_cleaned').select(
                    'id, title_cleaned, lead_paragraph, merged_content, original_article_id'
                ).in_('id', batch_ids).execute()
                
                if articles_result.data:
                    articles_data_list.extend(articles_result.data)
            
            articles_result = type('obj', (object,), {'data': articles_data_list})
            
            # 데이터 정리
            embeddings_df = pd.DataFrame(result.data)
            articles_df = pd.DataFrame(articles_result.data)
            
            # 임베딩 벡터를 numpy 배열로 변환
            embeddings_list = []
            for embedding_str in embeddings_df['embedding_vector']:
                if isinstance(embedding_str, str):
                    embedding_vector = json.loads(embedding_str)
                else:
                    embedding_vector = embedding_str
                embeddings_list.append(embedding_vector)
            
            self.embeddings = np.array(embeddings_list)
            
            # 기사 데이터와 매핑
            self.articles_data = articles_df.merge(
                embeddings_df[['cleaned_article_id']], 
                left_on='id', 
                right_on='cleaned_article_id'
            )
            
            console.print(f"✅ 데이터 로드 완료: {len(self.embeddings)}개 기사")
            console.print(f"   - 임베딩 차원: {self.embeddings.shape[1]}")
            console.print(f"   - 기사 메타데이터: {len(self.articles_data)}개")
            
            return True
            
        except Exception as e:
            console.print(f"❌ 데이터 로드 실패: {e}")
            return False
    
    def run_umap_reduction(self, n_components: int = 2, n_neighbors: int = 15, min_dist: float = 0.1) -> bool:
        """UMAP 차원 축소 실행"""
        try:
            console.print("🔄 UMAP 차원 축소 실행 중...")
            
            # UMAP 리듀서 생성
            self.umap_reducer = umap.UMAP(
                n_components=n_components,
                n_neighbors=n_neighbors,
                min_dist=min_dist,
                random_state=42,
                verbose=True
            )
            
            # 차원 축소 실행
            self.umap_embedding = self.umap_reducer.fit_transform(self.embeddings)
            
            console.print(f"✅ UMAP 완료: {self.embeddings.shape[1]}D → {n_components}D")
            console.print(f"   - 파라미터: n_neighbors={n_neighbors}, min_dist={min_dist}")
            
            return True
            
        except Exception as e:
            console.print(f"❌ UMAP 실행 실패: {e}")
            return False
    
    def run_hdbscan_clustering(self, min_cluster_size: int = 5, min_samples: int = 3) -> bool:
        """HDBSCAN 클러스터링 실행"""
        try:
            console.print("🔄 HDBSCAN 클러스터링 실행 중...")
            
            # HDBSCAN 클러스터러 생성
            self.hdbscan_clusterer = hdbscan.HDBSCAN(
                min_cluster_size=min_cluster_size,
                min_samples=min_samples,
                metric='euclidean'
            )
            
            # 클러스터링 실행
            self.cluster_labels = self.hdbscan_clusterer.fit_predict(self.umap_embedding)
            
            # 클러스터 통계
            unique_labels = np.unique(self.cluster_labels)
            n_clusters = len(unique_labels) - (1 if -1 in unique_labels else 0)
            n_noise = list(self.cluster_labels).count(-1)
            
            console.print(f"✅ HDBSCAN 완료:")
            console.print(f"   - 클러스터 수: {n_clusters}개")
            console.print(f"   - 노이즈 기사: {n_noise}개")
            console.print(f"   - 파라미터: min_cluster_size={min_cluster_size}, min_samples={min_samples}")
            
            return True
            
        except Exception as e:
            console.print(f"❌ HDBSCAN 실행 실패: {e}")
            return False
    
    def evaluate_clusters(self) -> Dict[str, float]:
        """클러스터 품질 평가"""
        try:
            console.print("📊 클러스터 품질 평가 중...")
            
            # 노이즈가 아닌 데이터만 사용
            valid_mask = self.cluster_labels != -1
            if np.sum(valid_mask) < 2:
                console.print("❌ 유효한 클러스터가 부족합니다.")
                return {}
            
            # 실루엣 점수 계산
            silhouette_avg = silhouette_score(
                self.umap_embedding[valid_mask], 
                self.cluster_labels[valid_mask]
            )
            
            # 클러스터별 통계
            cluster_stats = {}
            for label in np.unique(self.cluster_labels):
                if label == -1:  # 노이즈 제외
                    continue
                cluster_mask = self.cluster_labels == label
                cluster_size = np.sum(cluster_mask)
                cluster_stats[f'cluster_{label}'] = {
                    'size': cluster_size,
                    'percentage': cluster_size / len(self.cluster_labels) * 100
                }
            
            self.cluster_stats = {
                'silhouette_score': silhouette_avg,
                'n_clusters': len(np.unique(self.cluster_labels)) - 1,
                'n_noise': np.sum(self.cluster_labels == -1),
                'cluster_details': cluster_stats
            }
            
            console.print(f"✅ 평가 완료:")
            console.print(f"   - 실루엣 점수: {silhouette_avg:.3f}")
            console.print(f"   - 클러스터 수: {self.cluster_stats['n_clusters']}개")
            console.print(f"   - 노이즈 비율: {self.cluster_stats['n_noise']/len(self.cluster_labels)*100:.1f}%")
            
            return self.cluster_stats
            
        except Exception as e:
            console.print(f"❌ 평가 실패: {e}")
            return {}
    
    def visualize_clusters(self, save_path: str = None) -> bool:
        """클러스터 시각화"""
        try:
            console.print("🎨 클러스터 시각화 생성 중...")
            
            # 데이터 준비
            df_viz = pd.DataFrame({
                'x': self.umap_embedding[:, 0],
                'y': self.umap_embedding[:, 1],
                'cluster': self.cluster_labels,
                'title': self.articles_data['title_cleaned'].values
            })
            
            # matplotlib 시각화
            plt.figure(figsize=(12, 8))
            scatter = plt.scatter(df_viz['x'], df_viz['y'], 
                               c=df_viz['cluster'], 
                               cmap='tab20', 
                               alpha=0.7,
                               s=50)
            plt.colorbar(scatter)
            plt.title('Political News Clusters (UMAP + HDBSCAN)')
            plt.xlabel('UMAP Dimension 1')
            plt.ylabel('UMAP Dimension 2')
            
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                console.print(f"✅ 시각화 저장: {save_path}")
            
            plt.show()
            
            return True
            
        except Exception as e:
            console.print(f"❌ 시각화 실패: {e}")
            return False
    
    def analyze_clusters(self) -> List[Dict[str, Any]]:
        """클러스터 분석 및 정보 추출"""
        try:
            console.print("🔍 클러스터 분석 중...")
            
            clusters_info = []
            
            for label in np.unique(self.cluster_labels):
                if label == -1:  # 노이즈 제외
                    continue
                
                cluster_mask = self.cluster_labels == label
                cluster_articles = self.articles_data[cluster_mask]
                
                # 클러스터 정보 수집
                cluster_info = {
                    'cluster_id': int(label),
                    'size': int(np.sum(cluster_mask)),
                    'articles': cluster_articles.to_dict('records'),
                    'representative_article': self._find_representative_article(cluster_articles),
                    'keywords': self._extract_keywords(cluster_articles)
                }
                
                clusters_info.append(cluster_info)
            
            self.clusters_info = clusters_info
            
            console.print(f"✅ 분석 완료: {len(clusters_info)}개 클러스터")
            
            return clusters_info
            
        except Exception as e:
            console.print(f"❌ 분석 실패: {e}")
            return []
    
    def _find_representative_article(self, cluster_articles: pd.DataFrame) -> Dict[str, Any]:
        """대표 기사 찾기 (클러스터 중심과 가장 가까운 기사)"""
        # 간단히 첫 번째 기사를 대표로 선택 (나중에 개선 가능)
        if len(cluster_articles) > 0:
            return cluster_articles.iloc[0].to_dict()
        return {}
    
    def _extract_keywords(self, cluster_articles: pd.DataFrame) -> List[str]:
        """클러스터 키워드 추출 (간단한 버전)"""
        # 제목에서 공통 단어 추출 (나중에 TF-IDF로 개선)
        titles = cluster_articles['title_cleaned'].dropna().tolist()
        if titles:
            # 간단한 키워드 추출 (실제로는 더 정교한 방법 필요)
            all_words = ' '.join(titles).split()
            word_freq = pd.Series(all_words).value_counts()
            return word_freq.head(5).index.tolist()
        return []
    
    def save_to_database(self) -> bool:
        """클러스터 결과를 데이터베이스에 저장"""
        try:
            console.print("💾 데이터베이스 저장 중...")
            
            # issues 테이블에 클러스터 저장
            for cluster_info in self.clusters_info:
                issue_data = {
                    'title': f"클러스터 {cluster_info['cluster_id']}",
                    'subtitle': f"{cluster_info['size']}개 기사",
                    'summary': self._generate_cluster_summary(cluster_info),
                    'left_view': "보수 관점 분석 필요",
                    'center_view': "중립 관점 분석 필요", 
                    'right_view': "진보 관점 분석 필요",
                    'source': "AI 클러스터링",
                    'date': datetime.now().date()
                }
                
                # issue 저장
                issue_result = self.supabase.client.table('issues').insert(issue_data).execute()
                if not issue_result.data:
                    console.print(f"❌ 이슈 저장 실패: 클러스터 {cluster_info['cluster_id']}")
                    continue
                
                issue_id = issue_result.data[0]['id']
                
                # issue_articles 테이블에 매핑 저장
                for article in cluster_info['articles']:
                    article_mapping = {
                        'issue_id': issue_id,
                        'article_id': article['id'],
                        'stance': 'center'  # 기본값, 나중에 개선
                    }
                    
                    self.supabase.client.table('issue_articles').insert(article_mapping).execute()
            
            console.print("✅ 데이터베이스 저장 완료")
            return True
            
        except Exception as e:
            console.print(f"❌ 데이터베이스 저장 실패: {e}")
            return False
    
    def _generate_cluster_summary(self, cluster_info: Dict[str, Any]) -> str:
        """클러스터 요약 생성"""
        keywords = ', '.join(cluster_info['keywords'][:3])
        return f"이 클러스터는 {cluster_info['size']}개의 기사로 구성되어 있으며, 주요 키워드는 {keywords}입니다."
    
    def run_full_pipeline(self) -> bool:
        """전체 파이프라인 실행"""
        try:
            console.print(Panel.fit(
                "[bold blue]🚀 UMAP + HDBSCAN 클러스터링 파이프라인 시작[/bold blue]",
                title="클러스터링 실행"
            ))
            
            # 1. 데이터 로드
            if not self.load_embeddings_data():
                return False
            
            # 2. UMAP 차원 축소
            if not self.run_umap_reduction():
                return False
            
            # 3. HDBSCAN 클러스터링
            if not self.run_hdbscan_clustering():
                return False
            
            # 4. 품질 평가
            self.evaluate_clusters()
            
            # 5. 시각화
            self.visualize_clusters('clustering_results.png')
            
            # 6. 클러스터 분석
            self.analyze_clusters()
            
            # 7. 데이터베이스 저장
            self.save_to_database()
            
            console.print(Panel.fit(
                "[bold green]✅ 클러스터링 파이프라인 완료![/bold green]",
                title="완료"
            ))
            
            return True
            
        except Exception as e:
            console.print(f"❌ 파이프라인 실행 실패: {e}")
            return False

def main():
    """메인 함수"""
    clusterer = PoliticalNewsClusterer()
    clusterer.run_full_pipeline()

if __name__ == "__main__":
    main()
