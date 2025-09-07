#!/usr/bin/env python3
"""
샘플 데이터로 UMAP + HDBSCAN 클러스터링 테스트
"""

import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
import json

# 클러스터링 라이브러리
import umap
import hdbscan
from sklearn.metrics import silhouette_score

# 시각화 라이브러리
import matplotlib.pyplot as plt
import seaborn as sns

# 프로젝트 모듈
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_manager import get_supabase_client
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

class SampleClusterer:
    """샘플 데이터 클러스터러"""
    
    def __init__(self, sample_size: int = 200):
        """클러스터러 초기화"""
        self.sample_size = sample_size
        self.supabase = get_supabase_client()
        self.embeddings = None
        self.articles_data = None
        self.cluster_labels = None
        self.umap_embedding = None
        
    def load_sample_data(self) -> bool:
        """샘플 데이터 로드"""
        try:
            console.print(f"📊 샘플 데이터 로드 중... (크기: {self.sample_size}개)")
            
            # 임베딩 데이터 조회 (페이지네이션 적용)
            all_embeddings = []
            offset = 0
            batch_size = 1000  # Supabase 기본 제한
            
            while len(all_embeddings) < self.sample_size:
                remaining = self.sample_size - len(all_embeddings)
                current_batch_size = min(batch_size, remaining)
                
                result = self.supabase.client.table('articles_embeddings').select(
                    'cleaned_article_id, embedding_vector, model_name'
                ).eq('embedding_type', 'combined').range(offset, offset + current_batch_size - 1).execute()
                
                if not result.data:
                    break
                    
                all_embeddings.extend(result.data)
                offset += current_batch_size
                
                if len(result.data) < current_batch_size:
                    break
            
            result = type('obj', (object,), {'data': all_embeddings})
            
            if not result.data:
                console.print("❌ 임베딩 데이터가 없습니다.")
                return False
            
            # 기사 메타데이터 조회 (배치 처리)
            article_ids = [item['cleaned_article_id'] for item in result.data]
            
            batch_size = 50
            articles_data_list = []
            
            for i in range(0, len(article_ids), batch_size):
                batch_ids = article_ids[i:i+batch_size]
                articles_result = self.supabase.client.table('articles_cleaned').select(
                    'id, title_cleaned, lead_paragraph, merged_content, original_article_id'
                ).in_('id', batch_ids).execute()
                
                if articles_result.data:
                    articles_data_list.extend(articles_result.data)
            
            # 데이터 정리
            embeddings_df = pd.DataFrame(result.data)
            articles_df = pd.DataFrame(articles_data_list)
            
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
            
            console.print(f"✅ 샘플 데이터 로드 완료:")
            console.print(f"   - 임베딩: {len(self.embeddings)}개")
            console.print(f"   - 기사 메타데이터: {len(self.articles_data)}개")
            console.print(f"   - 임베딩 차원: {self.embeddings.shape[1]}")
            
            return True
            
        except Exception as e:
            console.print(f"❌ 데이터 로드 실패: {e}")
            return False
    
    def run_umap(self, n_components: int = 2) -> bool:
        """UMAP 차원 축소"""
        try:
            console.print("🔄 UMAP 차원 축소 실행 중...")
            
            # UMAP 파라미터 조정 (샘플 데이터에 맞게)
            n_neighbors = min(15, len(self.embeddings) - 1)
            
            reducer = umap.UMAP(
                n_components=n_components,
                n_neighbors=n_neighbors,
                min_dist=0.1,
                random_state=42,
                verbose=True
            )
            
            self.umap_embedding = reducer.fit_transform(self.embeddings)
            
            console.print(f"✅ UMAP 완료: {self.embeddings.shape[1]}D → {n_components}D")
            console.print(f"   - 파라미터: n_neighbors={n_neighbors}, min_dist=0.1")
            
            return True
            
        except Exception as e:
            console.print(f"❌ UMAP 실행 실패: {e}")
            return False
    
    def run_hdbscan(self) -> bool:
        """HDBSCAN 클러스터링"""
        try:
            console.print("🔄 HDBSCAN 클러스터링 실행 중...")
            
            # HDBSCAN 파라미터 조정 (샘플 데이터에 맞게)
            min_cluster_size = max(3, len(self.embeddings) // 20)  # 전체의 5%
            min_samples = max(2, min_cluster_size // 2)
            
            clusterer = hdbscan.HDBSCAN(
                min_cluster_size=min_cluster_size,
                min_samples=min_samples,
                metric='euclidean'
            )
            
            self.cluster_labels = clusterer.fit_predict(self.umap_embedding)
            
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
    
    def evaluate_clusters(self) -> Dict[str, Any]:
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
            
            console.print(f"✅ 평가 완료:")
            console.print(f"   - 실루엣 점수: {silhouette_avg:.3f}")
            console.print(f"   - 클러스터 수: {len(cluster_stats)}개")
            console.print(f"   - 노이즈 비율: {np.sum(self.cluster_labels == -1)/len(self.cluster_labels)*100:.1f}%")
            
            return {
                'silhouette_score': silhouette_avg,
                'n_clusters': len(cluster_stats),
                'n_noise': np.sum(self.cluster_labels == -1),
                'cluster_details': cluster_stats
            }
            
        except Exception as e:
            console.print(f"❌ 평가 실패: {e}")
            return {}
    
    def visualize_clusters(self) -> bool:
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
            plt.title(f'Political News Clusters (Sample: {len(self.embeddings)} articles)')
            plt.xlabel('UMAP Dimension 1')
            plt.ylabel('UMAP Dimension 2')
            
            # 클러스터별 기사 수 표시
            for label in np.unique(self.cluster_labels):
                if label == -1:
                    continue
                cluster_mask = self.cluster_labels == label
                cluster_center = self.umap_embedding[cluster_mask].mean(axis=0)
                plt.annotate(f'C{label}\n({np.sum(cluster_mask)})', 
                           cluster_center, 
                           ha='center', 
                           va='center',
                           bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
            
            plt.tight_layout()
            plt.savefig('sample_clustering_results.png', dpi=300, bbox_inches='tight')
            console.print("✅ 시각화 저장: sample_clustering_results.png")
            
            # 항상 이미지 저장하고 GUI 환경에서만 show 실행
            import os
            if os.environ.get('DISPLAY') or os.environ.get('WAYLAND_DISPLAY'):
                plt.show()
            else:
                plt.close()  # 메모리 정리
                console.print("💾 시각화 이미지가 저장되었습니다. GUI 환경이 아니므로 화면에 표시되지 않습니다.")
            
            return True
            
        except Exception as e:
            console.print(f"❌ 시각화 실패: {e}")
            return False
    
    def analyze_clusters(self) -> List[Dict[str, Any]]:
        """클러스터 분석"""
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
                    'articles': cluster_articles[['id', 'title_cleaned', 'lead_paragraph']].to_dict('records'),
                    'representative_article': cluster_articles.iloc[0].to_dict() if len(cluster_articles) > 0 else {}
                }
                
                clusters_info.append(cluster_info)
            
            # 결과 테이블 출력
            table = Table(title="클러스터 분석 결과")
            table.add_column("클러스터 ID", style="cyan")
            table.add_column("기사 수", style="magenta")
            table.add_column("대표 기사 제목", style="green")
            
            for cluster_info in clusters_info:
                title = cluster_info['representative_article'].get('title_cleaned', 'N/A')
                if len(title) > 50:
                    title = title[:50] + "..."
                
                table.add_row(
                    str(cluster_info['cluster_id']),
                    str(cluster_info['size']),
                    title
                )
            
            console.print(table)
            
            return clusters_info
            
        except Exception as e:
            console.print(f"❌ 분석 실패: {e}")
            return []
    
    def run_sample_clustering(self) -> bool:
        """샘플 클러스터링 전체 파이프라인"""
        try:
            console.print(Panel.fit(
                f"[bold blue]🚀 샘플 클러스터링 시작 (크기: {self.sample_size}개)[/bold blue]",
                title="샘플 클러스터링"
            ))
            
            # 1. 데이터 로드
            if not self.load_sample_data():
                return False
            
            # 2. UMAP 차원 축소
            if not self.run_umap():
                return False
            
            # 3. HDBSCAN 클러스터링
            if not self.run_hdbscan():
                return False
            
            # 4. 품질 평가
            self.evaluate_clusters()
            
            # 5. 시각화
            self.visualize_clusters()
            
            # 6. 클러스터 분석
            self.analyze_clusters()
            
            console.print(Panel.fit(
                "[bold green]✅ 샘플 클러스터링 완료![/bold green]",
                title="완료"
            ))
            
            return True
            
        except Exception as e:
            console.print(f"❌ 샘플 클러스터링 실패: {e}")
            return False

def main():
    """메인 함수"""
    # 샘플 크기 선택
    console.print("샘플 크기를 선택하세요:")
    console.print("1. 100개 기사")
    console.print("2. 200개 기사")
    console.print("3. 500개 기사")
    
    choice = input("선택 (1-3): ").strip()
    
    sample_sizes = {'1': 100, '2': 200, '3': 500}
    sample_size = sample_sizes.get(choice, 200)
    
    clusterer = SampleClusterer(sample_size=sample_size)
    clusterer.run_sample_clustering()

if __name__ == "__main__":
    main()
