#!/usr/bin/env python3
"""
클러스터 프로세서 클래스 - KISS 원칙 적용
UMAP + HDBSCAN 클러스터링만 담당하는 단일 책임
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import umap
import hdbscan
from rich.console import Console

from utils.supabase_manager import get_supabase_client
from clustering.config import get_config

console = Console()

class ClusterProcessor:
    """클러스터 프로세서 클래스 - 단일 책임: 클러스터링"""
    
    def __init__(self, embeddings, embeddings_data, articles_data, media_outlets):
        """초기화"""
        self.config = get_config()
        self.supabase = get_supabase_client()
        self.embeddings = embeddings
        self.embeddings_data = embeddings_data
        self.articles_data = articles_data
        self.media_outlets = media_outlets
        
        self.umap_embedding = None
        self.cluster_labels = None
        self.clusters_info = None
    
    def run_umap_reduction(self) -> bool:
        """UMAP 차원 축소"""
        try:
            console.print("🔄 UMAP 차원 축소 실행 중...")
            
            n_samples = len(self.embeddings)
            
            # config 기반 파라미터 설정
            if n_samples < 100:
                n_neighbors = min(5, n_samples - 1)
                min_dist = 0.1
            elif n_samples < 500:
                n_neighbors = min(10, n_samples // 10)
                min_dist = 0.2
            else:
                n_neighbors = self.config["umap_n_neighbors"]
                min_dist = self.config["umap_min_dist"]
            
            reducer = umap.UMAP(
                n_components=self.config["umap_n_components"],
                n_neighbors=n_neighbors,
                min_dist=min_dist,
                random_state=42,
                n_jobs=-1
            )
            
            self.umap_embedding = reducer.fit_transform(self.embeddings)
            
            console.print(f"✅ UMAP 완료: {self.embeddings.shape[1]}D → 2D")
            return True
            
        except Exception as e:
            console.print(f"❌ UMAP 실패: {e}")
            return False
    
    def run_hdbscan_clustering(self) -> bool:
        """HDBSCAN 클러스터링"""
        try:
            console.print("🔄 HDBSCAN 클러스터링 실행 중...")
            
            n_samples = len(self.embeddings)
            
            # config 기반 파라미터 설정 (config 값 우선 사용)
            min_cluster_size = self.config["hdbscan_min_cluster_size"]
            min_samples = self.config["hdbscan_min_samples"]
            
            clusterer = hdbscan.HDBSCAN(
                min_cluster_size=min_cluster_size,
                min_samples=min_samples,
                metric='euclidean'
            )
            
            self.cluster_labels = clusterer.fit_predict(self.umap_embedding)
            
            console.print(f"✅ HDBSCAN 완료: {len(set(self.cluster_labels))}개 클러스터")
            return True
            
        except Exception as e:
            console.print(f"❌ HDBSCAN 실패: {e}")
            return False
    
    def analyze_clusters(self) -> bool:
        """클러스터 분석"""
        try:
            console.print("📊 클러스터 분석 중...")
            
            # 클러스터별 정보 수집
            clusters_info = []
            
            for cluster_id in set(self.cluster_labels):
                if cluster_id == -1:  # 노이즈 제외
                    continue
                
                # 클러스터에 속한 임베딩 인덱스
                cluster_indices = np.where(self.cluster_labels == cluster_id)[0]
                
                # 임베딩 ID들
                embedding_ids = self.embeddings_data.iloc[cluster_indices]['cleaned_article_id'].tolist()
                
                # 기사 정보
                cluster_articles = self.articles_data[self.articles_data['id'].isin(embedding_ids)]
                
                # 언론사별 기사 수
                media_counts = cluster_articles['media_id'].value_counts().to_dict()
                
                clusters_info.append({
                    'cluster_id': cluster_id,
                    'size': len(cluster_indices),
                    'embedding_ids': embedding_ids,
                    'media_counts': media_counts,
                    'articles': cluster_articles.to_dict('records')
                })
            
            self.clusters_info = clusters_info
            console.print(f"✅ 클러스터 분석 완료: {len(clusters_info)}개 클러스터")
            return True
            
        except Exception as e:
            console.print(f"❌ 클러스터 분석 실패: {e}")
            return False
    
    def process_clustering(self) -> bool:
        """전체 클러스터링 프로세스"""
        console.print("🚀 클러스터링 프로세스 시작...")
        
        if not self.run_umap_reduction():
            return False
        if not self.run_hdbscan_clustering():
            return False
        if not self.analyze_clusters():
            return False
        
        console.print("✅ 클러스터링 프로세스 완료!")
        return True