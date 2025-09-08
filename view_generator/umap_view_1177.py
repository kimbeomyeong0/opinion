#!/usr/bin/env python3
"""
기존 umap_view.html 디자인을 유지하면서 전체 1,177개 기사 데이터로 교체
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.supabase_manager import get_supabase_client
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
import pandas as pd
from rich.console import Console
import umap
import hdbscan

console = Console()

def generate_umap_view_1177():
    """기존 디자인을 유지하면서 전체 1,177개 기사로 UMAP 시각화 생성"""
    console.print("🎨 기존 디자인으로 UMAP 시각화 생성 시작...")
    
    # Supabase 클라이언트 초기화
    supabase = get_supabase_client()
    if not supabase.client:
        console.print("❌ Supabase 클라이언트 초기화 실패")
        return
    
    try:
        # 클러스터 데이터 로드
        console.print("📊 클러스터 데이터 로드 중...")
        clusters_result = supabase.client.table('issues').select('*').execute()
        clusters = clusters_result.data
        
        if not clusters:
            console.print("❌ 클러스터 데이터가 없습니다")
            return
        
        # 임베딩 데이터 로드 (전체 1,177개) - articles_embeddings 테이블에서 직접
        console.print("📊 임베딩 데이터 로드 중...")
        
        # 페이지네이션으로 articles_embeddings 테이블에서 임베딩 가져오기
        embeddings_data = []
        offset = 0
        limit = 1000
        
        while True:
            result = supabase.client.table('articles_embeddings').select('*').range(offset, offset + limit - 1).execute()
            if not result.data:
                break
            embeddings_data.extend(result.data)
            offset += limit
            if len(result.data) < limit:
                break
        
        if not embeddings_data:
            console.print("❌ 임베딩 데이터가 없습니다")
            return
        
        console.print(f"✅ 데이터 로드 완료: {len(clusters)}개 클러스터, {len(embeddings_data)}개 임베딩")
        
        # 임베딩 벡터 추출 (문자열을 파싱해서 숫자 배열로 변환)
        embeddings = []
        article_ids = []
        for emb in embeddings_data:
            if emb.get('embedding_vector'):
                try:
                    # 문자열을 파싱해서 리스트로 변환
                    embedding_str = emb['embedding_vector']
                    if isinstance(embedding_str, str):
                        # 문자열에서 대괄호 제거하고 쉼표로 분리
                        embedding_str = embedding_str.strip('[]')
                        embedding_list = [float(x.strip()) for x in embedding_str.split(',')]
                        embeddings.append(embedding_list)
                        article_ids.append(emb['cleaned_article_id'])
                except Exception as e:
                    console.print(f"⚠️ 임베딩 파싱 실패: {e}")
                    continue
        
        embeddings = np.array(embeddings)
        console.print(f"✅ 임베딩 벡터 추출 완료: {len(embeddings)}개")
        
        # UMAP 차원 축소
        console.print("🔄 UMAP 차원 축소 실행 중...")
        reducer = umap.UMAP(
            n_components=2,
            n_neighbors=30,
            min_dist=0.1,
            random_state=42,
            verbose=True,
            n_jobs=-1
        )
        
        umap_coords = reducer.fit_transform(embeddings)
        console.print(f"✅ UMAP 완료: {embeddings.shape[1]}D → 2D")
        
        # HDBSCAN 클러스터링
        console.print("🔄 HDBSCAN 클러스터링 실행 중...")
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=5,
            min_samples=2,
            metric='euclidean',
            cluster_selection_epsilon=0.1
        )
        
        cluster_labels = clusterer.fit_predict(umap_coords)
        console.print(f"✅ HDBSCAN 완료: {len(np.unique(cluster_labels))}개 클러스터")
        
        # 클러스터별로 데이터 분리
        unique_clusters = np.unique(cluster_labels)
        unique_clusters = unique_clusters[unique_clusters != -1]  # 노이즈 제외
        
        console.print(f"✅ 클러스터 수: {len(unique_clusters)}개")
        
        # 색상 팔레트 생성 (기존과 동일한 색상)
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD', 
                 '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9', '#F8C471', '#82E0AA', 
                 '#F1948A', '#D7BDE2', '#A9DFBF', '#F9E79F', '#D5A6BD', '#A3E4D7', 
                 '#FADBD8', '#FF6B6B']
        
        # Plotly 데이터 생성
        data = []
        
        for i, cluster_id in enumerate(unique_clusters):
            mask = cluster_labels == cluster_id
            cluster_coords = umap_coords[mask]
            
            if len(cluster_coords) > 0:
                # 클러스터 정보 찾기 (실제 클러스터 ID와 매핑)
                cluster_info = next((c for c in clusters if c['id'] == cluster_id), None)
                cluster_title = cluster_info['title'] if cluster_info else f"클러스터 {cluster_id}"
                
                trace = go.Scatter(
                    x=cluster_coords[:, 0],
                    y=cluster_coords[:, 1],
                    mode='markers',
                    marker=dict(
                        size=4,
                        color=colors[i % len(colors)],
                        opacity=0.8,
                        line=dict(width=0.5, color='white')
                    ),
                    name=cluster_title,
                    hovertemplate=f'<b>{cluster_title}</b><br>클러스터: {cluster_id}<extra></extra>'
                )
                data.append(trace)
        
        # 노이즈 포인트 추가 (클러스터에 속하지 않은 기사들)
        noise_mask = cluster_labels == -1
        if np.any(noise_mask):
            noise_coords = umap_coords[noise_mask]
            noise_trace = go.Scatter(
                x=noise_coords[:, 0],
                y=noise_coords[:, 1],
                mode='markers',
                marker=dict(
                    size=2,
                    color='#E8E8E8',
                    opacity=0.4,
                    line=dict(width=0.5, color='white')
                ),
                name='노이즈',
                hovertemplate='<b>노이즈</b><br>클러스터: -1<extra></extra>'
            )
            data.append(noise_trace)
        
        # 기존 umap_view.html 템플릿 읽기
        with open('umap_view_backup.html', 'r', encoding='utf-8') as f:
            template = f.read()
        
        # 데이터를 JavaScript 형태로 변환
        data_js = "[\n"
        for i, trace in enumerate(data):
            data_js += f"    {{'x': {trace.x.tolist() if hasattr(trace.x, 'tolist') else list(trace.x)}, "
            data_js += f"'y': {trace.y.tolist() if hasattr(trace.y, 'tolist') else list(trace.y)}, "
            data_js += f"'mode': '{trace.mode}', "
            data_js += f"'name': '{trace.name}', "
            data_js += f"'marker': {trace.marker}, "
            data_js += f"'hovertemplate': '{trace.hovertemplate}'}}"
            if i < len(data) - 1:
                data_js += ","
            data_js += "\n"
        data_js += "]"
        
        # 기존 하드코딩된 데이터를 실제 데이터로 교체
        template = template.replace(
            "allData = [{'x': [0.8413994908332825, 0.6810765266418457",
            f"allData = {data_js}"
        )
        
        # 통계 정보 업데이트
        template = template.replace(
            "let totalArticles = 1177; // 전체 기사 수",
            f"let totalArticles = {len(embeddings_data)}; // 전체 기사 수"
        )
        
        # HTML 파일 저장
        output_path = "umap_view.html"
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(template)
        
        console.print(f"✅ UMAP 시각화 생성 완료: {output_path}")
        console.print(f"   - 전체 기사: {len(embeddings_data)}개")
        console.print(f"   - 클러스터: {len(unique_clusters)}개")
        console.print(f"   - 노이즈: {np.sum(noise_mask) if np.any(noise_mask) else 0}개")
        
    except Exception as e:
        console.print(f"❌ UMAP 시각화 생성 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    generate_umap_view_1177()
