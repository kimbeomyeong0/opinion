#!/usr/bin/env python3
"""
UMAP 시각화 생성기 - 전체 1,177개 기사 포함
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

console = Console()

def generate_umap_visualization():
    """UMAP 시각화 생성 - 전체 1,177개 기사 포함"""
    console.print("🎨 UMAP 시각화 생성 시작...")
    
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
        
        # 임베딩 데이터 로드 (전체 1,177개) - 페이지네이션 사용
        console.print("📊 임베딩 데이터 로드 중...")
        embeddings = supabase.get_all_articles_for_embedding()
        
        if not embeddings:
            console.print("❌ 임베딩 데이터가 없습니다")
            return
        
        console.print(f"✅ 데이터 로드 완료: {len(clusters)}개 클러스터, {len(embeddings)}개 임베딩")
        
        # UMAP 좌표 추출
        umap_coords = []
        cluster_labels = []
        article_ids = []
        
        for emb in embeddings:
            if emb.get('umap_x') is not None and emb.get('umap_y') is not None:
                umap_coords.append([emb['umap_x'], emb['umap_y']])
                cluster_labels.append(emb.get('cluster_id', -1))
                article_ids.append(emb['article_id'])
        
        umap_coords = np.array(umap_coords)
        cluster_labels = np.array(cluster_labels)
        
        console.print(f"✅ UMAP 좌표 추출 완료: {len(umap_coords)}개")
        
        # 클러스터별로 데이터 분리
        unique_clusters = np.unique(cluster_labels)
        unique_clusters = unique_clusters[unique_clusters != -1]  # 노이즈 제외
        
        console.print(f"✅ 클러스터 수: {len(unique_clusters)}개")
        
        # 색상 팔레트 생성
        colors = px.colors.qualitative.Set3
        
        # Plotly 데이터 생성
        data = []
        
        for i, cluster_id in enumerate(unique_clusters):
            mask = cluster_labels == cluster_id
            cluster_coords = umap_coords[mask]
            
            if len(cluster_coords) > 0:
                # 클러스터 정보 찾기
                cluster_info = next((c for c in clusters if c['id'] == cluster_id), None)
                cluster_title = cluster_info['title'] if cluster_info else f"클러스터 {cluster_id}"
                
                trace = go.Scatter(
                    x=cluster_coords[:, 0],
                    y=cluster_coords[:, 1],
                    mode='markers',
                    marker=dict(
                        size=8,
                        color=colors[i % len(colors)],
                        opacity=0.7,
                        line=dict(width=0.5, color='white')
                    ),
                    name=cluster_title,
                    text=[f"클러스터 {cluster_id}<br>기사 {j+1}" for j in range(len(cluster_coords))],
                    hovertemplate='%{text}<br>X: %{x:.2f}<br>Y: %{y:.2f}<extra></extra>'
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
                    size=6,
                    color='lightgray',
                    opacity=0.5,
                    line=dict(width=0.5, color='white')
                ),
                name='노이즈',
                text=['노이즈 기사'] * len(noise_coords),
                hovertemplate='%{text}<br>X: %{x:.2f}<br>Y: %{y:.2f}<extra></extra>'
            )
            data.append(noise_trace)
        
        # 레이아웃 설정
        layout = go.Layout(
            title=dict(
                text="정치 이슈 UMAP 시각화 (전체 1,177개 기사)",
                x=0.5,
                font=dict(size=20, color='#2c3e50')
            ),
            xaxis=dict(
                showgrid=False,
                showticklabels=False,
                zeroline=False,
                showline=False
            ),
            yaxis=dict(
                showgrid=False,
                showticklabels=False,
                zeroline=False,
                showline=False
            ),
            width=1200,
            height=800,
            plot_bgcolor='white',
            paper_bgcolor='white',
            hovermode='closest',
            showlegend=True,
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.01
            ),
            margin=dict(l=0, r=0, t=60, b=0)
        )
        
        # HTML 생성
        fig = go.Figure(data=data, layout=layout)
        
        html_content = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>정치 이슈 UMAP 시각화</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 2.5rem;
            font-weight: 300;
        }}
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            padding: 30px;
            background: #f8f9fa;
        }}
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
        }}
        .stat-number {{
            font-size: 2rem;
            font-weight: bold;
            color: #2c3e50;
            margin-bottom: 5px;
        }}
        .stat-label {{
            color: #7f8c8d;
            font-size: 0.9rem;
        }}
        .plot-container {{
            padding: 30px;
        }}
        #plotly-container {{
            width: 100%;
            height: 800px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>정치 이슈 UMAP 시각화</h1>
            <p>전체 1,177개 기사의 이슈 분포를 2차원으로 시각화</p>
        </div>
        
        <div class="stats">
            <div class="stat-card">
                <div class="stat-number">{len(embeddings)}</div>
                <div class="stat-label">전체 기사</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{len(unique_clusters)}</div>
                <div class="stat-label">클러스터 수</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{np.sum(noise_mask) if np.any(noise_mask) else 0}</div>
                <div class="stat-label">노이즈 기사</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">9</div>
                <div class="stat-label">언론사 수</div>
            </div>
        </div>
        
        <div class="plot-container">
            <div id="plotly-container"></div>
        </div>
    </div>
    
    <script>
        const data = {fig.to_json()};
        Plotly.newPlot('plotly-container', data.data, data.layout, {{displayModeBar: true}});
    </script>
</body>
</html>
"""
        
        # HTML 파일 저장
        output_path = "umap_visualization_full.html"
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        console.print(f"✅ UMAP 시각화 생성 완료: {output_path}")
        console.print(f"   - 전체 기사: {len(embeddings)}개")
        console.print(f"   - 클러스터: {len(unique_clusters)}개")
        console.print(f"   - 노이즈: {np.sum(noise_mask) if np.any(noise_mask) else 0}개")
        
    except Exception as e:
        console.print(f"❌ UMAP 시각화 생성 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    generate_umap_visualization()