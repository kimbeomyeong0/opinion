#!/usr/bin/env python3
"""
클러스터링 디버깅 스크립트 - 단계별 확인
"""

import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
import json

# 프로젝트 모듈
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_manager import get_supabase_client
from rich.console import Console
from rich.panel import Panel

console = Console()

def debug_step_1_data_load():
    """1단계: 데이터 로드 디버깅"""
    console.print("🔍 1단계: 데이터 로드 디버깅 시작")
    
    try:
        supabase = get_supabase_client()
        console.print("✅ Supabase 클라이언트 초기화 완료")
        
        # 임베딩 데이터 조회
        console.print("📊 임베딩 데이터 조회 중...")
        result = supabase.client.table('articles_embeddings').select(
            'cleaned_article_id, embedding_vector, model_name'
        ).eq('embedding_type', 'combined').limit(10).execute()  # 처음 10개만
        
        console.print(f"✅ 임베딩 데이터 조회 완료: {len(result.data)}개")
        
        if result.data:
            console.print("📋 첫 번째 임베딩 샘플:")
            sample = result.data[0]
            console.print(f"   - cleaned_article_id: {sample['cleaned_article_id']}")
            console.print(f"   - model_name: {sample['model_name']}")
            console.print(f"   - embedding_vector 타입: {type(sample['embedding_vector'])}")
            
            # 임베딩 벡터 확인
            if isinstance(sample['embedding_vector'], str):
                embedding_vector = json.loads(sample['embedding_vector'])
                console.print(f"   - 벡터 차원: {len(embedding_vector)}")
            else:
                console.print(f"   - 벡터 차원: {len(sample['embedding_vector'])}")
        
        return True
        
    except Exception as e:
        console.print(f"❌ 1단계 실패: {e}")
        return False

def debug_step_2_articles_load():
    """2단계: 기사 데이터 로드 디버깅"""
    console.print("🔍 2단계: 기사 데이터 로드 디버깅 시작")
    
    try:
        supabase = get_supabase_client()
        
        # 임베딩 데이터 조회 (10개만)
        result = supabase.client.table('articles_embeddings').select(
            'cleaned_article_id'
        ).eq('embedding_type', 'combined').limit(10).execute()
        
        article_ids = [item['cleaned_article_id'] for item in result.data]
        console.print(f"📊 조회할 기사 ID: {len(article_ids)}개")
        
        # 기사 메타데이터 조회
        console.print("📊 기사 메타데이터 조회 중...")
        articles_result = supabase.client.table('articles_cleaned').select(
            'id, title_cleaned, lead_paragraph'
        ).in_('id', article_ids).execute()
        
        console.print(f"✅ 기사 데이터 조회 완료: {len(articles_result.data)}개")
        
        if articles_result.data:
            console.print("📋 첫 번째 기사 샘플:")
            sample = articles_result.data[0]
            console.print(f"   - id: {sample['id']}")
            console.print(f"   - title: {sample['title_cleaned'][:50]}...")
            console.print(f"   - lead: {sample['lead_paragraph'][:50]}...")
        
        return True
        
    except Exception as e:
        console.print(f"❌ 2단계 실패: {e}")
        return False

def debug_step_3_umap_import():
    """3단계: UMAP 라이브러리 import 디버깅"""
    console.print("🔍 3단계: UMAP 라이브러리 import 디버깅")
    
    try:
        import umap
        console.print("✅ UMAP 라이브러리 import 성공")
        console.print(f"   - UMAP 버전: {umap.__version__}")
        
        # 간단한 UMAP 객체 생성 테스트
        reducer = umap.UMAP(n_components=2, random_state=42)
        console.print("✅ UMAP 객체 생성 성공")
        
        return True
        
    except Exception as e:
        console.print(f"❌ 3단계 실패: {e}")
        return False

def debug_step_4_hdbscan_import():
    """4단계: HDBSCAN 라이브러리 import 디버깅"""
    console.print("🔍 4단계: HDBSCAN 라이브러리 import 디버깅")
    
    try:
        import hdbscan
        console.print("✅ HDBSCAN 라이브러리 import 성공")
        try:
            console.print(f"   - HDBSCAN 버전: {hdbscan.__version__}")
        except:
            console.print("   - HDBSCAN 버전: 확인 불가")
        
        # 간단한 HDBSCAN 객체 생성 테스트
        clusterer = hdbscan.HDBSCAN(min_cluster_size=5)
        console.print("✅ HDBSCAN 객체 생성 성공")
        
        return True
        
    except Exception as e:
        console.print(f"❌ 4단계 실패: {e}")
        return False

def debug_step_5_small_test():
    """5단계: 작은 데이터로 전체 파이프라인 테스트"""
    console.print("🔍 5단계: 작은 데이터로 전체 파이프라인 테스트")
    
    try:
        import umap
        import hdbscan
        import numpy as np
        
        # 가짜 데이터 생성 (10개 기사, 1536차원)
        console.print("📊 가짜 데이터 생성 중...")
        fake_embeddings = np.random.rand(10, 1536)
        console.print(f"✅ 가짜 데이터 생성 완료: {fake_embeddings.shape}")
        
        # UMAP 테스트
        console.print("🔄 UMAP 테스트 중...")
        reducer = umap.UMAP(n_components=2, random_state=42, verbose=True)
        umap_result = reducer.fit_transform(fake_embeddings)
        console.print(f"✅ UMAP 완료: {umap_result.shape}")
        
        # HDBSCAN 테스트
        console.print("🔄 HDBSCAN 테스트 중...")
        clusterer = hdbscan.HDBSCAN(min_cluster_size=2, min_samples=2)
        cluster_labels = clusterer.fit_predict(umap_result)
        console.print(f"✅ HDBSCAN 완료: {cluster_labels}")
        
        return True
        
    except Exception as e:
        console.print(f"❌ 5단계 실패: {e}")
        return False

def main():
    """메인 디버깅 함수"""
    console.print(Panel.fit(
        "[bold blue]🔍 클러스터링 디버깅 시작[/bold blue]",
        title="디버깅"
    ))
    
    steps = [
        ("데이터 로드", debug_step_1_data_load),
        ("기사 데이터 로드", debug_step_2_articles_load),
        ("UMAP import", debug_step_3_umap_import),
        ("HDBSCAN import", debug_step_4_hdbscan_import),
        ("작은 데이터 테스트", debug_step_5_small_test)
    ]
    
    for step_name, step_func in steps:
        console.print(f"\n{'='*50}")
        console.print(f"단계: {step_name}")
        console.print(f"{'='*50}")
        
        if not step_func():
            console.print(f"❌ {step_name} 단계에서 실패했습니다.")
            break
        
        console.print(f"✅ {step_name} 단계 완료!")
    
    console.print(Panel.fit(
        "[bold green]🔍 디버깅 완료[/bold green]",
        title="완료"
    ))

if __name__ == "__main__":
    main()
