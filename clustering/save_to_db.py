#!/usr/bin/env python3
"""
클러스터링 결과를 데이터베이스에 저장
"""

import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
import json

# 프로젝트 모듈
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_manager import get_supabase_client
from clustering.sample_cluster import SampleClusterer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

class ClusterToDatabase:
    """클러스터 결과를 데이터베이스에 저장하는 클래스"""
    
    def __init__(self):
        """초기화"""
        self.supabase = get_supabase_client()
        self.clusterer = None
        self.clusters_info = []
        
    def load_clustering_results(self) -> bool:
        """클러스터링 결과 로드"""
        try:
            console.print("📊 클러스터링 결과 로드 중...")
            
            # 전체 데이터로 클러스터링 실행
            self.clusterer = SampleClusterer(sample_size=1017)
            
            if not self.clusterer.load_sample_data():
                return False
            
            if not self.clusterer.run_umap():
                return False
            
            if not self.clusterer.run_hdbscan():
                return False
            
            # 클러스터 분석
            self.clusters_info = self.clusterer.analyze_clusters()
            
            console.print(f"✅ 클러스터링 결과 로드 완료: {len(self.clusters_info)}개 클러스터")
            return True
            
        except Exception as e:
            console.print(f"❌ 클러스터링 결과 로드 실패: {e}")
            return False
    
    def save_clusters_to_issues(self) -> bool:
        """클러스터를 issues 테이블에 저장"""
        try:
            console.print("💾 클러스터를 issues 테이블에 저장 중...")
            
            saved_issues = []
            
            for cluster_info in self.clusters_info:
                # 클러스터 정보로 이슈 생성
                issue_data = {
                    'title': f"클러스터 {cluster_info['cluster_id']} - {self._get_cluster_theme(cluster_info)}",
                    'subtitle': f"{cluster_info['size']}개 기사",
                    'summary': self._generate_cluster_summary(cluster_info),
                    'left_view': self._generate_political_view(cluster_info, 'left'),
                    'center_view': self._generate_political_view(cluster_info, 'center'),
                    'right_view': self._generate_political_view(cluster_info, 'right'),
                    'source': "AI 클러스터링 (UMAP + HDBSCAN)",
                    'date': datetime.now().date().isoformat()
                }
                
                # 이슈 저장
                issue_result = self.supabase.client.table('issues').insert(issue_data).execute()
                
                if issue_result.data:
                    issue_id = issue_result.data[0]['id']
                    saved_issues.append({
                        'issue_id': issue_id,
                        'cluster_id': cluster_info['cluster_id'],
                        'size': cluster_info['size']
                    })
                    console.print(f"✅ 이슈 저장 완료: 클러스터 {cluster_info['cluster_id']} → 이슈 {issue_id}")
                else:
                    console.print(f"❌ 이슈 저장 실패: 클러스터 {cluster_info['cluster_id']}")
            
            console.print(f"✅ 총 {len(saved_issues)}개 이슈 저장 완료")
            return True
            
        except Exception as e:
            console.print(f"❌ 이슈 저장 실패: {e}")
            return False
    
    def save_article_mappings(self) -> bool:
        """기사-클러스터 매핑을 issue_articles 테이블에 저장"""
        try:
            console.print("💾 기사-클러스터 매핑을 issue_articles 테이블에 저장 중...")
            
            # 먼저 이슈 ID들을 조회
            issues_result = self.supabase.client.table('issues').select('id, title').like('title', '클러스터%').execute()
            
            if not issues_result.data:
                console.print("❌ 이슈 데이터를 찾을 수 없습니다.")
                return False
            
            # 클러스터 ID와 이슈 ID 매핑
            cluster_to_issue = {}
            for issue in issues_result.data:
                # 제목에서 클러스터 ID 추출
                title = issue['title']
                if '클러스터' in title:
                    try:
                        cluster_id = int(title.split('클러스터')[1].split()[0])
                        cluster_to_issue[cluster_id] = issue['id']
                    except:
                        continue
            
            total_mappings = 0
            
            for cluster_info in self.clusters_info:
                cluster_id = cluster_info['cluster_id']
                issue_id = cluster_to_issue.get(cluster_id)
                
                if not issue_id:
                    console.print(f"❌ 클러스터 {cluster_id}에 해당하는 이슈를 찾을 수 없습니다.")
                    continue
                
                # 해당 클러스터의 기사들에 대해 매핑 저장
                cluster_mask = self.clusterer.cluster_labels == cluster_id
                cluster_articles = self.clusterer.articles_data[cluster_mask]
                
                mappings = []
                for _, article in cluster_articles.iterrows():
                    # articles_cleaned의 original_article_id를 사용
                    original_article_id = article.get('original_article_id', article['id'])
                    mapping = {
                        'issue_id': issue_id,
                        'article_id': original_article_id,
                        'stance': 'center'  # 기본값, 나중에 개선
                    }
                    mappings.append(mapping)
                
                # 배치로 저장 (개별 처리로 오류 확인)
                if mappings:
                    success_count = 0
                    for mapping in mappings:
                        try:
                            result = self.supabase.client.table('issue_articles').insert(mapping).execute()
                            if result.data:
                                success_count += 1
                            else:
                                console.print(f"❌ 매핑 저장 실패: {mapping['article_id']}")
                        except Exception as e:
                            console.print(f"❌ 매핑 저장 오류: {mapping['article_id']} - {e}")
                    
                    total_mappings += success_count
                    console.print(f"✅ 클러스터 {cluster_id}: {success_count}/{len(mappings)}개 기사 매핑 저장")
            
            console.print(f"✅ 총 {total_mappings}개 기사-이슈 매핑 저장 완료")
            return True
            
        except Exception as e:
            console.print(f"❌ 매핑 저장 실패: {e}")
            return False
    
    def _get_cluster_theme(self, cluster_info: Dict[str, Any]) -> str:
        """클러스터 테마 추출"""
        representative = cluster_info.get('representative_article', {})
        title = representative.get('title_cleaned', '')
        
        # 간단한 키워드 기반 테마 분류
        if any(keyword in title for keyword in ['한수원', '에너지', '전력', '원자력']):
            return "에너지/한수원"
        elif any(keyword in title for keyword in ['의료', '건강보험', '병원', '진료']):
            return "의료/건강보험"
        elif any(keyword in title for keyword in ['북한', '북중', '비핵화', '한반도']):
            return "국제정치/북한"
        else:
            return "기타"
    
    def _generate_cluster_summary(self, cluster_info: Dict[str, Any]) -> str:
        """클러스터 요약 생성"""
        size = cluster_info['size']
        theme = self._get_cluster_theme(cluster_info)
        representative = cluster_info.get('representative_article', {})
        title = representative.get('title_cleaned', '')
        
        if len(title) > 100:
            title = title[:100] + "..."
        
        return f"{theme} 관련 이슈로 {size}개의 기사가 포함되어 있습니다. 대표 기사: {title}"
    
    def _generate_political_view(self, cluster_info: Dict[str, Any], view_type: str) -> str:
        """정치적 관점 생성"""
        theme = self._get_cluster_theme(cluster_info)
        
        if view_type == 'left':
            return f"{theme} 관련 진보적 관점 분석이 필요합니다."
        elif view_type == 'center':
            return f"{theme} 관련 중립적 관점 분석이 필요합니다."
        else:  # right
            return f"{theme} 관련 보수적 관점 분석이 필요합니다."
    
    def run_full_save(self) -> bool:
        """전체 저장 프로세스 실행"""
        try:
            console.print(Panel.fit(
                "[bold blue]💾 클러스터링 결과 데이터베이스 저장 시작[/bold blue]",
                title="데이터베이스 저장"
            ))
            
            # 1. 클러스터링 결과 로드
            if not self.load_clustering_results():
                return False
            
            # 2. 클러스터를 이슈로 저장
            if not self.save_clusters_to_issues():
                return False
            
            # 3. 기사-이슈 매핑 저장
            if not self.save_article_mappings():
                return False
            
            console.print(Panel.fit(
                "[bold green]✅ 데이터베이스 저장 완료![/bold green]",
                title="완료"
            ))
            
            return True
            
        except Exception as e:
            console.print(f"❌ 저장 프로세스 실패: {e}")
            return False

def main():
    """메인 함수"""
    saver = ClusterToDatabase()
    saver.run_full_save()

if __name__ == "__main__":
    main()
