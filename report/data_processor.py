#!/usr/bin/env python3
"""
데이터 처리 전용 클래스
Supabase에서 데이터를 조회하고 가공하는 로직을 담당
"""

from typing import Dict, Any, List
from utils.supabase_manager import SupabaseManager
from rich.console import Console

console = Console()


class DataProcessor:
    """데이터 처리 전용 클래스"""
    
    def __init__(self):
        """초기화"""
        self.supabase_manager = SupabaseManager()
    
    def get_article_stats(self, issue_id: str) -> Dict[str, int]:
        """이슈별 기사 통계 조회"""
        try:
            result = self.supabase_manager.client.table('articles').select(
                'media_id, media_outlets!inner(bias)'
            ).eq('issue_id', issue_id).execute()
            
            if not result.data:
                return {"total": 0, "left": 0, "center": 0, "right": 0}
            
            stats = {"total": len(result.data), "left": 0, "center": 0, "right": 0}
            
            for item in result.data:
                bias = item['media_outlets']['bias']
                if bias in stats:
                    stats[bias] += 1
            
            return stats
            
        except Exception as e:
            console.print(f"❌ 기사 통계 조회 실패: {str(e)}")
            return {"total": 0, "left": 0, "center": 0, "right": 0}
    
    def fetch_all_issues(self) -> List[Dict[str, Any]]:
        """모든 이슈 데이터 조회"""
        try:
            result = self.supabase_manager.client.table('issues').select(
                'id, title, issue_summary, created_at, source'
            ).order('created_at', desc=True).execute()
            
            if not result.data:
                console.print("❌ 이슈 데이터가 없습니다.")
                return []
            
            console.print(f"✅ {len(result.data)}개 이슈 데이터 조회 완료")
            return result.data
            
        except Exception as e:
            console.print(f"❌ 이슈 데이터 조회 실패: {str(e)}")
            return []
    
    def enrich_issues_with_stats(self, issues: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """이슈 데이터에 통계 정보 추가"""
        try:
            for issue in issues:
                stats = self.get_article_stats(issue['id'])
                issue['total_articles'] = stats['total']
            
            return issues
            
        except Exception as e:
            console.print(f"❌ 이슈 통계 정보 추가 실패: {str(e)}")
            return issues
    
    def sort_issues_by_article_count(self, issues: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """기사 수 순으로 이슈 정렬 (많은 순서대로)"""
        try:
            sorted_issues = sorted(issues, key=lambda x: x.get('total_articles', 0), reverse=True)
            console.print("✅ 기사 수 순으로 정렬 완료")
            return sorted_issues
            
        except Exception as e:
            console.print(f"❌ 이슈 정렬 실패: {str(e)}")
            return issues
    
    def get_all_stats_for_issues(self, issues: List[Dict[str, Any]]) -> List[Dict[str, int]]:
        """모든 이슈의 통계 정보 조회"""
        try:
            all_stats = []
            for issue in issues:
                stats = self.get_article_stats(issue['id'])
                all_stats.append(stats)
            
            return all_stats
            
        except Exception as e:
            console.print(f"❌ 전체 이슈 통계 조회 실패: {str(e)}")
            return []
    
    def process_all_data(self) -> tuple[List[Dict[str, Any]], List[Dict[str, int]]]:
        """모든 데이터 처리 파이프라인"""
        try:
            # 1. 이슈 데이터 조회
            issues = self.fetch_all_issues()
            if not issues:
                return [], []
            
            # 2. 통계 정보 추가
            issues = self.enrich_issues_with_stats(issues)
            
            # 3. 정렬
            issues = self.sort_issues_by_article_count(issues)
            
            # 4. 모든 통계 정보 조회
            all_stats = self.get_all_stats_for_issues(issues)
            
            console.print(f"✅ 데이터 처리 완료: {len(issues)}개 이슈")
            return issues, all_stats
            
        except Exception as e:
            console.print(f"❌ 데이터 처리 실패: {str(e)}")
            return [], []
