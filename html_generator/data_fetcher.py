#!/usr/bin/env python3
"""
이슈 데이터 조회 모듈
Supabase에서 이슈 데이터를 가져와서 HTML용으로 가공
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime
from rich.console import Console

from utils.supabase_manager import get_supabase_client
from html_generator.config import get_config

console = Console()

class IssueDataFetcher:
    """이슈 데이터 조회 클래스"""
    
    def __init__(self):
        """초기화"""
        self.config = get_config()
        self.supabase = get_supabase_client()
        self.issues_data = None
    
    def fetch_issues(self) -> bool:
        """이슈 데이터 조회"""
        try:
            console.print("📊 이슈 데이터 조회 중...")
            
            # 최신 이슈들 조회 (source 기준 내림차순)
            result = self.supabase.client.table('issues').select(
                'id, title, subtitle, summary, left_view, center_view, right_view, '
                'left_source, center_source, right_source, source, date'
            ).order('source', desc=True).limit(self.config["max_issues_display"]).execute()
            
            if not result.data:
                console.print("❌ 이슈 데이터가 없습니다.")
                return False
            
            self.issues_data = pd.DataFrame(result.data)
            console.print(f"✅ 이슈 데이터 조회 완료: {len(result.data)}개")
            return True
            
        except Exception as e:
            console.print(f"❌ 이슈 데이터 조회 실패: {e}")
            return False
    
    def process_issues_data(self) -> list:
        """이슈 데이터를 HTML용으로 가공"""
        if self.issues_data is None:
            return []
        
        processed_issues = []
        
        for _, issue in self.issues_data.iterrows():
            # 소스 수치 정리
            left_source = int(issue.get('left_source', 0))
            center_source = int(issue.get('center_source', 0))
            right_source = int(issue.get('right_source', 0))
            total_source = int(issue.get('source', 0))
            
            # 관점별 비율 계산
            if total_source > 0:
                left_ratio = (left_source / total_source) * 100
                center_ratio = (center_source / total_source) * 100
                right_ratio = (right_source / total_source) * 100
            else:
                left_ratio = center_ratio = right_ratio = 0
            
            # 이슈 데이터 가공
            processed_issue = {
                'id': issue.get('id'),
                'title': issue.get('title', ''),
                'subtitle': issue.get('subtitle', ''),
                'summary': issue.get('summary', ''),
                'date': issue.get('date', ''),
                'views': {
                    'left': {
                        'content': issue.get('left_view', ''),
                        'source_count': left_source,
                        'ratio': round(left_ratio, 1),
                        'label': '지지',
                        'color': self.config['theme_colors']['primary']
                    },
                    'center': {
                        'content': issue.get('center_view', ''),
                        'source_count': center_source,
                        'ratio': round(center_ratio, 1),
                        'label': '중립',
                        'color': self.config['theme_colors']['secondary']
                    },
                    'right': {
                        'content': issue.get('right_view', ''),
                        'source_count': right_source,
                        'ratio': round(right_ratio, 1),
                        'label': '비판',
                        'color': self.config['theme_colors']['accent']
                    }
                },
                'total_source': total_source,
                'has_views': any([
                    issue.get('left_view', '').strip(),
                    issue.get('center_view', '').strip(),
                    issue.get('right_view', '').strip()
                ])
            }
            
            processed_issues.append(processed_issue)
        
        console.print(f"✅ 이슈 데이터 가공 완료: {len(processed_issues)}개")
        return processed_issues
    
    def get_issues_summary(self) -> dict:
        """전체 이슈 요약 정보 반환"""
        if self.issues_data is None or self.issues_data.empty:
            return {
                'total_issues': 0,
                'total_sources': 0,
                'generation_date': datetime.now().strftime('%Y-%m-%d %H:%M'),
                'date_range': 'N/A'
            }
        
        total_issues = len(self.issues_data)
        total_sources = self.issues_data['source'].sum()
        
        # 날짜 범위 계산
        dates = pd.to_datetime(self.issues_data['date'], errors='coerce')
        if not dates.isna().all():
            min_date = dates.min().strftime('%Y-%m-%d')
            max_date = dates.max().strftime('%Y-%m-%d')
            date_range = f"{min_date} ~ {max_date}" if min_date != max_date else min_date
        else:
            date_range = 'N/A'
        
        return {
            'total_issues': total_issues,
            'total_sources': int(total_sources),
            'generation_date': datetime.now().strftime('%Y-%m-%d %H:%M'),
            'date_range': date_range
        }
