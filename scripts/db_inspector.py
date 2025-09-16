#!/usr/bin/env python3
"""
데이터베이스 조회 및 분석 스크립트
개발을 위한 데이터베이스 상태 확인 도구
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.supabase_manager import SupabaseManager

console = Console()

class DatabaseInspector:
    """데이터베이스 조회 및 분석 클래스"""
    
    def __init__(self):
        """초기화"""
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            console.print("❌ 데이터베이스 연결 실패", style="red")
            sys.exit(1)
    
    def get_table_info(self) -> Dict[str, Any]:
        """모든 테이블의 기본 정보 조회"""
        tables_info = {}
        
        # 주요 테이블들
        tables = ['articles', 'media_outlets', 'issues', 'issue_articles']
        
        for table in tables:
            try:
                # 테이블 레코드 수 조회
                count_result = self.supabase_manager.client.table(table).select('*', count='exact').execute()
                count = count_result.count if count_result.count is not None else 0
                
                # 최근 데이터 조회 (created_at 또는 published_at 기준)
                recent_data = None
                if table == 'articles':
                    recent_result = self.supabase_manager.client.table(table).select('*').order('published_at', desc=True).limit(1).execute()
                else:
                    recent_result = self.supabase_manager.client.table(table).select('*').order('created_at', desc=True).limit(1).execute()
                
                if recent_result.data:
                    recent_data = recent_result.data[0]
                
                tables_info[table] = {
                    'count': count,
                    'recent_data': recent_data
                }
                
            except Exception as e:
                console.print(f"❌ {table} 테이블 조회 실패: {str(e)}", style="red")
                tables_info[table] = {'count': 0, 'recent_data': None, 'error': str(e)}
        
        return tables_info
    
    def display_table_summary(self):
        """테이블 요약 정보 표시"""
        console.print("\n📊 데이터베이스 테이블 요약", style="bold blue")
        
        tables_info = self.get_table_info()
        
        # 테이블 생성
        table = Table(title="테이블 정보")
        table.add_column("테이블명", style="cyan", no_wrap=True)
        table.add_column("레코드 수", style="magenta", justify="right")
        table.add_column("최신 데이터", style="green")
        table.add_column("상태", style="yellow")
        
        for table_name, info in tables_info.items():
            count = info.get('count', 0)
            recent_data = info.get('recent_data')
            error = info.get('error')
            
            if error:
                status = f"❌ 오류"
                recent_str = "-"
            else:
                status = "✅ 정상"
                if recent_data:
                    if table_name == 'articles':
                        recent_str = recent_data.get('published_at', 'Unknown')[:10]
                    else:
                        recent_str = recent_data.get('created_at', 'Unknown')[:10]
                else:
                    recent_str = "데이터 없음"
            
            table.add_row(table_name, str(count), recent_str, status)
        
        console.print(table)
    
    def get_articles_stats(self):
        """기사 관련 통계 정보"""
        console.print("\n📰 기사 통계", style="bold blue")
        
        try:
            # 전체 기사 수
            total_result = self.supabase_manager.client.table('articles').select('*', count='exact').execute()
            total_count = total_result.count if total_result.count is not None else 0
            
            # 언론사별 기사 수
            media_stats = {}
            media_outlets = self.supabase_manager.client.table('media_outlets').select('*').execute()
            
            for outlet in media_outlets.data:
                media_id = outlet['id']
                media_name = outlet['name']
                
                count_result = self.supabase_manager.client.table('articles').select('*', count='exact').eq('media_id', media_id).execute()
                count = count_result.count if count_result.count is not None else 0
                media_stats[media_name] = count
            
            # 최근 7일간 기사 수
            week_ago = (datetime.now() - timedelta(days=7)).isoformat()
            recent_result = self.supabase_manager.client.table('articles').select('*', count='exact').gte('published_at', week_ago).execute()
            recent_count = recent_result.count if recent_result.count is not None else 0
            
            # 전처리 상태별 통계
            preprocessed_result = self.supabase_manager.client.table('articles').select('*', count='exact').eq('is_preprocessed', True).execute()
            preprocessed_count = preprocessed_result.count if preprocessed_result.count is not None else 0
            
            # 통계 표시
            stats_table = Table(title="기사 통계")
            stats_table.add_column("항목", style="cyan")
            stats_table.add_column("값", style="magenta", justify="right")
            
            stats_table.add_row("전체 기사 수", str(total_count))
            stats_table.add_row("최근 7일 기사 수", str(recent_count))
            stats_table.add_row("전처리 완료 기사 수", str(preprocessed_count))
            stats_table.add_row("전처리 미완료 기사 수", str(total_count - preprocessed_count))
            
            console.print(stats_table)
            
            # 언론사별 통계
            if media_stats:
                console.print("\n📺 언론사별 기사 수", style="bold green")
                media_table = Table()
                media_table.add_column("언론사", style="cyan")
                media_table.add_column("기사 수", style="magenta", justify="right")
                
                for media_name, count in sorted(media_stats.items(), key=lambda x: x[1], reverse=True):
                    media_table.add_row(media_name, str(count))
                
                console.print(media_table)
            
        except Exception as e:
            console.print(f"❌ 기사 통계 조회 실패: {str(e)}", style="red")
    
    def get_sample_data(self, table_name: str, limit: int = 5):
        """테이블의 샘플 데이터 조회"""
        console.print(f"\n🔍 {table_name} 테이블 샘플 데이터 (최대 {limit}개)", style="bold blue")
        
        try:
            if table_name == 'articles':
                result = self.supabase_manager.client.table(table_name).select('*').order('published_at', desc=True).limit(limit).execute()
            else:
                result = self.supabase_manager.client.table(table_name).select('*').limit(limit).execute()
            
            if not result.data:
                console.print("데이터가 없습니다.", style="yellow")
                return
            
            # 첫 번째 레코드의 컬럼명으로 테이블 생성
            columns = list(result.data[0].keys())
            
            sample_table = Table(title=f"{table_name} 샘플 데이터")
            for col in columns:
                sample_table.add_column(col, style="cyan", max_width=30)
            
            for row in result.data:
                row_data = []
                for col in columns:
                    value = str(row.get(col, ''))
                    if len(value) > 30:
                        value = value[:27] + "..."
                    row_data.append(value)
                sample_table.add_row(*row_data)
            
            console.print(sample_table)
            
        except Exception as e:
            console.print(f"❌ {table_name} 샘플 데이터 조회 실패: {str(e)}", style="red")
    
    def check_data_quality(self):
        """데이터 품질 검사"""
        console.print("\n🔍 데이터 품질 검사", style="bold blue")
        
        try:
            # 기사 데이터 품질 검사
            articles_result = self.supabase_manager.client.table('articles').select('*').execute()
            
            if not articles_result.data:
                console.print("기사 데이터가 없습니다.", style="yellow")
                return
            
            total_articles = len(articles_result.data)
            issues = []
            
            for article in articles_result.data:
                # 필수 필드 검사
                if not article.get('title'):
                    issues.append(f"제목이 없는 기사: ID {article.get('id')}")
                if not article.get('url'):
                    issues.append(f"URL이 없는 기사: ID {article.get('id')}")
                if not article.get('media_id'):
                    issues.append(f"언론사 ID가 없는 기사: ID {article.get('id')}")
                if not article.get('published_at'):
                    issues.append(f"발행일이 없는 기사: ID {article.get('id')}")
            
            # 중복 URL 검사
            urls = [article.get('url') for article in articles_result.data if article.get('url')]
            duplicate_urls = set([url for url in urls if urls.count(url) > 1])
            
            if duplicate_urls:
                issues.append(f"중복 URL 발견: {len(duplicate_urls)}개")
            
            # 결과 표시
            quality_table = Table(title="데이터 품질 검사 결과")
            quality_table.add_column("검사 항목", style="cyan")
            quality_table.add_column("결과", style="magenta")
            
            quality_table.add_row("전체 기사 수", str(total_articles))
            quality_table.add_row("품질 이슈 수", str(len(issues)))
            quality_table.add_row("중복 URL 수", str(len(duplicate_urls)))
            
            console.print(quality_table)
            
            if issues:
                console.print("\n⚠️ 발견된 이슈들:", style="bold yellow")
                for issue in issues[:10]:  # 최대 10개만 표시
                    console.print(f"  • {issue}", style="yellow")
                if len(issues) > 10:
                    console.print(f"  ... 및 {len(issues) - 10}개 더", style="yellow")
            else:
                console.print("✅ 데이터 품질에 문제가 없습니다.", style="green")
                
        except Exception as e:
            console.print(f"❌ 데이터 품질 검사 실패: {str(e)}", style="red")
    
    def run_full_inspection(self):
        """전체 데이터베이스 검사 실행"""
        console.print("🔍 데이터베이스 전체 검사 시작", style="bold blue")
        
        # 테이블 요약
        self.display_table_summary()
        
        # 기사 통계
        self.get_articles_stats()
        
        # 데이터 품질 검사
        self.check_data_quality()
        
        # 샘플 데이터 표시
        console.print("\n📋 샘플 데이터", style="bold blue")
        self.get_sample_data('articles', 3)
        self.get_sample_data('media_outlets', 5)
        
        console.print("\n✅ 데이터베이스 검사 완료", style="bold green")

def main():
    """메인 함수"""
    inspector = DatabaseInspector()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'summary':
            inspector.display_table_summary()
        elif command == 'stats':
            inspector.get_articles_stats()
        elif command == 'quality':
            inspector.check_data_quality()
        elif command == 'sample':
            table_name = sys.argv[2] if len(sys.argv) > 2 else 'articles'
            limit = int(sys.argv[3]) if len(sys.argv) > 3 else 5
            inspector.get_sample_data(table_name, limit)
        else:
            console.print("사용법:", style="bold")
            console.print("  python db_inspector.py [command]")
            console.print("\n명령어:")
            console.print("  summary  - 테이블 요약 정보")
            console.print("  stats    - 기사 통계")
            console.print("  quality  - 데이터 품질 검사")
            console.print("  sample [table] [limit] - 샘플 데이터 조회")
            console.print("  (명령어 없음) - 전체 검사")
    else:
        inspector.run_full_inspection()

if __name__ == "__main__":
    main()
