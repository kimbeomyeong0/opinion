#!/usr/bin/env python3
"""
데이터베이스 테이블 스키마 및 데이터 확인 스크립트
"""

import sys
import os
from typing import Dict, Any, List
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, Confirm

# 프로젝트 모듈
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.supabase_manager import get_supabase_client

console = Console()

class DatabaseInspector:
    """데이터베이스 검사기"""
    
    def __init__(self):
        """초기화"""
        self.supabase = get_supabase_client()
        self.tables = [
            'articles',
            'articles_cleaned', 
            'issues',
            'issue_articles',
            'media_outlets'
        ]
    
    def show_table_schema(self, table_name: str) -> bool:
        """테이블 스키마 표시"""
        try:
            console.print(f"\n📋 {table_name} 테이블 스키마:")
            
            # 샘플 데이터로 스키마 추론
            result = self.supabase.client.table(table_name).select('*').limit(1).execute()
            
            if result.data:
                sample = result.data[0]
                schema_table = Table(title=f"{table_name} 스키마")
                schema_table.add_column("컬럼명", style="cyan")
                schema_table.add_column("타입", style="green")
                schema_table.add_column("샘플값", style="yellow")
                
                for key, value in sample.items():
                    value_type = type(value).__name__
                    sample_value = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                    schema_table.add_row(key, value_type, sample_value)
                
                console.print(schema_table)
                return True
            else:
                console.print(f"❌ {table_name} 테이블에 데이터가 없습니다.")
                return False
                
        except Exception as e:
            console.print(f"❌ {table_name} 스키마 조회 실패: {e}")
            return False
    
    def show_table_stats(self, table_name: str) -> bool:
        """테이블 통계 표시"""
        try:
            console.print(f"\n📊 {table_name} 테이블 통계:")
            
            # 전체 데이터 수 조회
            result = self.supabase.client.table(table_name).select('*', count='exact').execute()
            total_count = result.count if hasattr(result, 'count') else len(result.data)
            
            # 샘플 데이터 조회
            sample_result = self.supabase.client.table(table_name).select('*').limit(5).execute()
            
            stats_table = Table(title=f"{table_name} 통계")
            stats_table.add_column("항목", style="cyan")
            stats_table.add_column("값", style="green")
            
            stats_table.add_row("전체 데이터 수", f"{total_count:,}개")
            stats_table.add_row("샘플 데이터", f"{len(sample_result.data)}개")
            
            console.print(stats_table)
            
            # 샘플 데이터 표시
            if sample_result.data:
                console.print(f"\n📝 {table_name} 샘플 데이터:")
                sample_table = Table()
                
                # 첫 번째 행의 키들을 컬럼으로 설정
                first_row = sample_result.data[0]
                for key in first_row.keys():
                    sample_table.add_column(key, style="yellow")
                
                # 샘플 데이터 추가
                for row in sample_result.data:
                    sample_table.add_row(*[str(value)[:30] + "..." if len(str(value)) > 30 else str(value) for value in row.values()])
                
                console.print(sample_table)
            
            return True
            
        except Exception as e:
            console.print(f"❌ {table_name} 통계 조회 실패: {e}")
            return False
    
    
    def show_article_analysis(self) -> bool:
        """기사 데이터 분석"""
        try:
            console.print("\n📰 기사 데이터 분석:")
            
            # articles_cleaned 통계
            result = self.supabase.client.table('articles_cleaned').select('*', count='exact').execute()
            total_articles = result.count if hasattr(result, 'count') else len(result.data)
            
            # 샘플 기사들
            sample = self.supabase.client.table('articles_cleaned').select(
                'id, title_cleaned, merged_content'
            ).limit(3).execute()
            
            stats_table = Table(title="기사 통계")
            stats_table.add_column("항목", style="cyan")
            stats_table.add_column("값", style="green")
            
            stats_table.add_row("전체 기사", f"{total_articles:,}개")
            
            if sample.data:
                avg_content_length = sum(len(item.get('merged_content', '')) for item in sample.data) / len(sample.data)
                stats_table.add_row("평균 내용 길이", f"{avg_content_length:.0f}자")
            
            console.print(stats_table)
            
            # 샘플 기사 표시
            if sample.data:
                console.print(f"\n📝 기사 샘플:")
                for i, article in enumerate(sample.data, 1):
                    content = article.get('merged_content', '')
                    console.print(f"\n{i}. {article.get('title_cleaned', 'N/A')}")
                    console.print(f"   내용 길이: {len(content)}자")
                    console.print(f"   미리보기: {content[:100]}...")
            
            return True
            
        except Exception as e:
            console.print(f"❌ 기사 분석 실패: {e}")
            return False
    
    def show_all_tables(self) -> bool:
        """모든 테이블 개요 표시"""
        try:
            console.print("\n🗂️  전체 테이블 개요:")
            
            overview_table = Table(title="데이터베이스 테이블 개요")
            overview_table.add_column("테이블명", style="cyan")
            overview_table.add_column("데이터 수", style="green")
            overview_table.add_column("상태", style="yellow")
            
            for table_name in self.tables:
                try:
                    result = self.supabase.client.table(table_name).select('*', count='exact').execute()
                    count = result.count if hasattr(result, 'count') else len(result.data)
                    status = "✅ 정상" if count > 0 else "⚠️  비어있음"
                    overview_table.add_row(table_name, f"{count:,}개", status)
                except Exception as e:
                    overview_table.add_row(table_name, "❌ 오류", str(e)[:30])
            
            console.print(overview_table)
            return True
            
        except Exception as e:
            console.print(f"❌ 테이블 개요 조회 실패: {e}")
            return False
    
    def interactive_mode(self):
        """대화형 모드"""
        console.print(Panel.fit(
            "[bold blue]🔍 데이터베이스 검사기[/bold blue]\n"
            "테이블 스키마와 데이터를 확인할 수 있습니다.",
            title="DB Inspector"
        ))
        
        while True:
            console.print("\n[bold yellow]선택하세요:[/bold yellow]")
            console.print("1. 전체 테이블 개요")
            console.print("2. 특정 테이블 스키마 보기")
            console.print("3. 특정 테이블 통계 보기")
            console.print("4. 기사 데이터 분석")
            console.print("0. 종료")
            
            choice = Prompt.ask("선택", choices=["0", "1", "2", "3", "4"], default="1")
            
            if choice == "0":
                console.print("👋 종료합니다.")
                break
            elif choice == "1":
                self.show_all_tables()
            elif choice == "2":
                table_name = Prompt.ask("테이블명", choices=self.tables)
                self.show_table_schema(table_name)
            elif choice == "3":
                table_name = Prompt.ask("테이블명", choices=self.tables)
                self.show_table_stats(table_name)
            elif choice == "4":
                self.show_article_analysis()

def main():
    """메인 함수"""
    try:
        inspector = DatabaseInspector()
        inspector.interactive_mode()
    except Exception as e:
        console.print(f"❌ 실행 중 오류 발생: {e}")

if __name__ == "__main__":
    main()
