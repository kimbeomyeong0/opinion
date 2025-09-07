#!/usr/bin/env python3
"""
Issues 및 Issue Articles 테이블 데이터 삭제 스크립트
- issues 테이블의 모든 데이터 삭제
- issue_articles 테이블의 모든 데이터 삭제
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.supabase_manager import get_supabase_client
from rich.console import Console
from rich.table import Table
from rich.prompt import Confirm

console = Console()

def clear_issues_data():
    """Issues 관련 테이블 데이터 삭제"""
    try:
        supabase = get_supabase_client()
        
        # 현재 데이터 개수 확인
        console.print("\n[bold blue]📊 현재 데이터 현황 확인[/bold blue]")
        
        # issue_articles 테이블 개수 확인
        issue_articles_count = supabase.client.table('issue_articles').select('count', count='exact').execute()
        issue_articles_total = issue_articles_count.count if issue_articles_count.count is not None else 0
        
        # issues 테이블 개수 확인
        issues_count = supabase.client.table('issues').select('count', count='exact').execute()
        issues_total = issues_count.count if issues_count.count is not None else 0
        
        # 테이블로 현황 표시
        table = Table(title="현재 데이터 현황")
        table.add_column("테이블", style="cyan")
        table.add_column("데이터 개수", style="magenta")
        
        table.add_row("issue_articles", str(issue_articles_total))
        table.add_row("issues", str(issues_total))
        
        console.print(table)
        
        if issue_articles_total == 0 and issues_total == 0:
            console.print("\n[bold green]✅ 이미 모든 데이터가 삭제되어 있습니다.[/bold green]")
            return True
        
        # 삭제 확인
        console.print(f"\n[bold yellow]⚠️  다음 데이터가 삭제됩니다:[/bold yellow]")
        console.print(f"   - issue_articles: {issue_articles_total}개")
        console.print(f"   - issues: {issues_total}개")
        
        if not Confirm.ask("\n정말로 삭제하시겠습니까?", default=False):
            console.print("[bold red]❌ 삭제가 취소되었습니다.[/bold red]")
            return False
        
        # issue_articles 테이블 삭제 (먼저 삭제 - 외래키 제약 때문)
        console.print("\n[bold blue]🗑️  issue_articles 테이블 데이터 삭제 중...[/bold blue]")
        if issue_articles_total > 0:
            result = supabase.client.table('issue_articles').delete().neq('issue_id', '00000000-0000-0000-0000-000000000000').execute()
            console.print(f"✅ issue_articles 테이블 {issue_articles_total}개 데이터 삭제 완료")
        else:
            console.print("✅ issue_articles 테이블은 이미 비어있습니다")
        
        # issues 테이블 삭제
        console.print("\n[bold blue]🗑️  issues 테이블 데이터 삭제 중...[/bold blue]")
        if issues_total > 0:
            result = supabase.client.table('issues').delete().neq('id', '00000000-0000-0000-0000-000000000000').execute()
            console.print(f"✅ issues 테이블 {issues_total}개 데이터 삭제 완료")
        else:
            console.print("✅ issues 테이블은 이미 비어있습니다")
        
        # 삭제 후 확인
        console.print("\n[bold blue]📊 삭제 후 데이터 현황 확인[/bold blue]")
        
        # issue_articles 테이블 개수 재확인
        issue_articles_count_after = supabase.client.table('issue_articles').select('count', count='exact').execute()
        issue_articles_after = issue_articles_count_after.count if issue_articles_count_after.count is not None else 0
        
        # issues 테이블 개수 재확인
        issues_count_after = supabase.client.table('issues').select('count', count='exact').execute()
        issues_after = issues_count_after.count if issues_count_after.count is not None else 0
        
        # 결과 테이블
        result_table = Table(title="삭제 후 데이터 현황")
        result_table.add_column("테이블", style="cyan")
        result_table.add_column("삭제 전", style="red")
        result_table.add_column("삭제 후", style="green")
        result_table.add_column("상태", style="yellow")
        
        result_table.add_row(
            "issue_articles", 
            str(issue_articles_total), 
            str(issue_articles_after),
            "✅ 완료" if issue_articles_after == 0 else "❌ 실패"
        )
        result_table.add_row(
            "issues", 
            str(issues_total), 
            str(issues_after),
            "✅ 완료" if issues_after == 0 else "❌ 실패"
        )
        
        console.print(result_table)
        
        if issue_articles_after == 0 and issues_after == 0:
            console.print("\n[bold green]🎉 모든 데이터 삭제가 완료되었습니다![/bold green]")
            return True
        else:
            console.print("\n[bold red]❌ 일부 데이터 삭제에 실패했습니다.[/bold red]")
            return False
            
    except Exception as e:
        console.print(f"\n[bold red]❌ 데이터 삭제 중 오류 발생: {str(e)}[/bold red]")
        return False

def main():
    """메인 함수"""
    console.print("[bold blue]🧹 Issues 데이터 삭제 스크립트[/bold blue]")
    console.print("=" * 50)
    
    success = clear_issues_data()
    
    if success:
        console.print("\n[bold green]✅ 스크립트 실행 완료[/bold green]")
    else:
        console.print("\n[bold red]❌ 스크립트 실행 실패[/bold red]")
        sys.exit(1)

if __name__ == "__main__":
    main()
