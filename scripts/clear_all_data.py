#!/usr/bin/env python3
"""
모든 테이블 데이터 삭제 스크립트 (media_outlets 제외)
media_outlets 테이블을 제외하고 모든 테이블의 데이터를 삭제합니다.
"""

import os
import sys
from dotenv import load_dotenv
from supabase import create_client
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

console = Console()

def get_supabase_client():
    """Supabase 클라이언트 생성"""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if not url or not key:
        console.print("❌ Supabase 환경변수가 설정되지 않았습니다.")
        console.print("SUPABASE_URL과 SUPABASE_KEY를 .env 파일에 설정해주세요.")
        return None
    
    return create_client(url, key)

def get_table_counts(supabase):
    """각 테이블의 데이터 개수 조회"""
    tables = [
        'articles',
        'articles_cleaned', 
        'articles_embeddings',
        'issues',
        'issue_articles',
        'media_outlets'  # 참고용으로만 조회
    ]
    
    counts = {}
    for table in tables:
        try:
            result = supabase.table(table).select('*', count='exact').execute()
            counts[table] = result.count
        except Exception as e:
            console.print(f"❌ {table} 테이블 조회 실패: {e}")
            counts[table] = "오류"
    
    return counts

def clear_table_data(supabase, table_name):
    """특정 테이블의 모든 데이터 삭제"""
    try:
        # 모든 데이터 삭제 (WHERE 조건 없이)
        result = supabase.table(table_name).delete().neq('id', '00000000-0000-0000-0000-000000000000').execute()
        console.print(f"✅ {table_name} 테이블 데이터 삭제 완료")
        return True
    except Exception as e:
        console.print(f"❌ {table_name} 테이블 삭제 실패: {e}")
        return False

def main():
    """메인 함수"""
    console.print(Panel.fit(
        "[bold red]⚠️  모든 테이블 데이터 삭제 스크립트 ⚠️[/bold red]\n"
        "[yellow]media_outlets 테이블을 제외하고 모든 데이터가 삭제됩니다.[/yellow]",
        title="데이터 삭제 경고",
        border_style="red"
    ))
    
    # 확인 요청
    confirm = input("\n정말로 모든 데이터를 삭제하시겠습니까? (yes/no): ").lower().strip()
    if confirm != 'yes':
        console.print("❌ 작업이 취소되었습니다.")
        return
    
    # Supabase 클라이언트 초기화
    supabase = get_supabase_client()
    if not supabase:
        return
    
    console.print("\n📊 삭제 전 데이터 현황:")
    
    # 삭제 전 데이터 개수 조회
    before_counts = get_table_counts(supabase)
    
    # 테이블로 표시
    table = Table(title="테이블별 데이터 개수 (삭제 전)")
    table.add_column("테이블명", style="cyan")
    table.add_column("데이터 개수", style="magenta")
    table.add_column("삭제 대상", style="red")
    
    for table_name, count in before_counts.items():
        is_target = table_name != 'media_outlets'
        delete_status = "❌ 제외" if not is_target else "✅ 삭제"
        table.add_row(table_name, str(count), delete_status)
    
    console.print(table)
    
    # 삭제 대상 테이블들 (외래키 제약조건을 고려한 순서)
    # 1. articles_embeddings (다른 테이블을 참조)
    # 2. issue_articles (다른 테이블을 참조)  
    # 3. articles_cleaned (issue_articles에서 참조)
    # 4. articles (articles_embeddings에서 참조)
    # 5. issues (issue_articles에서 참조)
    target_tables = ['articles_embeddings', 'issue_articles', 'articles_cleaned', 'articles', 'issues']
    
    console.print(f"\n🗑️  {len(target_tables)}개 테이블의 데이터를 삭제합니다...")
    
    # 각 테이블 데이터 삭제
    success_count = 0
    for table_name in target_tables:
        if clear_table_data(supabase, table_name):
            success_count += 1
    
    console.print(f"\n📊 삭제 후 데이터 현황:")
    
    # 삭제 후 데이터 개수 조회
    after_counts = get_table_counts(supabase)
    
    # 결과 테이블로 표시
    result_table = Table(title="테이블별 데이터 개수 (삭제 후)")
    result_table.add_column("테이블명", style="cyan")
    result_table.add_column("삭제 전", style="yellow")
    result_table.add_column("삭제 후", style="green")
    result_table.add_column("상태", style="bold")
    
    for table_name in before_counts.keys():
        before = before_counts[table_name]
        after = after_counts[table_name]
        
        if table_name == 'media_outlets':
            status = "🛡️ 보호됨"
        elif after == 0:
            status = "✅ 삭제 완료"
        else:
            status = "⚠️ 부분 삭제"
        
        result_table.add_row(table_name, str(before), str(after), status)
    
    console.print(result_table)
    
    console.print(f"\n🎉 데이터 삭제 완료!")
    console.print(f"✅ 성공: {success_count}/{len(target_tables)}개 테이블")
    console.print(f"🛡️ 보호: media_outlets 테이블")

if __name__ == "__main__":
    main()
