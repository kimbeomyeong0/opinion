#!/usr/bin/env python3
"""
성향별 관점 생성 실행 스크립트
"""

import asyncio
import sys
import os
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# 프로젝트 루트를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from view_generator.bias_view_generator import get_view_generator
from utils.supabase_manager import get_supabase_client

console = Console()

async def generate_views_for_issue(issue_id: str):
    """특정 이슈의 성향별 관점 생성"""
    
    console.print(Panel(f"이슈 {issue_id} 성향별 관점 생성", style="bold blue"))
    
    # View Generator 초기화
    try:
        generator = get_view_generator()
    except Exception as e:
        console.print(f"❌ View Generator 초기화 실패: {str(e)}")
        return False
    
    # 성향별 관점 생성
    try:
        bias_views = await generator.generate_all_bias_views(issue_id)
        
        if not bias_views:
            console.print("❌ 성향별 관점 생성에 실패했습니다.")
            return False
        
        # 결과 표시
        console.print("\n📊 생성된 성향별 관점:")
        
        for bias, view in bias_views.items():
            console.print(f"\n🔸 {bias.upper()} 성향:")
            # TEXT 타입이므로 그대로 출력 (줄바꿈 처리 불필요)
            console.print(view)
        
        # 데이터베이스 업데이트
        success = generator.update_issue_views(issue_id, bias_views)
        
        if success:
            console.print(f"\n✅ 이슈 {issue_id} 성향별 관점 생성 및 저장 완료!")
            return True
        else:
            console.print(f"\n❌ 이슈 {issue_id} 성향별 관점 저장 실패")
            return False
            
    except Exception as e:
        console.print(f"❌ 성향별 관점 생성 중 오류: {str(e)}")
        return False

async def generate_views_for_all_issues():
    """모든 이슈의 성향별 관점 생성"""
    
    console.print(Panel("모든 이슈 성향별 관점 생성", style="bold blue"))
    
    # Supabase 클라이언트 초기화
    supabase = get_supabase_client()
    if not supabase.client:
        console.print("❌ Supabase 클라이언트를 초기화할 수 없습니다.")
        return False
    
    # 모든 이슈 조회
    try:
        result = supabase.client.table('issues').select('id, title').execute()
        
        if not result.data:
            console.print("📭 생성할 이슈가 없습니다.")
            return True
        
        console.print(f"📊 총 {len(result.data)}개 이슈의 성향별 관점을 생성합니다.")
        
        success_count = 0
        
        for issue in result.data:
            issue_id = issue['id']
            issue_title = issue['title']
            
            console.print(f"\n🎯 이슈 처리 중: {issue_title[:50]}...")
            
            success = await generate_views_for_issue(issue_id)
            if success:
                success_count += 1
        
        console.print(f"\n🎉 완료! {success_count}/{len(result.data)}개 이슈 처리 완료")
        return success_count == len(result.data)
        
    except Exception as e:
        console.print(f"❌ 이슈 목록 조회 실패: {str(e)}")
        return False

def show_issue_list():
    """이슈 목록 표시"""
    
    console.print(Panel("Issues 테이블 현황", style="bold green"))
    
    # Supabase 클라이언트 초기화
    supabase = get_supabase_client()
    if not supabase.client:
        console.print("❌ Supabase 클라이언트를 초기화할 수 없습니다.")
        return
    
    try:
        result = supabase.client.table('issues').select('id, title, left_source, center_source, right_source, left_view, center_view, right_view').execute()
        
        if not result.data:
            console.print("📭 이슈가 없습니다.")
            return
        
        # 테이블 생성
        table = Table(title="Issues 목록")
        table.add_column("ID", style="cyan", width=8)
        table.add_column("제목", style="white", width=50)
        table.add_column("좌파 관점", style="red", width=15)
        table.add_column("중도 관점", style="yellow", width=15)
        table.add_column("우파 관점", style="blue", width=15)
        
        for issue in result.data:
            # source 컬럼 (기사 수)
            left_source = issue.get('left_source', '')
            center_source = issue.get('center_source', '')
            right_source = issue.get('right_source', '')
            
            # view 컬럼 (관점)
            left_view = issue.get('left_view', '')
            center_view = issue.get('center_view', '')
            right_view = issue.get('right_view', '')
            
            # source는 기사 수, view는 관점 생성 여부
            left_display = f"기사 {left_source}개" if str(left_source).isdigit() else f"기사 {left_source}개"
            center_display = f"기사 {center_source}개" if str(center_source).isdigit() else f"기사 {center_source}개"
            right_display = f"기사 {right_source}개" if str(right_source).isdigit() else f"기사 {right_source}개"
            
            # 관점 생성 여부 표시
            if left_view:
                left_display += " + 관점"
            if center_view:
                center_display += " + 관점"
            if right_view:
                right_display += " + 관점"
            
            table.add_row(
                issue['id'][:8],
                issue['title'][:47] + "..." if len(issue['title']) > 50 else issue['title'],
                left_display,
                center_display,
                right_display
            )
        
        console.print(table)
        
    except Exception as e:
        console.print(f"❌ 이슈 목록 조회 실패: {str(e)}")

async def main():
    """메인 함수"""
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "list":
            show_issue_list()
        elif command == "all":
            await generate_views_for_all_issues()
        elif command.startswith("issue:"):
            issue_id = command.split(":", 1)[1]
            await generate_views_for_issue(issue_id)
        else:
            console.print("❌ 잘못된 명령어입니다.")
            console.print("사용법:")
            console.print("  python run_view_generator.py list          # 이슈 목록 보기")
            console.print("  python run_view_generator.py all           # 모든 이슈 처리")
            console.print("  python run_view_generator.py issue:ID      # 특정 이슈 처리")
    else:
        console.print("🎯 성향별 관점 생성기")
        console.print("\n사용법:")
        console.print("  python run_view_generator.py list          # 이슈 목록 보기")
        console.print("  python run_view_generator.py all           # 모든 이슈 처리")
        console.print("  python run_view_generator.py issue:ID      # 특정 이슈 처리")

if __name__ == "__main__":
    asyncio.run(main())
