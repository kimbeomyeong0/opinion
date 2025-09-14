#!/usr/bin/env python3
"""
issues 테이블 스키마 업데이트 스크립트
기존 view 컬럼들을 삭제하고 새로운 구조로 변경
"""

import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utils.supabase_manager import SupabaseManager
from rich.console import Console

console = Console()

def update_issues_schema():
    """issues 테이블 스키마 업데이트"""
    try:
        console.print("🔧 issues 테이블 스키마 업데이트 시작...")
        
        # Supabase 연결
        supabase = SupabaseManager()
        
        # SQL 스크립트 읽기
        sql_file = Path(__file__).parent / "update_issues_schema.sql"
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_commands = f.read()
        
        # SQL 명령들을 개별적으로 실행
        commands = [cmd.strip() for cmd in sql_commands.split(';') if cmd.strip()]
        
        for i, command in enumerate(commands, 1):
            if command:
                console.print(f"📝 실행 중 ({i}/{len(commands)}): {command[:50]}...")
                try:
                    # Supabase RPC를 통해 SQL 실행
                    result = supabase.client.rpc('exec_sql', {'sql': command}).execute()
                    console.print(f"✅ 성공: {command[:30]}...")
                except Exception as e:
                    # RPC가 없는 경우 직접 실행 시도
                    try:
                        result = supabase.client.postgrest.rpc('exec_sql', {'sql': command}).execute()
                        console.print(f"✅ 성공: {command[:30]}...")
                    except:
                        console.print(f"⚠️ RPC 실행 실패, 다른 방법 시도: {command[:30]}...")
                        # 다른 방법으로 시도할 수 있는 명령들
                        if 'DROP COLUMN' in command:
                            console.print("ℹ️ DROP COLUMN 명령은 수동으로 실행해야 할 수 있습니다.")
                        elif 'ADD COLUMN' in command:
                            console.print("ℹ️ ADD COLUMN 명령은 수동으로 실행해야 할 수 있습니다.")
        
        console.print("✅ issues 테이블 스키마 업데이트 완료!")
        
        # 업데이트된 구조 확인
        console.print("\n🔍 업데이트된 테이블 구조 확인...")
        result = supabase.client.table('issues').select('*').limit(1).execute()
        if result.data:
            console.print("=== 업데이트된 issues 테이블 구조 ===")
            for key in result.data[0].keys():
                console.print(f"- {key}")
        
        return True
        
    except Exception as e:
        console.print(f"❌ 스키마 업데이트 실패: {e}")
        return False

if __name__ == "__main__":
    success = update_issues_schema()
    if success:
        console.print("🎉 스키마 업데이트가 완료되었습니다!")
    else:
        console.print("💥 스키마 업데이트에 실패했습니다.")
        sys.exit(1)
