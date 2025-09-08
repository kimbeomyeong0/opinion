#!/usr/bin/env python3
"""
데이터 마이그레이션 통합 스크립트
- 스키마 마이그레이션
- 텍스트 변환
"""

import sys
import os
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# 프로젝트 루트를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.supabase_manager import get_supabase_client

console = Console()

class DataMigrator:
    """데이터 마이그레이션 클래스"""
    
    def __init__(self):
        self.supabase = get_supabase_client()
        if not self.supabase.client:
            raise ValueError("Supabase 연결 실패")
    
    def show_current_schema(self):
        """현재 스키마 확인"""
        console.print(Panel("현재 Issues 테이블 스키마", style="bold blue"))
        
        try:
            # 샘플 데이터로 현재 컬럼 확인
            result = self.supabase.client.table('issues').select('*').limit(1).execute()
            
            if not result.data:
                console.print("📭 issues 테이블에 데이터가 없습니다.")
                return
            
            sample_record = result.data[0]
            
            table = Table(title="현재 컬럼 구조")
            table.add_column("컬럼명", style="cyan")
            table.add_column("타입", style="yellow")
            table.add_column("값 예시", style="white")
            
            for key, value in sample_record.items():
                value_type = type(value).__name__
                value_preview = str(value)[:30] + "..." if len(str(value)) > 30 else str(value)
                table.add_row(key, value_type, value_preview)
            
            console.print(table)
            
        except Exception as e:
            console.print(f"❌ 스키마 확인 실패: {str(e)}")
    
    def show_migration_plan(self):
        """마이그레이션 계획 표시"""
        console.print(Panel("마이그레이션 계획", style="bold green"))
        
        console.print("\n📋 변경 사항:")
        console.print("   1. left_view → left_source (기사 수 저장)")
        console.print("   2. center_view → center_source (기사 수 저장)")
        console.print("   3. right_view → right_source (기사 수 저장)")
        console.print("   4. left_view (새 컬럼) - TEXT 타입 (관점 저장)")
        console.print("   5. center_view (새 컬럼) - TEXT 타입 (관점 저장)")
        console.print("   6. right_view (새 컬럼) - TEXT 타입 (관점 저장)")
        
        console.print("\n🔧 필요한 SQL 쿼리:")
        console.print("""
-- 1. 컬럼명 변경
ALTER TABLE issues RENAME COLUMN left_view TO left_source;
ALTER TABLE issues RENAME COLUMN center_view TO center_source;
ALTER TABLE issues RENAME COLUMN right_view TO right_source;

-- 2. 새로운 view 컬럼 추가 (TEXT 타입)
ALTER TABLE issues ADD COLUMN left_view TEXT;
ALTER TABLE issues ADD COLUMN center_view TEXT;
ALTER TABLE issues ADD COLUMN right_view TEXT;

-- 3. 컬럼 설명 추가
COMMENT ON COLUMN issues.left_source IS '좌파 성향 기사 수';
COMMENT ON COLUMN issues.center_source IS '중도 성향 기사 수';
COMMENT ON COLUMN issues.right_source IS '우파 성향 기사 수';
COMMENT ON COLUMN issues.left_view IS '진보적 관점 (TEXT 형식)';
COMMENT ON COLUMN issues.center_view IS '중도적 관점 (TEXT 형식)';
COMMENT ON COLUMN issues.right_view IS '보수적 관점 (TEXT 형식)';
        """)
    
    def migrate_view_columns_to_text(self):
        """view 컬럼들을 TEXT 타입으로 변경"""
        try:
            console.print("🔄 view 컬럼을 TEXT 타입으로 변경 중...")
            
            # SQL 쿼리 실행
            migration_queries = [
                # 기존 JSONB 컬럼을 TEXT로 변경
                "ALTER TABLE issues ALTER COLUMN left_view TYPE TEXT USING left_view::TEXT;",
                "ALTER TABLE issues ALTER COLUMN center_view TYPE TEXT USING center_view::TEXT;", 
                "ALTER TABLE issues ALTER COLUMN right_view TYPE TEXT USING right_view::TEXT;",
                
                # 컬럼 코멘트 추가
                "COMMENT ON COLUMN issues.left_view IS '진보적 관점 (TEXT 형식)';",
                "COMMENT ON COLUMN issues.center_view IS '중도적 관점 (TEXT 형식)';",
                "COMMENT ON COLUMN issues.right_view IS '보수적 관점 (TEXT 형식)';"
            ]
            
            for query in migration_queries:
                console.print(f"실행: {query}")
                result = self.supabase.client.rpc('exec_sql', {'sql': query}).execute()
                console.print(f"✅ 성공: {query}")
            
            console.print("🎉 view 컬럼 TEXT 타입 변경 완료!")
            return True
            
        except Exception as e:
            console.print(f"❌ 마이그레이션 실패: {str(e)}")
            return False
    
    def update_code_references(self):
        """코드에서 참조하는 컬럼명 업데이트"""
        console.print(Panel("코드 업데이트 필요사항", style="bold yellow"))
        
        console.print("\n📝 수정이 필요한 파일들:")
        console.print("   1. view_generator/bias_view_generator.py")
        console.print("   2. view_generator/run_view_generator.py")
        console.print("   3. clustering/cluster.py (이슈 생성 부분)")
        
        console.print("\n🔧 주요 변경사항:")
        console.print("   - left_view, center_view, right_view → left_source, center_source, right_source")
        console.print("   - 새로운 left_view, center_view, right_view 컬럼은 TEXT 타입으로 사용")

def main():
    """메인 함수"""
    console.print("🎯 데이터 마이그레이션 도구")
    
    try:
        migrator = DataMigrator()
        
        if len(sys.argv) > 1:
            command = sys.argv[1]
            
            if command == "schema":
                migrator.show_current_schema()
                migrator.show_migration_plan()
                migrator.update_code_references()
            elif command == "migrate":
                migrator.migrate_view_columns_to_text()
            else:
                console.print("❌ 잘못된 명령어입니다.")
                console.print("사용법:")
                console.print("  python data_migrator.py schema    # 스키마 확인 및 계획")
                console.print("  python data_migrator.py migrate   # 마이그레이션 실행")
        else:
            console.print("\n사용법:")
            console.print("  python data_migrator.py schema    # 스키마 확인 및 계획")
            console.print("  python data_migrator.py migrate   # 마이그레이션 실행")
            
    except Exception as e:
        console.print(f"❌ 마이그레이션 도구 초기화 실패: {str(e)}")

if __name__ == "__main__":
    main()
