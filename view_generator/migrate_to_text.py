#!/usr/bin/env python3
"""
view 컬럼을 JSONB에서 TEXT로 변경하는 마이그레이션 스크립트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_manager import get_supabase_client

def migrate_view_columns_to_text():
    """view 컬럼들을 TEXT 타입으로 변경"""
    
    supabase = get_supabase_client()
    if not supabase.client:
        print("❌ Supabase 연결 실패")
        return False
    
    try:
        print("🔄 view 컬럼을 TEXT 타입으로 변경 중...")
        
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
            print(f"실행: {query}")
            result = supabase.client.rpc('exec_sql', {'sql': query}).execute()
            print(f"✅ 성공: {query}")
        
        print("🎉 view 컬럼 TEXT 타입 변경 완료!")
        return True
        
    except Exception as e:
        print(f"❌ 마이그레이션 실패: {str(e)}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("Issues 테이블 view 컬럼 TEXT 타입 변경")
    print("=" * 60)
    
    success = migrate_view_columns_to_text()
    
    if success:
        print("\n✅ 마이그레이션 완료!")
        print("이제 view 컬럼들이 TEXT 타입으로 저장됩니다.")
    else:
        print("\n❌ 마이그레이션 실패!")
        print("수동으로 Supabase에서 컬럼 타입을 변경해주세요.")
