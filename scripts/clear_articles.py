#!/usr/bin/env python3
"""
Articles 테이블 데이터 삭제 스크립트
"""

import os
import sys
from dotenv import load_dotenv
from supabase import create_client

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

def get_supabase_client():
    """Supabase 클라이언트 생성"""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if not url or not key:
        print("❌ Supabase 환경변수가 설정되지 않았습니다.")
        return None
    
    return create_client(url, key)

def main():
    """메인 함수"""
    print("🗑️  Articles 테이블 데이터 삭제 시작...")
    
    # Supabase 클라이언트 초기화
    supabase = get_supabase_client()
    if not supabase:
        return
    
    try:
        # 삭제 전 데이터 개수 확인
        result = supabase.table('articles').select('*', count='exact').execute()
        print(f"📊 삭제 전 articles 데이터 개수: {result.count}")
        
        if result.count == 0:
            print("✅ articles 테이블이 이미 비어있습니다.")
            return
        
        # 모든 데이터 삭제
        print("🗑️  데이터 삭제 중...")
        delete_result = supabase.table('articles').delete().neq('id', '00000000-0000-0000-0000-000000000000').execute()
        
        # 삭제 후 데이터 개수 확인
        result_after = supabase.table('articles').select('*', count='exact').execute()
        print(f"📊 삭제 후 articles 데이터 개수: {result_after.count}")
        
        print("✅ Articles 테이블 데이터 삭제 완료!")
        
    except Exception as e:
        print(f"❌ 삭제 중 오류 발생: {e}")

if __name__ == "__main__":
    main()
