#!/usr/bin/env python3
"""
내용 처리 실행 스크립트
- articles_cleaned 테이블에서 기사 조회
- 리드문 추출 + 내용 통합 수행
- merged_content 컬럼에 저장
"""

import sys
import os
from datetime import datetime, timedelta, timezone

# 프로젝트 루트를 Python 경로에 추가
sys.path.append('/Users/kimbeomyeong/opinion')

from utils.supabase_manager import SupabaseManager
from preprocessing.modules.content_processor import ContentProcessor

def main():
    """내용 처리 메인 함수"""
    print("🚀 내용 처리 프로세스 시작...")
    
    # Supabase 연결
    supabase_manager = SupabaseManager()
    if not supabase_manager.client:
        print("❌ Supabase 연결 실패")
        return
    
    # 내용 프로세서 초기화
    content_processor = ContentProcessor()
    
    try:
        # 내용 통합 처리 실행
        result = content_processor.process_content_merge()
        
        print(f"\n📊 내용 처리 결과:")
        print(f"  총 기사 수: {result['total_articles']}")
        print(f"  성공한 통합: {result['successful_merges']}개")
        print(f"  실패한 통합: {result['failed_merges']}개")
        print(f"  성공한 저장: {result['successful_saves']}개")
        print(f"  처리 시간: {result['processing_time']:.2f}초")
        print(f"  통합 전략: {result['merge_strategies']}")
        print(f"  성공 여부: {'✅ 성공' if result['success'] else '❌ 실패'}")
        
        if result.get('error_message'):
            print(f"  오류 메시지: {result['error_message']}")
            
    except Exception as e:
        print(f"❌ 내용 처리 프로세스 실패: {str(e)}")

if __name__ == "__main__":
    main()
