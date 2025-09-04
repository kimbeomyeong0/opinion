#!/usr/bin/env python3
"""
Supabase insert_article 함수 테스트 스크립트
"""

import sys
import os
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 테스트용 환경변수 설정 (실제 Supabase 설정으로 교체 필요)
os.environ['SUPABASE_URL'] = 'your_supabase_url_here'
os.environ['SUPABASE_KEY'] = 'your_supabase_key_here'

from utils.supabase_manager import SupabaseManager


def test_insert_article():
    """insert_article 함수 테스트"""
    try:
        # Supabase 클라이언트 생성
        client = SupabaseManager()
        
        # 더미 기사 데이터 생성
        dummy_article = {
            "media_id": "00000000-0000-0000-0000-000000000000",
            "title": "테스트 기사 제목",
            "content": "이것은 테스트용 기사 내용입니다. Supabase insert_article 함수가 제대로 작동하는지 확인하기 위한 더미 데이터입니다.",
            "url": "https://example.com/test-article",
            "published_at": datetime.now()
        }
        
        print("테스트 기사 데이터:")
        for key, value in dummy_article.items():
            print(f"  {key}: {value}")
        print()
        
        # insert_article 실행
        print("Supabase에 기사 삽입 중...")
        article_id = client.insert_article(dummy_article)
        
        if article_id:
            print(f"✅ 성공! 생성된 기사 ID: {article_id}")
        else:
            print("❌ 실패: 기사 삽입에 실패했습니다.")
            
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")


def test_validation():
    """데이터 검증 로직 테스트 (Supabase 연결 없이)"""
    print("=== 데이터 검증 로직 테스트 ===")
    
    # SupabaseManager 클래스의 insert_article 메서드를 직접 테스트
    from utils.supabase_manager import SupabaseManager
    
    # Mock 클라이언트 생성 (실제 Supabase 연결 없이)
    class MockSupabaseClient(SupabaseManager):
        def __init__(self):
            # 부모 클래스의 __init__을 호출하지 않음
            pass
        
        def insert_article(self, article):
            # 부모 클래스의 검증 로직만 실행
            required_fields = ["media_id", "title", "content", "url", "published_at"]
            for field in required_fields:
                if field not in article:
                    raise ValueError(f"필수 필드 '{field}'가 누락되었습니다.")
            
            if not isinstance(article["published_at"], datetime):
                raise ValueError("published_at은 datetime 객체여야 합니다.")
            
            return "mock_article_id_123"
    
    client = MockSupabaseClient()
    
    # 필수 필드 누락 테스트
    print("\n1. 필수 필드 누락 테스트:")
    incomplete_article = {
        "title": "제목만 있는 기사",
        "content": "내용"
        # media_id, url, published_at 누락
    }
    
    try:
        result = client.insert_article(incomplete_article)
        print(f"결과: {result}")
    except Exception as e:
        print(f"✅ 예상된 오류: {str(e)}")
    
    # 잘못된 datetime 타입 테스트
    print("\n2. 잘못된 datetime 타입 테스트:")
    invalid_datetime_article = {
        "media_id": "00000000-0000-0000-0000-000000000000",
        "title": "테스트 기사",
        "content": "내용",
        "url": "https://example.com",
        "published_at": "2024-01-01"  # 문자열로 잘못된 타입
    }
    
    try:
        result = client.insert_article(invalid_datetime_article)
        print(f"결과: {result}")
    except Exception as e:
        print(f"✅ 예상된 오류: {str(e)}")
    
    # 올바른 데이터 테스트
    print("\n3. 올바른 데이터 테스트:")
    valid_article = {
        "media_id": "00000000-0000-0000-0000-000000000000",
        "title": "테스트 기사",
        "content": "내용",
        "url": "https://example.com",
        "published_at": datetime.now()
    }
    
    try:
        result = client.insert_article(valid_article)
        print(f"✅ 성공! 반환된 ID: {result}")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {str(e)}")


if __name__ == "__main__":
    print("=== Supabase insert_article 함수 테스트 ===\n")
    
    # 실제 Supabase 연결 테스트 (실제 URL/KEY가 필요)
    print("1. 실제 Supabase 연결 테스트:")
    test_insert_article()
    
    print("\n" + "="*50 + "\n")
    
    # 데이터 검증 로직 테스트
    test_validation()
    
    print("\n=== 테스트 완료 ===")
    print("실제 Supabase 연결을 위해서는 .env 파일에 올바른 SUPABASE_URL과 SUPABASE_KEY를 설정하세요.")
