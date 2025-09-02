import os
from datetime import datetime
from typing import Dict, Optional
from dotenv import load_dotenv
from supabase import create_client, Client

# .env 파일 로드
load_dotenv()


class SupabaseClient:
    def __init__(self):
        """Supabase 클라이언트 초기화"""
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL과 SUPABASE_KEY 환경변수가 필요합니다.")
        
        self.client: Client = create_client(self.url, self.key)
    
    def insert_article(self, article: Dict) -> Optional[str]:
        """
        articles 테이블에 데이터를 삽입하고 생성된 article의 id를 반환
        
        Args:
            article: {"media_id": str, "title": str, "content": str, "url": str, "published_at": datetime}
            
        Returns:
            str: 생성된 article의 id, 실패시 None
        """
        try:
            # 필수 필드 검증
            required_fields = ["media_id", "title", "content", "url", "published_at"]
            for field in required_fields:
                if field not in article:
                    raise ValueError(f"필수 필드 '{field}'가 누락되었습니다.")
            
            # published_at이 datetime 객체인지 확인
            if not isinstance(article["published_at"], datetime):
                raise ValueError("published_at은 datetime 객체여야 합니다.")
            
            # Supabase에 데이터 삽입
            response = self.client.table("articles").insert(article).execute()
            
            if response.data and len(response.data) > 0:
                return response.data[0].get("id")
            else:
                return None
                
        except Exception as e:
            print(f"Article 삽입 중 오류 발생: {str(e)}")
            return None


# 전역 인스턴스 (지연 초기화)
_supabase_client = None

def get_supabase_client():
    """Supabase 클라이언트 인스턴스를 반환 (싱글톤 패턴)"""
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = SupabaseClient()
    return _supabase_client
