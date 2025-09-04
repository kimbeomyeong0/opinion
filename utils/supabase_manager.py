#!/usr/bin/env python3
"""
Supabase 데이터베이스 관리 클래스
"""

import os
from datetime import datetime
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from supabase import create_client, Client
from rich.console import Console

load_dotenv()

console = Console()

class SupabaseManager:
    """Supabase 데이터베이스 관리 클래스"""
    
    def __init__(self):
        """Supabase 클라이언트 초기화"""
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_KEY")
        
        if not self.url or not self.key:
            console.print("❌ Supabase 환경변수가 설정되지 않았습니다.")
            console.print("SUPABASE_URL과 SUPABASE_KEY를 .env 파일에 설정해주세요.")
            self.client = None
        else:
            self.client = create_client(self.url, self.key)
            console.print("✅ Supabase 클라이언트 초기화 완료")
    
    def get_media_outlet(self, name: str) -> Optional[Dict[str, Any]]:
        """
        media_outlets 테이블에서 언론사 정보 조회
        
        Args:
            name: 언론사 이름
            
        Returns:
            언론사 정보 또는 None
        """
        if not self.client:
            return None
            
        try:
            result = self.client.table('media_outlets').select('*').eq('name', name).execute()
            if result.data:
                return result.data[0]
            return None
        except Exception as e:
            console.print(f"❌ 언론사 조회 실패: {str(e)}")
            return None
    
    def create_media_outlet(self, name: str, bias: str = "center", website: str = "") -> Optional[int]:
        """
        새 언론사 추가
        
        Args:
            name: 언론사 이름
            bias: 정치적 성향 (left, center, right)
            website: 웹사이트 URL
            
        Returns:
            생성된 언론사 ID 또는 None
        """
        if not self.client:
            return None
            
        try:
            data = {
                'name': name,
                'bias': bias,
                'website': website
            }
            result = self.client.table('media_outlets').insert(data).execute()
            if result.data:
                console.print(f"✅ 언론사 생성 완료: {name} (ID: {result.data[0]['id']})")
                return result.data[0]['id']
            return None
        except Exception as e:
            console.print(f"❌ 언론사 생성 실패: {str(e)}")
            return None
    
    def insert_article(self, article: Dict[str, Any]) -> bool:
        """
        articles 테이블에 기사 삽입
        
        Args:
            article: 기사 데이터 딕셔너리
            
        Returns:
            삽입 성공 여부
        """
        if not self.client:
            return False
        
        try:
            # published_at이 datetime 객체인 경우 isoformat 문자열로 변환
            if 'published_at' in article and isinstance(article['published_at'], datetime):
                article['published_at'] = article['published_at'].isoformat()
            
            result = self.client.table('articles').insert(article).execute()
            if result.data:
                console.print(f"✅ 기사 삽입 성공: {article.get('title', 'Unknown')[:50]}...")
                return True
            else:
                console.print(f"❌ 기사 삽입 실패: {article.get('title', 'Unknown')[:50]}...")
                return False
        except Exception as e:
            console.print(f"❌ 기사 삽입 오류: {str(e)}")
            return False


# 전역 인스턴스 (지연 초기화) - 싱글톤 패턴
_supabase_manager = None

def get_supabase_client():
    """Supabase 클라이언트 인스턴스를 반환 (싱글톤 패턴)"""
    global _supabase_manager
    if _supabase_manager is None:
        _supabase_manager = SupabaseManager()
    return _supabase_manager
