#!/usr/bin/env python3
"""
Supabase 데이터베이스 관리 클래스
"""

import os
from datetime import datetime
from typing import Optional, Dict, Any, List
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
    
    # ===== 임베딩 관련 메서드들 =====
    
    def get_articles_for_embedding(self, offset: int = 0, limit: int = 1000, date_filter: str = None) -> List[Dict[str, Any]]:
        """
        임베딩을 생성할 기사 데이터 조회 (페이지네이션 적용)
        
        Args:
            offset: 시작 오프셋
            limit: 조회할 최대 개수 (Supabase 제한: 1000)
            date_filter: 날짜 필터 ('yesterday', 'today', None)
            
        Returns:
            기사 데이터 리스트
        """
        if not self.client:
            return []
        
        try:
            # merged_content가 있는 기사만 조회
            query = self.client.table('articles_cleaned')\
                .select('id, article_id, merged_content, title_cleaned, lead_paragraph')\
                .not_.is_('merged_content', 'null')\
                .neq('merged_content', '')\
                .order('created_at', desc=True)
            
            # 날짜 필터링 적용
            if date_filter:
                from datetime import datetime, timedelta
                import pytz
                
                kct = pytz.timezone('Asia/Seoul')
                utc = pytz.UTC
                
                if date_filter == 'yesterday':
                    # KCT 기준 전날 00:00-23:59
                    kct_yesterday = datetime.now(kct).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
                    kct_start = kct_yesterday
                    kct_end = kct_yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
                    
                    # UTC로 변환
                    utc_start = kct_start.astimezone(utc)
                    utc_end = kct_end.astimezone(utc)
                    
                    query = query.gte('created_at', utc_start.isoformat()).lte('created_at', utc_end.isoformat())
                    
                elif date_filter == 'today':
                    # KCT 기준 오늘 00:00-현재
                    kct_today = datetime.now(kct).replace(hour=0, minute=0, second=0, microsecond=0)
                    kct_start = kct_today
                    kct_end = datetime.now(kct)
                    
                    # UTC로 변환
                    utc_start = kct_start.astimezone(utc)
                    utc_end = kct_end.astimezone(utc)
                    
                    query = query.gte('created_at', utc_start.isoformat()).lte('created_at', utc_end.isoformat())
            
            result = query.range(offset, offset + limit - 1).execute()
            
            console.print(f"✅ 임베딩 대상 기사 {len(result.data)}개 조회 완료 (offset: {offset}, limit: {limit})")
            return result.data
            
        except Exception as e:
            console.print(f"❌ 기사 데이터 조회 실패: {str(e)}")
            return []
    
    def get_all_articles_for_embedding(self, batch_size: int = 1000) -> List[Dict[str, Any]]:
        """
        모든 기사 데이터를 페이지네이션으로 조회
        
        Args:
            batch_size: 배치 크기 (Supabase 제한 고려)
            
        Returns:
            전체 기사 데이터 리스트
        """
        if not self.client:
            return []
        
        try:
            all_articles = []
            offset = 0
            
            console.print("📊 전체 기사 데이터를 페이지네이션으로 조회 중...")
            
            while True:
                batch_articles = self.get_articles_for_embedding(offset, batch_size)
                
                if not batch_articles:
                    break
                
                all_articles.extend(batch_articles)
                console.print(f"   - 배치 {offset//batch_size + 1}: {len(batch_articles)}개 기사 추가 (총 {len(all_articles)}개)")
                
                # Supabase 제한에 도달했으면 중단
                if len(batch_articles) < batch_size:
                    break
                
                offset += batch_size
            
            console.print(f"✅ 전체 기사 데이터 조회 완료: {len(all_articles)}개")
            return all_articles
            
        except Exception as e:
            console.print(f"❌ 전체 기사 데이터 조회 실패: {str(e)}")
            return []
    
    def get_total_articles_count(self, date_filter: str = None) -> int:
        """임베딩 가능한 전체 기사 수 조회"""
        if not self.client:
            return 0
        
        try:
            query = self.client.table('articles_cleaned')\
                .select('id', count='exact')\
                .not_.is_('merged_content', 'null')\
                .neq('merged_content', '')
            
            # 날짜 필터링 적용
            if date_filter:
                from datetime import datetime, timedelta
                import pytz
                
                kct = pytz.timezone('Asia/Seoul')
                utc = pytz.UTC
                
                if date_filter == 'yesterday':
                    # KCT 기준 전날 00:00-23:59
                    kct_yesterday = datetime.now(kct).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
                    kct_start = kct_yesterday
                    kct_end = kct_yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
                    
                    # UTC로 변환
                    utc_start = kct_start.astimezone(utc)
                    utc_end = kct_end.astimezone(utc)
                    
                    query = query.gte('created_at', utc_start.isoformat()).lte('created_at', utc_end.isoformat())
                    
                elif date_filter == 'today':
                    # KCT 기준 오늘 00:00-현재
                    kct_today = datetime.now(kct).replace(hour=0, minute=0, second=0, microsecond=0)
                    kct_start = kct_today
                    kct_end = datetime.now(kct)
                    
                    # UTC로 변환
                    utc_start = kct_start.astimezone(utc)
                    utc_end = kct_end.astimezone(utc)
                    
                    query = query.gte('created_at', utc_start.isoformat()).lte('created_at', utc_end.isoformat())
            
            result = query.execute()
            
            count = result.count if result.count else 0
            console.print(f"📊 임베딩 가능한 전체 기사 수: {count:,}개")
            return count
            
        except Exception as e:
            console.print(f"❌ 기사 수 조회 실패: {str(e)}")
            return 0
    
    def check_existing_embeddings(self, cleaned_article_ids: List[str], embedding_type: str = "clustering") -> List[str]:
        """
        이미 임베딩이 존재하는 기사 ID 조회
        
        Args:
            cleaned_article_ids: 확인할 기사 ID 리스트
            embedding_type: 임베딩 타입
            
        Returns:
            이미 임베딩이 존재하는 기사 ID 리스트
        """
        if not self.client:
            return []
        
        try:
            result = self.client.table('articles_embeddings')\
                .select('cleaned_article_id')\
                .in_('cleaned_article_id', cleaned_article_ids)\
                .eq('embedding_type', embedding_type)\
                .execute()
            
            existing_ids = [row['cleaned_article_id'] for row in result.data]
            console.print(f"📊 이미 임베딩이 존재하는 기사: {len(existing_ids)}개")
            return existing_ids
            
        except Exception as e:
            console.print(f"❌ 기존 임베딩 확인 실패: {str(e)}")
            return []
    
    def save_embeddings(self, embeddings_data: List[Dict[str, Any]]) -> bool:
        """
        임베딩 데이터를 데이터베이스에 저장
        
        Args:
            embeddings_data: 저장할 임베딩 데이터 리스트
            
        Returns:
            저장 성공 여부
        """
        if not self.client:
            return False
        
        if not embeddings_data:
            console.print("⚠️ 저장할 임베딩 데이터가 없습니다.")
            return True
        
        try:
            result = self.client.table('articles_embeddings')\
                .insert(embeddings_data)\
                .execute()
            
            if result.data:
                console.print(f"✅ 임베딩 {len(embeddings_data)}개 저장 완료")
                return True
            else:
                console.print("❌ 임베딩 저장 실패")
                return False
                
        except Exception as e:
            console.print(f"❌ 임베딩 저장 중 오류: {str(e)}")
            return False
    
    def create_embedding_record(self, 
                              cleaned_article_id: str, 
                              embedding_vector: List[float],
                              embedding_type: str = "combined",
                              model_name: str = None,
                              model_version: str = "1.0") -> Dict[str, Any]:
        """
        임베딩 레코드 생성
        
        Args:
            cleaned_article_id: 기사 ID
            embedding_vector: 임베딩 벡터
            embedding_type: 임베딩 타입
            model_name: 모델 이름 (None이면 config에서 가져옴)
            model_version: 모델 버전
            
        Returns:
            임베딩 레코드 딕셔너리
        """
        # config에서 모델 설정 가져오기
        if model_name is None:
            import sys
            import os
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from embeddings.config import get_config
            config = get_config()
            model_name = config["embedding_model"]
        
        return {
            'cleaned_article_id': cleaned_article_id,
            'embedding_type': embedding_type,
            'embedding_vector': embedding_vector,
            'model_name': model_name,
            'model_version': model_version,
            'created_at': datetime.now().isoformat()
        }
    
    def get_embedding_statistics(self) -> Dict[str, Any]:
        """임베딩 통계 정보 조회"""
        if not self.client:
            return {
                'total_embeddings': 0,
                'clustering_embeddings': 0,
                'today_embeddings': 0
            }
        
        try:
            # 전체 임베딩 수
            total_result = self.client.table('articles_embeddings')\
                .select('id', count='exact')\
                .execute()
            
            # 클러스터링용 임베딩 수
            clustering_result = self.client.table('articles_embeddings')\
                .select('id', count='exact')\
                .eq('embedding_type', 'combined')\
                .execute()
            
            # 최근 생성된 임베딩 수 (오늘)
            today = datetime.now().strftime('%Y-%m-%d')
            today_result = self.client.table('articles_embeddings')\
                .select('id', count='exact')\
                .gte('created_at', f'{today} 00:00:00')\
                .execute()
            
            return {
                'total_embeddings': total_result.count if total_result.count else 0,
                'clustering_embeddings': clustering_result.count if clustering_result.count else 0,
                'today_embeddings': today_result.count if today_result.count else 0
            }
            
        except Exception as e:
            console.print(f"❌ 임베딩 통계 조회 실패: {str(e)}")
            return {
                'total_embeddings': 0,
                'clustering_embeddings': 0,
                'today_embeddings': 0
            }


# 전역 인스턴스 (지연 초기화) - 싱글톤 패턴
_supabase_manager = None

def get_supabase_client():
    """Supabase 클라이언트 인스턴스를 반환 (싱글톤 패턴)"""
    global _supabase_manager
    if _supabase_manager is None:
        _supabase_manager = SupabaseManager()
    return _supabase_manager
