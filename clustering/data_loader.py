#!/usr/bin/env python3
"""
데이터 로더 클래스 - KISS 원칙 적용
임베딩, 기사, 언론사 데이터를 로드하는 단일 책임
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pytz
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn

from utils.supabase_manager import get_supabase_client

console = Console()

def get_kct_to_utc_range(date_filter):
    """KCT 기준 날짜 필터를 UTC 기준으로 변환
    
    Args:
        date_filter: 'yesterday', 'today', None
        
    Returns:
        tuple: (start_utc, end_utc) 또는 None
    """
    if not date_filter:
        return None
    
    # 시간대 설정
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
        
    elif date_filter == 'today':
        # KCT 기준 오늘 00:00-현재
        kct_today = datetime.now(kct).replace(hour=0, minute=0, second=0, microsecond=0)
        kct_start = kct_today
        kct_end = datetime.now(kct)
        
        # UTC로 변환
        utc_start = kct_start.astimezone(utc)
        utc_end = kct_end.astimezone(utc)
    
    else:
        return None
    
    return utc_start, utc_end

class DataLoader:
    """데이터 로더 클래스 - 단일 책임: 데이터 로드"""
    
    def __init__(self, date_filter=None):
        """초기화
        
        Args:
            date_filter: 날짜 필터 옵션
                - None: 전체 기사
                - 'yesterday': 전날 기사만 (KCT 기준 00:00-23:59)
                - 'today': 오늘 기사만
        """
        self.supabase = get_supabase_client()
        self.embeddings_data = None
        self.articles_data = None
        self.media_outlets = None
        self.embeddings = None
        self.date_filter = date_filter
        
    def load_embeddings(self) -> bool:
        """임베딩 데이터 로드"""
        try:
            console.print("📊 임베딩 데이터 로드 중...")
            
            all_embeddings = []
            offset = 0
            batch_size = 100
            
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                console=console
            ) as progress:
                
                task = progress.add_task("임베딩 데이터 로드 중...", total=None)
                
                while True:
                    result = self.supabase.client.table('articles_embeddings').select(
                        'cleaned_article_id, embedding_vector, model_name'
                    ).eq('embedding_type', 'combined').range(offset, offset + batch_size - 1).execute()
                    
                    if not result.data:
                        break
                    
                    all_embeddings.extend(result.data)
                    progress.update(task, description=f"임베딩 데이터 로드 중... ({len(all_embeddings)}개)")
                    
                    if len(result.data) < batch_size:
                        break
                    
                    offset += batch_size
            
            if not all_embeddings:
                console.print("❌ 임베딩 데이터가 없습니다.")
                return False
            
            # DataFrame으로 변환
            self.embeddings_data = pd.DataFrame(all_embeddings)
            
            # 임베딩 벡터 추출
            self.embeddings = np.array([eval(emb) for emb in self.embeddings_data['embedding_vector']])
            
            console.print(f"✅ 임베딩 데이터 로드 완료: {len(all_embeddings)}개")
            return True
            
        except Exception as e:
            console.print(f"❌ 임베딩 데이터 로드 실패: {e}")
            return False
    
    def load_articles_data(self) -> bool:
        """기사 데이터 로드"""
        try:
            console.print("📰 기사 데이터 로드 중...")
            
            # 임베딩에 해당하는 기사들만 로드
            embedding_ids = self.embeddings_data['cleaned_article_id'].tolist()
            
            # 날짜 필터링 적용
            query = self.supabase.client.table('articles_cleaned').select(
                'id, title_cleaned, lead_paragraph, media_id, published_at'
            ).in_('id', embedding_ids)
            
            # KCT 기준을 UTC로 변환
            utc_range = get_kct_to_utc_range(self.date_filter)
            
            if utc_range:
                utc_start, utc_end = utc_range
                
                if self.date_filter == 'yesterday':
                    query = query.gte('published_at', utc_start.isoformat()).lte('published_at', utc_end.isoformat())
                    # KCT 시간으로 표시 (사용자 친화적)
                    kct_start = utc_start.astimezone(pytz.timezone('Asia/Seoul'))
                    kct_end = utc_end.astimezone(pytz.timezone('Asia/Seoul'))
                    console.print(f"📅 전날 기사 필터링 (KCT): {kct_start.strftime('%Y-%m-%d %H:%M')} ~ {kct_end.strftime('%Y-%m-%d %H:%M')}")
                    console.print(f"📅 UTC 변환: {utc_start.strftime('%Y-%m-%d %H:%M')} ~ {utc_end.strftime('%Y-%m-%d %H:%M')}")
                    
                elif self.date_filter == 'today':
                    query = query.gte('published_at', utc_start.isoformat()).lte('published_at', utc_end.isoformat())
                    # KCT 시간으로 표시 (사용자 친화적)
                    kct_start = utc_start.astimezone(pytz.timezone('Asia/Seoul'))
                    kct_end = utc_end.astimezone(pytz.timezone('Asia/Seoul'))
                    console.print(f"📅 오늘 기사 필터링 (KCT): {kct_start.strftime('%Y-%m-%d %H:%M')} ~ {kct_end.strftime('%Y-%m-%d %H:%M')}")
                    console.print(f"📅 UTC 변환: {utc_start.strftime('%Y-%m-%d %H:%M')} ~ {utc_end.strftime('%Y-%m-%d %H:%M')}")
            
            result = query.execute()
            
            if not result.data:
                console.print("❌ 기사 데이터가 없습니다.")
                return False
            
            self.articles_data = pd.DataFrame(result.data)
            
            # 날짜 필터링 결과 표시
            if self.date_filter:
                console.print(f"✅ 기사 데이터 로드 완료: {len(result.data)}개 (날짜 필터링 적용)")
            else:
                console.print(f"✅ 기사 데이터 로드 완료: {len(result.data)}개 (전체 기사)")
            
            return True
            
        except Exception as e:
            console.print(f"❌ 기사 데이터 로드 실패: {e}")
            return False
    
    def load_media_outlets(self) -> bool:
        """언론사 데이터 로드"""
        try:
            console.print("📺 언론사 데이터 로드 중...")
            
            result = self.supabase.client.table('media_outlets').select('*').execute()
            
            if not result.data:
                console.print("❌ 언론사 데이터가 없습니다.")
                return False
            
            self.media_outlets = pd.DataFrame(result.data)
            console.print(f"✅ 언론사 데이터 로드 완료: {len(result.data)}개")
            return True
            
        except Exception as e:
            console.print(f"❌ 언론사 데이터 로드 실패: {e}")
            return False
    
    def load_all_data(self) -> bool:
        """모든 데이터 로드"""
        console.print("🔄 모든 데이터 로드 시작...")
        
        if not self.load_embeddings():
            return False
        if not self.load_articles_data():
            return False
        if not self.load_media_outlets():
            return False
        
        console.print("✅ 모든 데이터 로드 완료!")
        return True