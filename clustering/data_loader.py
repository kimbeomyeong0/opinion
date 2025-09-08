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
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn

from utils.supabase_manager import get_supabase_client

console = Console()

class DataLoader:
    """데이터 로더 클래스 - 단일 책임: 데이터 로드"""
    
    def __init__(self):
        """초기화"""
        self.supabase = get_supabase_client()
        self.embeddings_data = None
        self.articles_data = None
        self.media_outlets = None
        self.embeddings = None
        
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
            
            result = self.supabase.client.table('articles_cleaned').select(
                'id, title_cleaned, lead_paragraph, media_id'
            ).in_('id', embedding_ids).execute()
            
            if not result.data:
                console.print("❌ 기사 데이터가 없습니다.")
                return False
            
            self.articles_data = pd.DataFrame(result.data)
            console.print(f"✅ 기사 데이터 로드 완료: {len(result.data)}개")
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
