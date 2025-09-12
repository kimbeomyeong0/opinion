#!/usr/bin/env python3
"""
임베딩 생성 스크립트
- articles_cleaned 테이블의 merged_content를 임베딩
- OpenAI text-embedding-3-large 모델 사용
- articles_embeddings 테이블에 저장
"""

import sys
import os
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
import openai
from openai import OpenAI

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utils.supabase_manager import SupabaseManager

class EmbeddingProcessor:
    """임베딩 처리 클래스"""
    
    def __init__(self):
        """초기화"""
        # 설정 (하드코딩)
        self.MAX_LENGTH = 4000  # 최대 텍스트 길이
        self.BATCH_SIZE = 100   # 배치 크기
        self.MODEL_NAME = "text-embedding-3-small"  # OpenAI 모델명 (1536차원)
        
        # Supabase 연결
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
        
        # OpenAI 클라이언트 초기화
        self.openai_client = OpenAI()
    
    def clear_embeddings_table(self) -> bool:
        """
        articles_embeddings 테이블 초기화
        
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            print("🗑️ articles_embeddings 테이블 초기화 중...")
            
            # 테이블의 모든 데이터 삭제
            result = self.supabase_manager.client.table('articles_embeddings').delete().neq('id', '00000000-0000-0000-0000-000000000000').execute()
            
            print("✅ articles_embeddings 테이블 초기화 완료")
            return True
            
        except Exception as e:
            print(f"❌ 테이블 초기화 실패: {str(e)}")
            return False
    
    def fetch_articles(self) -> List[Dict[str, Any]]:
        """
        articles_cleaned 테이블에서 기사 조회
        
        Returns:
            List[Dict]: 기사 리스트
        """
        try:
            print("📡 articles_cleaned에서 기사 조회 중...")
            
            result = self.supabase_manager.client.table('articles_cleaned').select(
                'id, article_id, media_id, merged_content, published_at'
            ).execute()
            
            articles = result.data if result.data else []
            print(f"✅ {len(articles)}개 기사 조회 완료")
            
            return articles
            
        except Exception as e:
            print(f"❌ 기사 조회 실패: {str(e)}")
            return []
    
    def preprocess_text(self, text: str) -> Optional[str]:
        """
        텍스트 전처리 (길이 제한)
        
        Args:
            text: 원본 텍스트
            
        Returns:
            str: 전처리된 텍스트 또는 None
        """
        if not text or not text.strip():
            return None
        
        # 텍스트 정리
        cleaned_text = text.strip()
        
        # 길이 제한
        if len(cleaned_text) > self.MAX_LENGTH:
            cleaned_text = cleaned_text[:self.MAX_LENGTH]
            print(f"⚠️ 텍스트 길이 제한: {len(text)}자 → {self.MAX_LENGTH}자")
        
        # 최소 길이 확인
        if len(cleaned_text) < 10:
            return None
        
        return cleaned_text
    
    def create_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """
        배치로 임베딩 생성
        
        Args:
            texts: 텍스트 리스트
            
        Returns:
            List[Optional[List[float]]]: 임베딩 벡터 리스트
        """
        try:
            # OpenAI API 호출
            response = self.openai_client.embeddings.create(
                model=self.MODEL_NAME,
                input=texts
            )
            
            # 임베딩 벡터 추출
            embeddings = []
            for item in response.data:
                embeddings.append(item.embedding)
            
            return embeddings
            
        except Exception as e:
            print(f"❌ 임베딩 생성 실패: {str(e)}")
            return [None] * len(texts)
    
    def save_embeddings_batch(self, embeddings_data: List[Dict[str, Any]]) -> bool:
        """
        배치로 임베딩 저장
        
        Args:
            embeddings_data: 저장할 임베딩 데이터 리스트
            
        Returns:
            bool: 저장 성공 여부
        """
        if not embeddings_data:
            return True
        
        try:
            result = self.supabase_manager.client.table('articles_embeddings').insert(embeddings_data).execute()
            
            if result.data:
                return True
            else:
                print("❌ 임베딩 저장 실패")
                return False
                
        except Exception as e:
            print(f"❌ 저장 실패: {str(e)}")
            return False
    
    def process_embeddings(self) -> bool:
        """
        임베딩 처리 메인 프로세스
        
        Returns:
            bool: 처리 성공 여부
        """
        try:
            print("🚀 임베딩 처리 시작...")
            
            # 1. 테이블 초기화
            if not self.clear_embeddings_table():
                return False
            
            # 2. 기사 조회
            articles = self.fetch_articles()
            if not articles:
                print("📝 처리할 기사가 없습니다.")
                return True
            
            # 3. 배치 처리
            total_articles = len(articles)
            processed_count = 0
            success_count = 0
            failed_count = 0
            
            print(f"🔧 {total_articles}개 기사를 {self.BATCH_SIZE}개씩 처리 중...")
            
            for i in range(0, total_articles, self.BATCH_SIZE):
                # 배치 추출
                batch_articles = articles[i:i + self.BATCH_SIZE]
                batch_texts = []
                batch_metadata = []
                
                # 텍스트 전처리
                for article in batch_articles:
                    processed_text = self.preprocess_text(article['merged_content'])
                    
                    if processed_text:
                        batch_texts.append(processed_text)
                        batch_metadata.append(article)
                    else:
                        failed_count += 1
                
                if not batch_texts:
                    processed_count += len(batch_articles)
                    continue
                
                # 임베딩 생성
                embeddings = self.create_embeddings_batch(batch_texts)
                
                # 저장 데이터 준비
                embeddings_data = []
                for j, (article, embedding) in enumerate(zip(batch_metadata, embeddings)):
                    if embedding:
                        embeddings_data.append({
                            'cleaned_article_id': article['id'],
                            'article_id': article['article_id'],
                            'media_id': article['media_id'],
                            'embedding_vector': embedding,
                            'model_name': self.MODEL_NAME
                        })
                        success_count += 1
                    else:
                        failed_count += 1
                
                # 저장
                if embeddings_data:
                    if self.save_embeddings_batch(embeddings_data):
                        success_count += len(embeddings_data)
                    else:
                        failed_count += len(embeddings_data)
                
                processed_count += len(batch_articles)
                
                # 진행 상황 출력
                progress = (processed_count / total_articles) * 100
                print(f"처리 중: {processed_count}/{total_articles} ({progress:.1f}%)")
            
            print(f"\n📊 임베딩 처리 완료:")
            print(f"  총 기사: {total_articles}개")
            print(f"  성공: {success_count}개")
            print(f"  실패: {failed_count}개")
            print(f"  성공률: {(success_count / total_articles * 100):.1f}%")
            
            return True
            
        except Exception as e:
            print(f"❌ 임베딩 처리 실패: {str(e)}")
            return False

def main():
    """메인 함수"""
    print("=" * 60)
    print("🔮 임베딩 생성 스크립트")
    print("=" * 60)
    
    try:
        # 임베딩 처리 실행
        processor = EmbeddingProcessor()
        success = processor.process_embeddings()
        
        if success:
            print("\n✅ 임베딩 생성 완료!")
        else:
            print("\n❌ 임베딩 생성 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()
