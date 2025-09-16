#!/usr/bin/env python3
"""
기사 임베딩 생성 및 저장 스크립트
- OpenAI text-embedding-3-large 모델 사용
- articles 테이블의 embedding 컬럼에 저장
- 배치 처리로 효율성 향상
"""

import time
import numpy as np
from typing import List, Dict, Any
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# 필요한 라이브러리 import
try:
    from openai import OpenAI
except ImportError:
    print("❌ OpenAI 라이브러리가 설치되지 않았습니다.")
    print("pip install openai")
    exit(1)

from utils.supabase_manager import SupabaseManager


class EmbeddingGenerator:
    """임베딩 생성 및 저장 클래스"""
    
    def __init__(self, batch_size: int = 100):
        """초기화"""
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
        
        self.batch_size = batch_size
        self.openai_client = OpenAI()
    
    def fetch_articles_without_embeddings(self, limit: int = None) -> List[Dict[str, Any]]:
        """임베딩이 없는 기사들 조회"""
        try:
            query = self.supabase_manager.client.table('articles').select(
                'id, title, lead_paragraph, political_category'
            ).eq('is_preprocessed', True).is_('embedding', 'null')
            
            if limit:
                query = query.limit(limit)
            
            result = query.execute()
            return result.data
            
        except Exception as e:
            print(f"❌ 기사 조회 실패: {str(e)}")
            return []
    
    def generate_embeddings(self, texts: List[str]) -> np.ndarray:
        """OpenAI 임베딩 생성"""
        try:
            embeddings = []
            
            # 배치로 임베딩 생성
            for i in range(0, len(texts), self.batch_size):
                batch_texts = texts[i:i + self.batch_size]
                
                response = self.openai_client.embeddings.create(
                    model="text-embedding-3-large",
                    input=batch_texts
                )
                
                batch_embeddings = [data.embedding for data in response.data]
                embeddings.extend(batch_embeddings)
                
                # API 제한 방지
                time.sleep(0.1)
            
            return np.array(embeddings)
            
        except Exception as e:
            print(f"❌ 임베딩 생성 실패: {str(e)}")
            return np.array([])
    
    def save_embeddings_to_db(self, articles: List[Dict[str, Any]], embeddings: np.ndarray) -> bool:
        """임베딩을 데이터베이스에 저장"""
        try:
            for i, article in enumerate(articles):
                if i < len(embeddings):
                    # 임베딩을 리스트로 변환 (Supabase vector 타입용)
                    embedding_list = embeddings[i].tolist()
                    
                    # 데이터베이스 업데이트
                    self.supabase_manager.client.table('articles').update({
                        'embedding': embedding_list
                    }).eq('id', article['id']).execute()
            
            return True
            
        except Exception as e:
            print(f"❌ 임베딩 저장 실패: {str(e)}")
            return False
    
    def process_articles(self, limit: int = None) -> bool:
        """기사 임베딩 생성 및 저장"""
        try:
            print("=" * 60)
            print("🔄 임베딩 생성 및 저장 시작")
            print("=" * 60)
            
            # 임베딩이 없는 기사들 조회
            articles = self.fetch_articles_without_embeddings(limit)
            
            if not articles:
                print("✅ 모든 기사의 임베딩이 이미 생성되어 있습니다.")
                return True
            
            print(f"📰 임베딩 생성 대상: {len(articles)}개 기사")
            
            # 텍스트 준비 (제목 + 리드문단)
            texts = []
            for article in articles:
                text = f"{article['title']} {article.get('lead_paragraph', '')}"
                texts.append(text)
            
            # 임베딩 생성
            print("🔄 임베딩 생성 중...")
            start_time = time.time()
            embeddings = self.generate_embeddings(texts)
            
            if len(embeddings) == 0:
                print("❌ 임베딩 생성 실패")
                return False
            
            generation_time = time.time() - start_time
            print(f"✅ 임베딩 생성 완료: {generation_time:.1f}초")
            
            # 데이터베이스에 저장
            print("💾 임베딩 저장 중...")
            save_start = time.time()
            success = self.save_embeddings_to_db(articles, embeddings)
            save_time = time.time() - save_start
            
            if success:
                print(f"✅ 임베딩 저장 완료: {save_time:.1f}초")
                print(f"🎉 총 {len(articles)}개 기사 임베딩 처리 완료!")
                return True
            else:
                print("❌ 임베딩 저장 실패")
                return False
            
        except Exception as e:
            print(f"❌ 임베딩 처리 실패: {str(e)}")
            return False


def main():
    """메인 함수"""
    try:
        # 임베딩 생성기 초기화
        generator = EmbeddingGenerator(batch_size=50)
        
        # 전체 기사 처리 (limit 없음)
        success = generator.process_articles()
        
        if success:
            print(f"\n✅ 임베딩 생성 및 저장 완료!")
        else:
            print(f"\n❌ 임베딩 생성 및 저장 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")


if __name__ == "__main__":
    main()
