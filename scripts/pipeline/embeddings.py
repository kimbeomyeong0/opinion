#!/usr/bin/env python3
"""
기사 임베딩 생성 및 저장 스크립트
- OpenAI text-embedding-3-large 모델 사용
- 리드문단만을 대상으로 임베딩 생성 (편향성 제거)
- articles 테이블의 embedding 컬럼에 저장
- 배치 처리로 효율성 향상
"""

import time
import json
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import warnings
import logging
warnings.filterwarnings('ignore')

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    
    def __init__(self, batch_size: int = 50) -> None:
        """초기화
        
        Args:
            batch_size: 배치 처리 크기 (OpenAI API 제한 고려)
        """
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
        
        self.batch_size = batch_size
        self.openai_client = OpenAI()
        logger.info(f"EmbeddingGenerator 초기화 완료 (배치 크기: {batch_size})")
    
    def fetch_all_articles_without_embeddings(self) -> List[Dict[str, Any]]:
        """임베딩이 없는 모든 기사를 페이지네이션으로 조회"""
        try:
            all_articles = []
            page_size = 1000  # Supabase 기본 제한
            offset = 0
            
            while True:
                result = self.supabase_manager.client.table('articles').select(
                    'id, title, lead_paragraph, political_category'
                ).eq('is_preprocessed', True).is_('embedding', 'null').range(offset, offset + page_size - 1).execute()
                
                if not result.data:
                    break
                    
                all_articles.extend(result.data)
                
                # 마지막 페이지인지 확인
                if len(result.data) < page_size:
                    break
                    
                offset += page_size
                print(f"📄 페이지 조회 중... {len(all_articles)}개 수집됨")
            
            print(f"🔍 조회된 미처리 임베딩 기사 수: {len(all_articles)}개")
            return all_articles
            
        except Exception as e:
            print(f"❌ 미처리 임베딩 기사 조회 실패: {str(e)}")
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
    
    def save_embeddings_to_db_optimized(self, articles: List[Dict[str, Any]], embeddings: np.ndarray) -> bool:
        """최적화된 배치 임베딩 저장"""
        try:
            success_count = 0
            
            for i, article in enumerate(articles):
                if i < len(embeddings):
                    try:
                        # 임베딩을 JSON 문자열로 변환하여 저장
                        embedding_list = embeddings[i].tolist()
                        embedding_json = json.dumps(embedding_list)
                        
                        # 데이터베이스 업데이트
                        result = self.supabase_manager.client.table('articles').update({
                            'embedding': embedding_json
                        }).eq('id', article['id']).execute()
                        
                        if result.data:
                            success_count += 1
                        else:
                            print(f"⚠️ 기사 업데이트 실패: {article.get('id', 'Unknown')}")
                            
                    except Exception as e:
                        print(f"⚠️ 개별 기사 저장 실패: {article.get('id', 'Unknown')} - {str(e)}")
            
            # 성공률 계산
            success_rate = success_count / len(articles) * 100
            if success_rate < 90:  # 90% 미만 성공 시 경고
                print(f"⚠️ 배치 저장 성공률 낮음: {success_rate:.1f}% ({success_count}/{len(articles)})")
                return False
            
            return success_count > 0
            
        except Exception as e:
            print(f"❌ 배치 임베딩 저장 실패: {str(e)}")
            return False
    
    def process_articles_optimized(self) -> bool:
        """Option 1: Direct Query 방식으로 최적화된 임베딩 생성"""
        try:
            print("=" * 60)
            print("🔄 리드문단 기반 임베딩 생성 및 저장 시작 (최적화 버전)")
            print("=" * 60)
            
            # 1단계: 모든 미처리 기사 한 번에 조회
            articles = self.fetch_all_articles_without_embeddings()
            if not articles:
                print("✅ 모든 기사의 임베딩이 이미 생성되어 있습니다.")
                return True
            
            total_articles = len(articles)
            print(f"📦 총 {total_articles:,}개의 기사를 배치로 처리합니다.")
            
            total_processed = 0
            total_failed = 0
            start_time = time.time()
            
            # 2단계: 조회된 모든 기사를 배치로 처리
            for i in range(0, total_articles, self.batch_size):
                # 현재 배치 추출
                batch_articles = articles[i:i + self.batch_size]
                batch_num = (i // self.batch_size) + 1
                total_batches = (total_articles + self.batch_size - 1) // self.batch_size
                
                print(f"📦 배치 {batch_num}/{total_batches} 처리 중... ({len(batch_articles)}개 기사)")
                
                # 텍스트 준비 (리드문단만 사용)
                texts = []
                for article in batch_articles:
                    # 리드문단만 사용하여 편향성 제거
                    text = article.get('lead_paragraph', '')
                    if not text.strip():
                        # 리드문단이 없는 경우 제목 사용 (fallback)
                        text = article.get('title', '')
                    texts.append(text)
                
                # 임베딩 생성
                batch_start = time.time()
                embeddings = self.generate_embeddings(texts)
                
                if len(embeddings) == 0:
                    print(f"❌ 배치 {batch_num} 임베딩 생성 실패")
                    total_failed += len(batch_articles)
                    continue
                
                # 데이터베이스에 저장 (최적화된 배치 저장)
                success = self.save_embeddings_to_db_optimized(batch_articles, embeddings)
                
                if success:
                    total_processed += len(batch_articles)
                    batch_time = time.time() - batch_start
                    print(f"✅ 배치 {batch_num} 완료: {len(batch_articles)}개 기사 ({batch_time:.1f}초)")
                else:
                    print(f"❌ 배치 {batch_num} 저장 실패")
                    total_failed += len(batch_articles)
                
                # 진행률 표시
                progress = min(100, (i + len(batch_articles)) / total_articles * 100)
                elapsed_time = time.time() - start_time
                rate = total_processed / elapsed_time if elapsed_time > 0 else 0
                eta = (total_articles - total_processed) / rate if rate > 0 else 0
                
                print(f"🚀 진행률: {progress:.1f}% | 성공: {total_processed:,}개 | 실패: {total_failed:,}개 | 속도: {rate:.1f}개/초 | 남은시간: {eta/60:.1f}분")
                
                # 배치 간 짧은 대기 (API 제한 방지)
                time.sleep(0.1)
            
            # 최종 결과
            total_time = time.time() - start_time
            print(f"\n🎉 임베딩 생성 완료!")
            print(f"✅ 성공: {total_processed:,}개 | ❌ 실패: {total_failed:,}개")
            print(f"⏱️ 소요시간: {total_time/60:.1f}분 | 📈 속도: {total_processed/total_time:.1f}개/초")
            
            return total_processed > 0
            
        except Exception as e:
            print(f"❌ 임베딩 처리 실패: {str(e)}")
            return False


def main():
    """메인 함수"""
    try:
        # 임베딩 생성기 초기화 (배치 크기 통일)
        batch_size = 50  # OpenAI API 제한을 고려한 적절한 크기
        generator = EmbeddingGenerator(batch_size=batch_size)
        
        print(f"⚙️ 설정: 배치 크기 {batch_size}개")
        print(f"🤖 모델: OpenAI text-embedding-3-large")
        print(f"📝 대상: 리드문단만 사용 (편향성 제거)")
        
        # 최적화된 방식으로 전체 기사 처리
        success = generator.process_articles_optimized()
        
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
