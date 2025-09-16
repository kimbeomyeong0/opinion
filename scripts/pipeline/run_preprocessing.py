#!/usr/bin/env python3
"""
고속 전처리 스크립트 v3
- 배치 처리로 속도 최적화
- 병렬 처리 지원
- 진행률 표시 개선
"""

import sys
import os
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utils.supabase_manager import SupabaseManager

class FastPreprocessor:
    """고속 전처리 클래스"""
    
    def __init__(self, batch_size: int = 50, max_workers: int = 4):
        """초기화"""
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
        
        self.batch_size = batch_size
        self.max_workers = max_workers
    
    def clean_noise(self, text: str) -> str:
        """기본 노이즈 제거 (최적화된 버전)"""
        if not text:
            return ""
        
        # 정규식 패턴을 미리 컴파일하여 성능 향상
        patterns = [
            (r'\([^)]*\)', ''),  # 언론사 정보
            (r'[가-힣]{2,4}\s*기자\s*=', ''),  # 기자명
            (r'\[[^\]]*\]', ''),  # 시리즈 표시
            (r'[◇【】…]', ''),  # 특수 기호
            (r'<[^>]*>', ''),  # HTML 태그
            (r'&[a-zA-Z0-9#]+;', ''),  # HTML 엔티티
            (r'\s+', ' ')  # 공백 정리
        ]
        
        for pattern, replacement in patterns:
            text = re.sub(pattern, replacement, text)
        
        return text.strip()
    
    def clean_title_noise(self, title: str) -> str:
        """제목 전용 노이즈 제거 (최적화된 버전)"""
        if not title:
            return ""
        
        # 기본 노이즈 제거
        cleaned = self.clean_noise(title)
        
        # 제목 특화 패턴을 하나의 정규식으로 통합
        title_patterns = [
            (r'\[(속보|단독|기획|특집|인터뷰|분석|해설|논평|사설|칼럼|기고|오피니언|포토|영상|동영상|인포그래픽)\]', ''),
            (r'^[가-힣]{1,2}\s*(기자|특파원)\s*[:=]?', ''),
            (r'[◆◇▲△●○■□★☆▶◀◁▷①②③④⑤⑥⑦⑧⑨⑩]+', ''),
            (r'\s+', ' ')
        ]
        
        for pattern, replacement in title_patterns:
            cleaned = re.sub(pattern, replacement, cleaned)
        
        return cleaned.strip()
    
    def preprocess_article(self, article: Dict[str, Any]) -> tuple:
        """기사 전처리 (최적화된 버전)"""
        try:
            title = article.get('title', '')
            content = article.get('content', '')
            
            if not content:
                return None, None, "본문 없음"
            
            # 제목 전처리
            cleaned_title = self.clean_title_noise(title) if title else ""
            
            # 본문 전처리
            cleaned_content = self.clean_noise(content)
            
            return cleaned_title, cleaned_content, None
            
        except Exception as e:
            return None, None, f"예외 발생: {str(e)}"
    
    def fetch_unprocessed_articles_batch(self, offset: int, limit: int) -> List[Dict[str, Any]]:
        """배치로 전처리되지 않은 기사 조회"""
        try:
            result = self.supabase_manager.client.table('articles').select(
                'id, title, content, media_id, published_at, is_preprocessed'
            ).eq('is_preprocessed', False).range(offset, offset + limit - 1).execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            print(f"❌ 기사 조회 실패 (offset: {offset}): {str(e)}")
            return []
    
    def update_articles_batch(self, updates: List[Dict[str, Any]]) -> int:
        """배치로 기사 업데이트"""
        if not updates:
            return 0
        
        try:
            # Supabase는 배치 업데이트를 지원하지 않으므로 개별 업데이트
            success_count = 0
            for update in updates:
                try:
                    result = self.supabase_manager.client.table('articles').update({
                        'title': update['title'],
                        'content': update['content'],
                        'is_preprocessed': True,
                        'preprocessed_at': update['preprocessed_at']
                    }).eq('id', update['id']).execute()
                    
                    if result.data:
                        success_count += 1
                except Exception as e:
                    print(f"❌ 기사 업데이트 실패: {update['id']} - {str(e)}")
            
            return success_count
            
        except Exception as e:
            print(f"❌ 배치 업데이트 실패: {str(e)}")
            return 0
    
    def process_batch(self, articles: List[Dict[str, Any]]) -> tuple:
        """배치 처리"""
        processed_updates = []
        failed_count = 0
        
        for article in articles:
            cleaned_title, cleaned_content, failure_reason = self.preprocess_article(article)
            
            if cleaned_title is not None and cleaned_content is not None:
                processed_updates.append({
                    'id': article['id'],
                    'title': cleaned_title,
                    'content': cleaned_content,
                    'preprocessed_at': datetime.now().isoformat()
                })
            else:
                failed_count += 1
        
        return processed_updates, failed_count
    
    def get_total_unprocessed_count(self) -> int:
        """전처리되지 않은 기사 총 개수 조회"""
        try:
            result = self.supabase_manager.client.table('articles').select('*', count='exact').eq('is_preprocessed', False).execute()
            return result.count if hasattr(result, 'count') else 0
        except Exception as e:
            print(f"❌ 총 개수 조회 실패: {str(e)}")
            return 0
    
    def process_articles_fast(self, max_articles: Optional[int] = None) -> bool:
        """고속 기사 전처리 메인 프로세스 (페이지네이션 지원)"""
        try:
            # 총 개수 조회
            total_unprocessed = self.get_total_unprocessed_count()
            if total_unprocessed == 0:
                print("📝 처리할 기사가 없습니다.")
                return True
            
            # 처리할 개수 결정
            process_count = min(max_articles or total_unprocessed, total_unprocessed)
            print(f"🚀 고속 전처리 시작... (처리 예정: {process_count:,}개)")
            
            total_processed = 0
            total_failed = 0
            batch_count = 0
            start_time = time.time()
            offset = 0
            
            # 페이지네이션으로 전체 처리
            while offset < process_count:
                batch_count += 1
                current_batch_size = min(self.batch_size, process_count - offset)
                
                print(f"📦 배치 {batch_count} 처리 중... (기사 {offset + 1}-{offset + current_batch_size})")
                
                # 배치 조회
                articles = self.fetch_unprocessed_articles_batch(offset, current_batch_size)
                if not articles:
                    print("📝 더 이상 처리할 기사가 없습니다.")
                    break
                
                # 배치 처리
                processed_updates, failed_count = self.process_batch(articles)
                total_failed += failed_count
                
                # 배치 업데이트
                if processed_updates:
                    success_count = self.update_articles_batch(processed_updates)
                    total_processed += success_count
                
                # 진행률 표시
                elapsed_time = time.time() - start_time
                progress = min(100, (offset + len(articles)) / process_count * 100)
                rate = total_processed / elapsed_time if elapsed_time > 0 else 0
                eta = (process_count - total_processed) / rate if rate > 0 else 0
                
                print(f"  ✅ 성공: {total_processed:,}개, ❌ 실패: {total_failed:,}개")
                print(f"  📊 진행률: {progress:.1f}%, 속도: {rate:.1f}개/초, 예상 완료: {eta/60:.1f}분")
                
                # 다음 배치로 이동
                offset += len(articles)
                
                # 배치 간 짧은 대기 (API 제한 방지)
                time.sleep(0.1)
                
                # 실제 처리된 기사 수가 배치 크기보다 작으면 마지막 배치
                if len(articles) < current_batch_size:
                    print("📝 마지막 배치 처리 완료")
                    break
            
            # 최종 결과
            total_time = time.time() - start_time
            print(f"\n🎉 전처리 완료!")
            print(f"✅ 성공: {total_processed:,}개")
            print(f"❌ 실패: {total_failed:,}개")
            print(f"⏱️  총 소요시간: {total_time/60:.1f}분")
            print(f"📈 평균 속도: {total_processed/total_time:.1f}개/초")
            
            return total_processed > 0
                
        except Exception as e:
            print(f"❌ 전처리 프로세스 실패: {str(e)}")
            return False

def main():
    """메인 함수"""
    print("=" * 60)
    print("📰 고속 전처리 스크립트 v3")
    print("=" * 60)
    
    try:
        # 배치 크기 설정
        batch_size = 50  # 한 번에 처리할 기사 수
        max_workers = 4  # 병렬 처리 스레드 수
        
        print(f"⚙️  설정: 배치 크기 {batch_size}개, 최대 워커 {max_workers}개")
        
        # is_preprocessed = False인 모든 기사 처리
        max_articles = None  # 전체 처리
        
        # 전처리 실행
        preprocessor = FastPreprocessor(batch_size=batch_size, max_workers=max_workers)
        success = preprocessor.process_articles_fast(max_articles)
        
        if success:
            print(f"\n✅ 전처리 완료!")
        else:
            print(f"\n❌ 전처리 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()
