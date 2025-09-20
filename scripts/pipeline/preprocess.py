#!/usr/bin/env python3
"""
고속 전처리 스크립트 v3 (키워드 기반 분류)
- 배치 처리로 속도 최적화
- 키워드 기반 정치 카테고리 분류 (LLM 없음)
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

# 정치 카테고리 정의
POLITICAL_CATEGORIES = {
    "국회/정당": ["국회", "의원", "정당", "여당", "야당", "국정감사", "상임위"],
    "행정부": ["정부", "대통령", "총리", "부처", "장관", "청와대", "국무회의"],
    "선거": ["선거", "투표", "후보", "당선", "득표", "선거구", "공천", "지방선거"],
    "사법/검찰": ["검찰", "법원", "재판", "기소", "수사", "판결", "검사", "특검", "헌재", "탄핵"],
    "정책/경제사회": ["정책", "예산", "법안", "개혁", "경제", "복지", "노동", "사회"],
    "외교/안보": ["외교", "안보", "국방", "북한", "미국", "중국", "일본", "한미", "한일", "군사"],
    "지역정치": ["지역", "시도", "시장", "도지사", "구청장", "지자체", "지방", "도의회", "광역의회"],
    "기타": []  # 명시적인 키워드가 없는 경우
}

class FastPreprocessor:
    """고속 전처리 클래스"""
    
    def __init__(self, batch_size: int = 100, max_workers: int = 4):
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
            (r'^\[.*?\]', ''),  # 맨 앞의 [내용] 제거
            (r'^\(.*?\)', ''),  # 맨 앞의 (내용) 제거
            (r'^<.*?>', ''),    # 맨 앞의 <내용> 제거
            (r'\s+', ' ')
        ]
        
        for pattern, replacement in title_patterns:
            cleaned = re.sub(pattern, replacement, cleaned)
        
        return cleaned.strip()
    
    def extract_lead_paragraph(self, content: str) -> str:
        """기사 본문에서 첫 번째 문장을 리드문으로 추출"""
        if not content:
            return ""
        
        # 본문을 문장 단위로 분리 (마침표 기준)
        sentences = content.split('.')
        if sentences:
            # 첫 번째 문장 추출 (빈 문자열 제외)
            first_sentence = sentences[0].strip()
            if first_sentence:
                # 마침표가 제거되었으므로 다시 추가
                return first_sentence + '.'
        
        # 문장 구분이 없는 경우 첫 100자로 제한
        return content.strip()[:100]
    
    def classify_by_keywords(self, title: str, lead_paragraph: str) -> str:
        """고속 가중치 기반 키워드 분류 (최적화됨)"""
        # 텍스트 전처리 (한 번만)
        title_lower = title.lower()
        lead_lower = lead_paragraph.lower()
        
        # 가중치 설정
        title_weight = 2.0
        content_weight = 1.0
        
        category_scores = {}
        max_score = 0
        best_category = "uncertain"
        
        # 카테고리별 점수 계산 (최적화된 루프)
        for category, keywords in POLITICAL_CATEGORIES.items():
            if not keywords:  # "기타" 카테고리 스킵
                continue
                
            score = 0
            
            # 키워드 검색을 한 번에 처리
            for keyword in keywords:
                if keyword in title_lower:
                    score += title_weight
                if keyword in lead_lower:
                    score += content_weight
            
            category_scores[category] = score
            
            # 최고 점수 추적 (별도 max() 호출 방지)
            if score > max_score:
                max_score = score
                best_category = category
        
        # 임계값 이상이면 분류 결과 반환 (임계값을 낮춰서 더 많은 기사 분류)
        if max_score >= 1.0:
            return best_category
        else:
            return "uncertain"  # 키워드로 분류되지 않은 경우
    
    def classify_by_llm(self, title: str, lead_paragraph: str) -> str:
        """LLM 분류 비활성화 - 키워드 기반 분류만 사용"""
        # 하이브리드 모델에서 LLM 부분을 제거하고 키워드 기반으로만 분류
        return "기타"  # LLM 없이 키워드로 분류되지 않은 경우 기본값
    
    def classify_political_category(self, title: str, lead_paragraph: str) -> str:
        """키워드 기반 정치 카테고리 분류 (LLM 없음)"""
        
        # 키워드 기반 분류만 사용
        keyword_category = self.classify_by_keywords(title, lead_paragraph)
        
        # 키워드로 분류되지 않은 경우 "기타"로 분류
        if keyword_category == "uncertain":
            return "기타"
        
        return keyword_category
    
    def preprocess_article(self, article: Dict[str, Any]) -> tuple:
        """기사 전처리 (키워드 기반 카테고리 분류)"""
        try:
            title = article.get('title', '')
            content = article.get('content', '')
            
            if not content:
                return None, None, None, None, "본문 없음"
            
            # 제목 전처리
            cleaned_title = self.clean_title_noise(title) if title else ""
            
            # 본문 전처리
            cleaned_content = self.clean_noise(content)
            
            # 첫 번째 문장 추출 (리드문)
            lead_paragraph = self.extract_lead_paragraph(cleaned_content)
            
            # 키워드 기반 정치 카테고리 분류
            political_category = self.classify_political_category(cleaned_title, lead_paragraph)
            
            return cleaned_title, cleaned_content, lead_paragraph, political_category, None
            
        except Exception as e:
            return None, None, None, None, f"예외 발생: {str(e)}"
    
    def fetch_all_false_articles(self) -> List[Dict[str, Any]]:
        """is_preprocessed = false인 모든 기사를 페이지네이션으로 조회"""
        try:
            all_articles = []
            page_size = 1000  # Supabase 기본 제한
            offset = 0
            
            while True:
                result = self.supabase_manager.client.table('articles').select(
                    'id, title, content, media_id, published_at, is_preprocessed'
                ).eq('is_preprocessed', False).range(offset, offset + page_size - 1).execute()
                
                if not result.data:
                    break
                    
                all_articles.extend(result.data)
                
                # 마지막 페이지인지 확인
                if len(result.data) < page_size:
                    break
                    
                offset += page_size
                print(f"📄 페이지 조회 중... {len(all_articles)}개 수집됨")
            
            print(f"🔍 조회된 false 기사 수: {len(all_articles)}개")
            return all_articles
            
        except Exception as e:
            print(f"❌ false 기사 조회 실패: {str(e)}")
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
                        'lead_paragraph': update['lead_paragraph'],
                        'political_category': update['political_category'],  # 새로 추가
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
        """배치 처리 (키워드 기반 카테고리 분류)"""
        processed_updates = []
        failed_count = 0
        
        for article in articles:
            cleaned_title, cleaned_content, lead_paragraph, political_category, failure_reason = self.preprocess_article(article)
            
            if cleaned_title is not None and cleaned_content is not None and lead_paragraph is not None:
                processed_updates.append({
                    'id': article['id'],
                    'title': cleaned_title,
                    'content': cleaned_content,
                    'lead_paragraph': lead_paragraph,
                    'political_category': political_category,  # 새로 추가
                    'preprocessed_at': datetime.now().isoformat()
                })
            else:
                failed_count += 1
                if failure_reason:
                    print(f"❌ 기사 처리 실패: {article.get('id', 'Unknown')} - {failure_reason}")
        
        return processed_updates, failed_count
    
    def get_total_unprocessed_count(self) -> int:
        """전처리되지 않은 기사 총 개수 조회"""
        try:
            result = self.supabase_manager.client.table('articles').select('*', count='exact').eq('is_preprocessed', False).execute()
            return result.count if hasattr(result, 'count') else 0
        except Exception as e:
            print(f"❌ 총 개수 조회 실패: {str(e)}")
            return 0
    
    def process_all_false_articles(self) -> bool:
        """Option 1: is_preprocessed = false인 모든 기사를 직접 조회하여 처리"""
        try:
            print("🚀 Direct Query 방식으로 전처리 시작...")
            
            # 1단계: 모든 false 기사 한 번에 조회
            false_articles = self.fetch_all_false_articles()
            if not false_articles:
                print("📝 처리할 false 기사가 없습니다.")
                return True
            
            total_articles = len(false_articles)
            print(f"📦 총 {total_articles:,}개의 false 기사를 배치로 처리합니다.")
            
            total_processed = 0
            total_failed = 0
            start_time = time.time()
            
            # 2단계: 조회된 모든 기사를 배치로 처리
            for i in range(0, total_articles, self.batch_size):
                # 현재 배치 추출
                batch_articles = false_articles[i:i + self.batch_size]
                batch_num = (i // self.batch_size) + 1
                total_batches = (total_articles + self.batch_size - 1) // self.batch_size
                
                print(f"📦 배치 {batch_num}/{total_batches} 처리 중... ({len(batch_articles)}개 기사)")
                
                # 배치 처리
                processed_updates, failed_count = self.process_batch(batch_articles)
                total_failed += failed_count
                
                # 배치 업데이트
                if processed_updates:
                    success_count = self.update_articles_batch(processed_updates)
                    total_processed += success_count
                
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
            print(f"🎉 전처리 완료! ✅ 성공: {total_processed:,}개 | ❌ 실패: {total_failed:,}개 | ⏱️ 소요시간: {total_time/60:.1f}분 | 📈 속도: {total_processed/total_time:.1f}개/초")
            
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
        batch_size = 100  # 한 번에 처리할 기사 수 (최적화됨)
        max_workers = 4  # 병렬 처리 스레드 수
        
        print(f"⚙️  설정: 배치 크기 {batch_size}개, 최대 워커 {max_workers}개")
        
        # is_preprocessed = False인 모든 기사 처리
        max_articles = None  # 전체 처리
        
        # 전처리 실행 (Option 1: Direct Query 방식)
        preprocessor = FastPreprocessor(batch_size=batch_size, max_workers=max_workers)
        success = preprocessor.process_all_false_articles()
        
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
