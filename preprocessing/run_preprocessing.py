#!/usr/bin/env python3
"""
기사 전처리 스크립트
- KST 기준 날짜 입력받아 UTC로 변환
- 해당 날짜의 기사들을 앞 5문장 + 노이즈 제거하여 전처리
- articles_cleaned 테이블에 저장
"""

import sys
import os
import re
from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Any, Optional

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utils.supabase_manager import SupabaseManager

class ArticlePreprocessor:
    """기사 전처리 클래스"""
    
    def __init__(self):
        """초기화"""
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
    
    def get_kst_date_range(self, date_str: str) -> tuple:
        """
        KST 날짜 문자열을 UTC 범위로 변환
        
        Args:
            date_str: "0909" 형태의 날짜 문자열
            
        Returns:
            tuple: (start_utc, end_utc) UTC datetime 객체들
        """
        try:
            # KST 시간대 설정
            kst = pytz.timezone('Asia/Seoul')
            utc = pytz.UTC
            
            # 현재 연도 가져오기
            current_year = datetime.now().year
            
            # 날짜 파싱 (MMDD 형태)
            month = int(date_str[:2])
            day = int(date_str[2:])
            
            # KST 기준 해당 날짜 00:00:00
            kst_start = kst.localize(datetime(current_year, month, day, 0, 0, 0))
            # KST 기준 해당 날짜 23:59:59
            kst_end = kst.localize(datetime(current_year, month, day, 23, 59, 59))
            
            # UTC로 변환
            utc_start = kst_start.astimezone(utc)
            utc_end = kst_end.astimezone(utc)
            
            print(f"📅 KST {month}월 {day}일 → UTC {utc_start.strftime('%Y-%m-%d %H:%M:%S')} ~ {utc_end.strftime('%Y-%m-%d %H:%M:%S')}")
            
            return utc_start, utc_end
            
        except Exception as e:
            raise Exception(f"날짜 변환 실패: {str(e)}")
    
    def extract_lead_sentences(self, content: str, max_sentences: int = 5) -> str:
        """
        앞 5문장 추출
        
        Args:
            content: 기사 본문
            max_sentences: 추출할 문장 수
            
        Returns:
            str: 추출된 문장들
        """
        if not content:
            return ""
        
        # 문장 분리 (간단한 '.' 기준)
        sentences = content.split('.')
        
        # 빈 문장 제거
        sentences = [s.strip() for s in sentences if s.strip()]
        
        # 앞 N문장 추출
        lead_sentences = sentences[:max_sentences]
        lead_content = '. '.join(lead_sentences)
        
        return lead_content.strip()
    
    def clean_noise(self, text: str) -> str:
        """
        기본 노이즈 제거
        
        Args:
            text: 정제할 텍스트
            
        Returns:
            str: 정제된 텍스트
        """
        if not text:
            return ""
        
        # 1. 언론사 정보 제거
        text = re.sub(r'\([^)]*\)', '', text)
        
        # 2. 기자명 제거
        text = re.sub(r'[가-힣]{2,4}\s*기자\s*=', '', text)
        
        # 3. 시리즈 표시 제거
        text = re.sub(r'\[[^\]]*\]', '', text)
        
        # 4. 특수 기호 제거
        text = re.sub(r'[◇【】…]', '', text)
        
        # 5. HTML 태그 제거
        text = re.sub(r'<[^>]*>', '', text)
        text = re.sub(r'&[a-zA-Z0-9#]+;', '', text)
        
        # 6. 공백 정리
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def preprocess_article(self, article: Dict[str, Any]) -> tuple:
        """
        기사 전처리
        
        Args:
            article: 기사 데이터
            
        Returns:
            tuple: (전처리된 텍스트 또는 None, 실패 원인)
        """
        try:
            content = article.get('content', '')
            if not content:
                return None, "내용 없음"
            
            # 1. 앞 5문장 추출
            lead_content = self.extract_lead_sentences(content)
            if not lead_content:
                return None, "리드 문장 추출 실패"
            
            # 2. 기본 노이즈 제거
            cleaned_content = self.clean_noise(lead_content)
            if not cleaned_content:
                return None, "노이즈 제거 후 내용 없음"
            
            # 3. 최소 길이 확인 (50자 미만이면 제외)
            if len(cleaned_content) < 50:
                return None, f"너무 짧음 ({len(cleaned_content)}자)"
            
            return cleaned_content, None
            
        except Exception as e:
            return None, f"예외 발생: {str(e)}"
    
    def fetch_articles_by_date(self, start_utc: datetime, end_utc: datetime) -> List[Dict[str, Any]]:
        """
        지정된 날짜 범위의 기사 조회
        
        Args:
            start_utc: 시작 UTC 시간
            end_utc: 종료 UTC 시간
            
        Returns:
            List[Dict]: 기사 리스트
        """
        try:
            print(f"📡 {start_utc.strftime('%Y-%m-%d')} 기사 조회 중...")
            
            result = self.supabase_manager.client.table('articles').select(
                'id, title, content, media_id, published_at'
            ).gte('published_at', start_utc.isoformat()).lte('published_at', end_utc.isoformat()).execute()
            
            articles = result.data if result.data else []
            print(f"✅ {len(articles)}개 기사 조회 완료")
            
            return articles
            
        except Exception as e:
            print(f"❌ 기사 조회 실패: {str(e)}")
            return []
    
    def save_to_articles_cleaned(self, processed_articles: List[Dict[str, Any]]) -> bool:
        """
        전처리된 기사를 articles_cleaned 테이블에 저장
        
        Args:
            processed_articles: 전처리된 기사 리스트
            
        Returns:
            bool: 저장 성공 여부
        """
        if not processed_articles:
            print("⚠️ 저장할 기사가 없습니다.")
            return True
        
        try:
            print(f"💾 {len(processed_articles)}개 기사를 articles_cleaned에 저장 중...")
            
            result = self.supabase_manager.client.table('articles_cleaned').insert(processed_articles).execute()
            
            if result.data:
                print(f"✅ {len(result.data)}개 기사 저장 완료")
                return True
            else:
                print("❌ 기사 저장 실패")
                return False
                
        except Exception as e:
            print(f"❌ 저장 실패: {str(e)}")
            return False
    
    def process_articles(self, date_str: str) -> bool:
        """
        기사 전처리 메인 프로세스
        
        Args:
            date_str: "0909" 형태의 날짜 문자열
            
        Returns:
            bool: 처리 성공 여부
        """
        try:
            print(f"🚀 {date_str} 기사 전처리 시작...")
            
            # 1. 날짜 범위 변환
            start_utc, end_utc = self.get_kst_date_range(date_str)
            
            # 2. 기사 조회
            articles = self.fetch_articles_by_date(start_utc, end_utc)
            
            if not articles:
                print("📝 처리할 기사가 없습니다.")
                return True
            
            # 3. 전처리 수행
            processed_articles = []
            success_count = 0
            failed_count = 0
            failure_reasons = {}
            
            print("🔧 기사 전처리 중...")
            
            for article in articles:
                processed_content, failure_reason = self.preprocess_article(article)
                
                if processed_content:
                    processed_articles.append({
                        'article_id': article['id'],
                        'merged_content': processed_content,
                        'media_id': article['media_id'],
                        'published_at': article['published_at']
                    })
                    success_count += 1
                else:
                    failed_count += 1
                    # 실패 원인 카운트
                    failure_reasons[failure_reason] = failure_reasons.get(failure_reason, 0) + 1
                
                # 진행 상황 출력
                if (success_count + failed_count) % 10 == 0:
                    print(f"  진행: {success_count + failed_count}/{len(articles)} (성공: {success_count}, 실패: {failed_count})")
            
            print(f"📊 전처리 완료: 성공 {success_count}개, 실패 {failed_count}개")
            
            # 실패 원인 상세 출력
            if failure_reasons:
                print("\n❌ 실패 원인 분석:")
                for reason, count in sorted(failure_reasons.items(), key=lambda x: x[1], reverse=True):
                    print(f"  - {reason}: {count}개")
            
            # 4. 저장
            if processed_articles:
                save_success = self.save_to_articles_cleaned(processed_articles)
                return save_success
            else:
                print("⚠️ 전처리된 기사가 없습니다.")
                return True
                
        except Exception as e:
            print(f"❌ 전처리 프로세스 실패: {str(e)}")
            return False

def main():
    """메인 함수"""
    print("=" * 60)
    print("📰 기사 전처리 스크립트")
    print("=" * 60)
    
    try:
        # 날짜 입력 받기
        while True:
            date_input = input("\n원하는 날짜를 입력하세요 (예: 0909): ").strip()
            
            if not date_input:
                print("❌ 날짜를 입력해주세요.")
                continue
            
            if len(date_input) != 4 or not date_input.isdigit():
                print("❌ MMDD 형태로 입력해주세요 (예: 0909).")
                continue
            
            # 날짜 유효성 검사
            try:
                month = int(date_input[:2])
                day = int(date_input[2:])
                
                if month < 1 or month > 12:
                    print("❌ 월은 01-12 사이여야 합니다.")
                    continue
                
                if day < 1 or day > 31:
                    print("❌ 일은 01-31 사이여야 합니다.")
                    continue
                
                break
                
            except ValueError:
                print("❌ 올바른 날짜를 입력해주세요.")
                continue
        
        # 전처리 실행
        preprocessor = ArticlePreprocessor()
        success = preprocessor.process_articles(date_input)
        
        if success:
            print(f"\n✅ {date_input} 기사 전처리 완료!")
        else:
            print(f"\n❌ {date_input} 기사 전처리 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()
