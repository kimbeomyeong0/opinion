#!/usr/bin/env python3
"""
텍스트 처리 실행 스크립트
- articles_cleaned 테이블에서 기사 조회
- 텍스트 정제 + 정규화 수행
- title_cleaned, content_cleaned 컬럼에 저장
"""

import sys
import os
from typing import List, Dict, Any

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.supabase_manager import SupabaseManager
from preprocessing.modules.text_processor import TextProcessor

def main():
    """텍스트 처리 메인 함수"""
    print("🚀 텍스트 처리 프로세스 시작...")
    
    # Supabase 연결
    supabase_manager = SupabaseManager()
    if not supabase_manager.client:
        print("❌ Supabase 연결 실패")
        return
    
    # 텍스트 프로세서 초기화
    text_processor = TextProcessor()
    
    try:
        # articles 테이블에서 articles_cleaned에 없는 기사들 조회
        print("📡 기사 데이터 조회 중...")
        
        # 먼저 이미 전처리된 기사 ID들을 가져옴
        processed_result = supabase_manager.client.table('articles_cleaned').select('article_id').execute()
        processed_ids = set(item['article_id'] for item in processed_result.data)
        
        # articles 테이블에서 모든 기사 조회 (날짜 필터 제거)
        result = supabase_manager.client.table('articles').select(
            'id, title, content, url, published_at, media_id'
        ).execute()
        
        # 이미 전처리된 기사 제외
        if result.data:
            result.data = [article for article in result.data if article['id'] not in processed_ids]
        
        articles = result.data if result else []
        print(f"✅ {len(articles)}개 기사 조회 완료")
        
        if not articles:
            print("📝 처리할 기사가 없습니다.")
            return
        
        # 텍스트 처리 수행
        print("🔧 텍스트 정제 및 정규화 시작...")
        processed_count = 0
        failed_count = 0
        
        for article in articles:
            try:
                article_id = article['id']
                title = article.get('title', '')
                content = article.get('content', '')
                url = article.get('url', '')
                
                # 간단한 언론사 식별
                media_outlet = 'unknown'
                if 'chosun.com' in url:
                    media_outlet = 'chosun'
                elif 'hani.co.kr' in url:
                    media_outlet = 'hani'
                elif 'yonhapnews.co.kr' in url:
                    media_outlet = 'yonhap'
                elif 'donga.com' in url:
                    media_outlet = 'donga'
                elif 'joongang.co.kr' in url:
                    media_outlet = 'joongang'
                elif 'khan.co.kr' in url:
                    media_outlet = 'khan'
                elif 'ohmynews.com' in url:
                    media_outlet = 'ohmynews'
                elif 'newsis.com' in url:
                    media_outlet = 'newsis'
                
                # 제목 정제
                cleaned_title, title_patterns = text_processor.clean_title(title, media_outlet)
                
                # 본문 정제
                cleaned_content, content_patterns = text_processor.clean_content(content, media_outlet)
                
                # 제목 정규화
                title_result = text_processor.normalize_text(cleaned_title)
                final_title = title_result.normalized_text
                
                # 본문 정규화
                content_result = text_processor.normalize_text(cleaned_content)
                final_content = content_result.normalized_text
                
                # articles_cleaned 테이블에 새 레코드 생성 (제목 + 리드만)
                data = {
                    'article_id': article_id,
                    'merged_content': f"제목: {final_title}\n\n리드: {final_content}",
                    'media_id': article.get('media_id'),
                    'published_at': article.get('published_at')
                }
                
                insert_result = supabase_manager.client.table('articles_cleaned').insert(data).execute()
                
                if insert_result.data:
                    # articles 테이블에는 preprocessing_status 컬럼이 없으므로 업데이트 불필요
                    pass
                
                if insert_result.data:
                    processed_count += 1
                    if processed_count % 100 == 0:
                        print(f"✅ {processed_count}개 기사 처리 완료...")
                else:
                    failed_count += 1
                    
            except Exception as e:
                print(f"❌ 기사 {article.get('id', 'Unknown')} 처리 실패: {str(e)}")
                failed_count += 1
                continue
        
        print(f"\n📊 텍스트 처리 완료:")
        print(f"  성공: {processed_count}개")
        print(f"  실패: {failed_count}개")
        print(f"  총 처리: {len(articles)}개")
        
    except Exception as e:
        print(f"❌ 텍스트 처리 프로세스 실패: {str(e)}")

if __name__ == "__main__":
    main()
