#!/usr/bin/env python3
"""
한겨레 정치 섹션 크롤러
- API 기반으로 기사 목록 및 본문 수집
- 페이지네이션 지원
- HTML 파싱으로 본문 추출
"""

import sys
import os
import re
import json
import time
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import html

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils.supabase_manager import SupabaseManager

class HaniPoliticsCrawler:
    """한겨레 정치 섹션 크롤러"""
    
    def __init__(self):
        """초기화"""
        self.base_url = "https://www.hani.co.kr"
        self.politics_url = "https://www.hani.co.kr/arti/politics"
        self.api_base_url = "https://www.hani.co.kr/_next/data/RJLhj-Yk0mGbw6YoyvYyz/arti/politics.json"
        
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
        
        # 세션 설정
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def get_media_outlet_id(self) -> Optional[int]:
        """한겨레 언론사 ID 조회"""
        try:
            result = self.supabase_manager.client.table('media_outlets').select('id').eq('name', '한겨레').execute()
            if result.data:
                return result.data[0]['id']
            return None
        except Exception as e:
            print(f"❌ 언론사 ID 조회 실패: {str(e)}")
            return None
    
    def fetch_articles_page(self, page: int) -> List[Dict[str, Any]]:
        """
        특정 페이지의 기사 목록 조회
        
        Args:
            page: 페이지 번호 (1부터 시작)
            
        Returns:
            List[Dict]: 기사 목록
        """
        try:
            # API URL 구성
            api_url = f"{self.api_base_url}?section=politics&page={page}"
            
            print(f"📡 페이지 {page} 기사 목록 조회 중...")
            
            response = self.session.get(api_url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # 기사 목록 추출
            articles = []
            if 'pageProps' in data and 'listData' in data['pageProps'] and 'articleList' in data['pageProps']['listData']:
                articles_data = data['pageProps']['listData']['articleList']
                
                for article_data in articles_data:
                    try:
                        # published_at 추출
                        published_at = article_data.get('updateDate', '')
                        if published_at:
                            # ISO 형식으로 변환
                            published_at = published_at.replace('Z', '+00:00')
                        
                        article = {
                            'title': article_data.get('title', ''),
                            'url': article_data.get('url', ''),
                            'published_at': published_at,
                            'media_id': self.get_media_outlet_id()
                        }
                        
                        # URL이 상대경로인 경우 절대경로로 변환
                        if article['url'] and not article['url'].startswith('http'):
                            article['url'] = urljoin(self.base_url, article['url'])
                        
                        articles.append(article)
                        
                    except Exception as e:
                        print(f"⚠️ 기사 데이터 파싱 실패: {str(e)}")
                        continue
            
            print(f"✅ 페이지 {page}: {len(articles)}개 기사 조회 완료")
            return articles
            
        except Exception as e:
            print(f"❌ 페이지 {page} 조회 실패: {str(e)}")
            return []
    
    def clean_text(self, text: str) -> str:
        """
        텍스트 정리
        
        Args:
            text: 정리할 텍스트
            
        Returns:
            str: 정리된 텍스트
        """
        if not text:
            return ""
        
        # HTML 엔티티 디코딩
        text = html.unescape(text)
        
        # <br> 태그를 \n으로 변환
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        
        # HTML 태그 제거
        text = re.sub(r'<[^>]+>', '', text)
        
        # 공백 정리
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        return text
    
    def extract_article_content(self, article_url: str) -> str:
        """
        기사 본문 추출
        
        Args:
            article_url: 기사 URL
            
        Returns:
            str: 추출된 본문 텍스트
        """
        try:
            print(f"📄 본문 추출 중: {article_url}")
            
            response = self.session.get(article_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # div.article-text 찾기
            article_text_div = soup.find('div', class_='article-text')
            if not article_text_div:
                print(f"⚠️ article-text div를 찾을 수 없습니다: {article_url}")
                return ""
            
            # 제거할 요소들
            unwanted_selectors = [
                '[class*="ArticleDetailAudioPlayer"]',
                '[class*="ArticleDetailContent_adWrap"]',
                '[class*="ArticleDetailContent_adFlex"]',
                '[class*="BaseAd_"]',
                'figure',
                'script',
                'style',
                'noscript',
                'iframe'
            ]
            
            # 불필요한 요소 제거
            for selector in unwanted_selectors:
                for element in article_text_div.select(selector):
                    element.decompose()
            
            # p.text 요소들 추출
            paragraphs = []
            for p in article_text_div.find_all('p', class_='text'):
                text = self.clean_text(p.get_text())
                if text and text.strip():  # 공백만 있는 단락 제거
                    paragraphs.append(text.strip())
            
            # 단락들을 \n\n로 연결
            content = '\n\n'.join(paragraphs)
            
            print(f"✅ 본문 추출 완료: {len(content)}자")
            return content
            
        except Exception as e:
            print(f"❌ 본문 추출 실패: {article_url} - {str(e)}")
            return ""
    
    def process_article(self, article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        개별 기사 처리
        
        Args:
            article: 기사 데이터
            
        Returns:
            Optional[Dict]: 처리된 기사 데이터
        """
        try:
            # 본문 추출
            content = self.extract_article_content(article['url'])
            if not content:
                print(f"⚠️ 본문 추출 실패로 건너뜀: {article['title']}")
                return None
            
            # 최종 기사 데이터 구성
            processed_article = {
                'title': article['title'],
                'content': content,
                'url': article['url'],
                'published_at': article['published_at'],
                'media_id': article['media_id'],
                'is_preprocessed': False  # 전처리되지 않은 상태로 저장
            }
            
            return processed_article
            
        except Exception as e:
            print(f"❌ 기사 처리 실패: {article['title']} - {str(e)}")
            return None
    
    def crawl_articles(self, max_pages: int = 5) -> List[Dict[str, Any]]:
        """
        기사 크롤링 메인 함수
        
        Args:
            max_pages: 최대 페이지 수
            
        Returns:
            List[Dict]: 크롤링된 기사 목록
        """
        try:
            print(f"🚀 한겨레 정치 섹션 크롤링 시작... (최대 {max_pages}페이지)")
            
            all_articles = []
            
            for page in range(1, max_pages + 1):
                # 페이지별 기사 목록 조회
                articles = self.fetch_articles_page(page)
                if not articles:
                    print(f"📝 페이지 {page}에 기사가 없습니다.")
                    break
                
                # 각 기사 처리
                for article in articles:
                    processed_article = self.process_article(article)
                    if processed_article:
                        all_articles.append(processed_article)
                
                # 페이지 간 대기
                time.sleep(1)
            
            print(f"🎉 크롤링 완료: 총 {len(all_articles)}개 기사")
            return all_articles
            
        except Exception as e:
            print(f"❌ 크롤링 실패: {str(e)}")
            return []
    
    def save_articles(self, articles: List[Dict[str, Any]]) -> int:
        """
        기사를 데이터베이스에 저장
        
        Args:
            articles: 저장할 기사 목록
            
        Returns:
            int: 저장된 기사 수
        """
        if not articles:
            print("📝 저장할 기사가 없습니다.")
            return 0
        
        try:
            print(f"💾 {len(articles)}개 기사를 데이터베이스에 저장 중...")
            
            success_count = 0
            for article in articles:
                if self.supabase_manager.insert_article(article):
                    success_count += 1
            
            print(f"✅ {success_count}개 기사 저장 완료")
            return success_count
            
        except Exception as e:
            print(f"❌ 기사 저장 실패: {str(e)}")
            return 0

def main():
    """메인 함수"""
    print("=" * 60)
    print("📰 한겨레 정치 섹션 크롤러")
    print("=" * 60)
    
    try:
        # 크롤러 초기화
        crawler = HaniPoliticsCrawler()
        
        # 크롤링 실행
        articles = crawler.crawl_articles(max_pages=10)
        
        if articles:
            # 데이터베이스에 저장
            saved_count = crawler.save_articles(articles)
            print(f"\n🎉 크롤링 및 저장 완료!")
            print(f"📊 크롤링: {len(articles)}개, 저장: {saved_count}개")
        else:
            print("\n❌ 크롤링된 기사가 없습니다.")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()
