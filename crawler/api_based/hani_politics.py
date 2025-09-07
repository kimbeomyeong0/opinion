#!/usr/bin/env python3
"""
한겨레 정치 기사 크롤러
API를 통해 정치 기사를 수집합니다.
"""

import asyncio
import httpx
import json
from datetime import datetime
from urllib.parse import urljoin
from rich.console import Console
from playwright.async_api import async_playwright
import pytz

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.supabase_manager import SupabaseManager

console = Console()

class HaniPoliticsCollector:
    """한겨레 정치 기사 수집기"""
    
    def __init__(self):
        self.media_name = "한겨레"
        self.base_url = "https://www.hani.co.kr"
        self.api_base = "https://www.hani.co.kr/_next/data/EM02RniQA0XrP2aTiUFUG/arti/politics.json"
        self.articles = []
        self.supabase_manager = SupabaseManager()
        
    async def _get_page_articles(self, page_num: int) -> list:
        """특정 페이지에서 기사 목록 수집"""
        try:
            url = f"{self.api_base}?section=politics&page={page_num}"
            console.print(f"📡 페이지 수집: {url}")
            
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                
                data = response.json()
                
                # articleList에서 기사 정보 추출
                article_list = data.get('pageProps', {}).get('listData', {}).get('articleList', [])
                console.print(f"🔍 API에서 {len(article_list)}개 기사 발견")
                
                articles = []
                if article_list:
                    # 각 페이지에서 최대 15개 기사 수집
                    max_articles_per_page = 15
                    collected_count = 0
                    
                    for article in article_list:
                        if collected_count >= max_articles_per_page:
                            break
                            
                        title = article.get('title', '').strip()
                        article_url = article.get('url', '')
                        create_date = article.get('createDate', '')
                        
                        if title and article_url:
                            # 상대 URL을 절대 URL로 변환
                            if article_url.startswith('/'):
                                full_url = urljoin(self.base_url, article_url)
                            else:
                                full_url = article_url
                            
                            articles.append({
                                'title': title,
                                'url': full_url,
                                'published_at': create_date
                            })
                            collected_count += 1
                            console.print(f"📰 기사 발견 [{collected_count}]: {title[:50]}...")
                
                console.print(f"📊 페이지에서 {len(articles)}개 기사 발견")
                return articles
                
        except Exception as e:
            console.print(f"❌ 페이지 수집 실패: {e}")
            return []
    
    async def _extract_content(self, url: str) -> dict:
        """기사 본문 추출"""
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                await page.goto(url, wait_until='domcontentloaded', timeout=60000)
                
                # 기사 본문 추출
                content_data = await page.evaluate("""
                    () => {
                        // 기사 본문 영역 찾기
                        const contentArea = document.querySelector('.article-text');
                        if (!contentArea) return { content: '', published_at: '' };
                        
                        // 불필요한 요소 제거
                        const elementsToRemove = [
                            '.ArticleDetailAudioPlayer_wrap__',
                            '.ArticleDetailContent_imageContainer__',
                            '.ArticleDetailContent_adWrap__',
                            '.ArticleDetailContent_adFlex__',
                            '.BaseAd_adWrapper__'
                        ];
                        
                        elementsToRemove.forEach(selector => {
                            const elements = contentArea.querySelectorAll(selector);
                            elements.forEach(el => el.remove());
                        });
                        
                        // <p class="text"> 태그만 추출
                        const textParagraphs = contentArea.querySelectorAll('p.text');
                        const contentLines = [];
                        
                        textParagraphs.forEach(p => {
                            const text = p.textContent || p.innerText || '';
                            const trimmedText = text.trim();
                            
                            // 기자 정보 제외 (이메일 포함)
                            if (trimmedText && 
                                !trimmedText.includes('@') && 
                                !trimmedText.includes('기자') &&
                                !trimmedText.includes('특파원') &&
                                !trimmedText.includes('통신원')) {
                                contentLines.push(trimmedText);
                            }
                        });
                        
                        // 각 <p>를 개행으로 구분하여 결합
                        const content = contentLines.join('\\n\\n');
                        
                        // 발행 시간 추출 (한겨레 특정 선택자)
                        const timeElement = document.querySelector('li.ArticleDetailView_dateListItem__mRc3d span');
                        const published_at = timeElement ? timeElement.textContent.trim() : '';
                        
                        return {
                            content: content,
                            published_at: published_at
                        };
                    }
                """)
                
                await browser.close()
                return content_data
                
        except Exception as e:
            console.print(f"❌ 본문 추출 실패 ({url}): {e}")
            return {"content": "", "published_at": ""}
    
    async def _parse_article_data(self, article: dict, content_data: dict) -> dict:
        """기사 데이터 파싱 및 정리"""
        try:
            # 발행 시간 처리
            published_at_str = content_data.get('published_at', '') or article.get('published_at', '')
            
            if published_at_str:
                # 한겨레 날짜 형식 파싱 (예: "2025-09-03 16:05")
                try:
                    if 'T' in published_at_str:
                        # ISO 형식인 경우
                        published_at = datetime.fromisoformat(published_at_str.replace('Z', '+00:00'))
                    else:
                        # "YYYY-MM-DD HH:MM" 형식인 경우 (KST 기준)
                        published_at = datetime.strptime(published_at_str, "%Y-%m-%d %H:%M")
                        # KST로 인식하고 UTC로 변환
                        kst = pytz.timezone("Asia/Seoul")
                        published_at = kst.localize(published_at).astimezone(pytz.UTC)
                except Exception as e:
                    console.print(f"⚠️ 발행시간 파싱 실패: {published_at_str} - {e}")
                    published_at = datetime.now(pytz.UTC)
            else:
                published_at = datetime.now(pytz.UTC)
            
            return {
                'title': article['title'],
                'url': article['url'],
                'content': content_data.get('content', ''),
                'published_at': published_at.isoformat(),
                'media_outlet': self.media_name
            }
            
        except Exception as e:
            console.print(f"❌ 기사 데이터 파싱 실패: {e}")
            return None
    
    async def collect_articles(self, num_pages: int = 10):
        """기사 수집"""
        console.print(f"📄 {num_pages}개 페이지에서 기사 수집 시작...")
        
        for page in range(1, num_pages + 1):
            console.print(f"📄 페이지 {page}/{num_pages} 처리 중...")
            articles = await self._get_page_articles(page)
            self.articles.extend(articles)
        
        console.print(f"📊 수집 완료: {len(self.articles)}개 성공")
    
    async def collect_contents(self):
        """기사 본문 수집"""
        console.print(f"📖 본문 수집 시작: {len(self.articles)}개 기사")
        
        for i, article in enumerate(self.articles, 1):
            console.print(f"📖 [{i}/{len(self.articles)}] 본문 수집 중: {article['title'][:50]}...")
            
            content_data = await self._extract_content(article['url'])
            
            # 기사 데이터에 본문과 발행시간 추가
            article['content'] = content_data.get('content', '')
            article['published_at'] = content_data.get('published_at', article.get('published_at', ''))
            
            console.print(f"✅ [{i}/{len(self.articles)}] 본문 수집 성공")
    
    async def save_articles(self):
        """기사 저장"""
        console.print(f"💾 Supabase에 {len(self.articles)}개 기사 저장 중...")
        
        # 언론사 확인
        media = self.supabase_manager.get_media_outlet(self.media_name)
        if not media:
            media_id = self.supabase_manager.create_media_outlet(
                name=self.media_name,
                bias="center-left",
                website=self.base_url
            )
        else:
            media_id = media["id"]
        
        # 중복 체크
        urls = [art["url"] for art in self.articles]
        existing_urls = set()
        
        try:
            for url in urls:
                exists = self.supabase_manager.client.table("articles").select("url").eq("url", url).execute()
                if exists.data:
                    existing_urls.add(url)
        except Exception as e:
            console.print(f"⚠️ 중복 체크 중 오류: {e}")
        
        success_count = 0
        skip_count = 0
        
        for i, article in enumerate(self.articles, 1):
            try:
                # 중복 체크
                if article["url"] in existing_urls:
                    skip_count += 1
                    console.print(f"⚠️ [{i}/{len(self.articles)}] 중복 기사 스킵: {article['title'][:50]}...")
                    continue
                
                # 기사 데이터 파싱
                parsed_article = await self._parse_article_data(article, article)
                
                if not parsed_article:
                    continue
                
                # media_id 추가
                if media_id:
                    parsed_article['media_id'] = media_id
                
                # media_outlet 필드 제거 (스키마에 없음)
                if 'media_outlet' in parsed_article:
                    del parsed_article['media_outlet']
                
                # Supabase에 저장
                result = self.supabase_manager.insert_article(parsed_article)
                
                if result:
                    success_count += 1
                    console.print(f"✅ [{i}/{len(self.articles)}] 저장 성공: {parsed_article['title'][:50]}...")
                else:
                    console.print(f"❌ [{i}/{len(self.articles)}] 저장 실패")
                    
            except Exception as e:
                console.print(f"❌ [{i}/{len(self.articles)}] 저장 중 오류: {e}")
        
        console.print(f"\n📊 저장 결과:")
        console.print(f"  ✅ 성공: {success_count}개")
        console.print(f"  ❌ 실패: {len(self.articles) - success_count - skip_count}개")
        console.print(f"  ⚠️ 중복 스킵: {skip_count}개")
        console.print(f"  📈 성공률: {success_count/len(self.articles)*100:.1f}%")
    
    async def run(self, num_pages: int = 10):
        """크롤러 실행"""
        try:
            console.print("🚀 한겨레 정치 기사 크롤링 시작")
            
            # 1. 기사 목록 수집
            await self.collect_articles(num_pages)
            
            if not self.articles:
                console.print("❌ 수집된 기사가 없습니다")
                return
            
            # 2. 기사 본문 수집
            await self.collect_contents()
            
            # 3. 기사 저장
            await self.save_articles()
            
            console.print("🎉 크롤링 완료!")
            
        except KeyboardInterrupt:
            console.print("⏹️ 사용자에 의해 중단되었습니다")
        except Exception as e:
            console.print(f"❌ 크롤링 중 오류 발생: {str(e)}")

async def main():
    collector = HaniPoliticsCollector()
    await collector.run(num_pages=10)  # 10페이지에서 각각 15개씩 총 150개 수집

if __name__ == "__main__":
    asyncio.run(main())
