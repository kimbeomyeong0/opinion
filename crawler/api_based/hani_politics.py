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
        self.politics_url = "https://www.hani.co.kr/arti/politics"
        self.articles = []
        self.supabase_manager = SupabaseManager()
        self._playwright = None
        self._browser = None
        self._semaphore = asyncio.Semaphore(3)  # 동시 처리 제한
        
    async def _get_page_articles(self, page_num: int) -> list:
        """특정 페이지에서 기사 목록 수집"""
        try:
            # 페이지 URL 구성
            if page_num == 1:
                url = self.politics_url
            else:
                url = f"{self.politics_url}?page={page_num}"
            
            console.print(f"📡 페이지 수집: {url}")
            
            # 브라우저 재사용
            if not self._browser:
                self._playwright = await async_playwright().start()
                self._browser = await self._playwright.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--disable-web-security',
                        '--disable-features=VizDisplayCompositor',
                        '--memory-pressure-off'
                    ]
                )
            
            page = await self._browser.new_page()
            await page.goto(url, wait_until='domcontentloaded', timeout=10000)
            
            # 기사 목록 추출
            articles = await page.evaluate("""
                    () => {
                        const articleElements = document.querySelectorAll('a[href*="/arti/politics/"][href$=".html"]');
                        const articles = [];
                        
                        articleElements.forEach((link, index) => {
                            const title = link.textContent.trim();
                            const href = link.href;
                            
                            // 제목이 있고, 실제 기사 URL인지 확인
                            if (title && href && href.includes('/arti/politics/') && href.endsWith('.html')) {
                                articles.push({
                                    title: title,
                                    url: href
                                });
                            }
                        });
                        
                        return articles.slice(0, 20); // 페이지당 최대 20개
                    }
                """)
            
            await page.close()
            
            console.print(f"🔍 페이지에서 {len(articles)}개 기사 발견")
            
            for i, article in enumerate(articles, 1):
                console.print(f"📰 기사 발견 [{i}]: {article['title'][:50]}...")
                
                return articles
                
        except Exception as e:
            console.print(f"❌ 페이지 수집 실패: {e}")
            return []
    
    async def _extract_content(self, url: str) -> dict:
        """기사 본문 추출"""
        async with self._semaphore:  # 동시 처리 제한
            try:
                # 브라우저 재사용
                if not self._browser:
                    self._playwright = await async_playwright().start()
                    self._browser = await self._playwright.chromium.launch(
                        headless=True,
                        args=[
                            '--no-sandbox',
                            '--disable-dev-shm-usage',
                            '--disable-gpu',
                            '--disable-web-security',
                            '--disable-features=VizDisplayCompositor',
                            '--memory-pressure-off'
                        ]
                    )
                
                page = await self._browser.new_page()
                await page.goto(url, wait_until='domcontentloaded', timeout=10000)
                
                # 기사 본문 추출 - 최적화된 선택자
                content_data = await page.evaluate("""
                    () => {
                        const result = { content: '', published_at: '' };
                        
                        // 1. 발행 시간 추출 (우선순위별)
                        const timeSelectors = [
                            'li.ArticleDetailView_dateListItem__mRc3d span',
                            '.article-date span',
                            '.date span',
                            'time'
                        ];
                        
                        for (const selector of timeSelectors) {
                            const element = document.querySelector(selector);
                            if (element && element.textContent.trim()) {
                                result.published_at = element.textContent.trim();
                                break;
                            }
                        }
                        
                        // 2. 본문 추출 (우선순위별)
                        const contentSelectors = [
                            '.article-text',
                            '.article-body',
                            '.content',
                            'article'
                        ];
                        
                        let contentArea = null;
                        for (const selector of contentSelectors) {
                            contentArea = document.querySelector(selector);
                            if (contentArea) break;
                        }
                        
                        if (contentArea) {
                            // 광고 요소 제거
                            const adSelectors = [
                                '.ArticleDetailAudioPlayer_wrap__',
                                '.ArticleDetailContent_imageContainer__',
                                '.ArticleDetailContent_adWrap__',
                                '.ArticleDetailContent_adFlex__',
                                '.BaseAd_adWrapper__',
                                '[class*="ad"]',
                                '[class*="Ad"]'
                            ];
                            
                            adSelectors.forEach(selector => {
                                const elements = contentArea.querySelectorAll(selector);
                                elements.forEach(el => el.remove());
                            });
                            
                            // 본문 텍스트 추출
                            const paragraphs = contentArea.querySelectorAll('p.text, p, div.text');
                            const contentLines = [];
                            
                            paragraphs.forEach(p => {
                                const text = p.textContent?.trim() || '';
                                
                                // 필터링: 기자 정보, 이메일, 너무 짧은 텍스트 제외
                                if (text && 
                                    text.length > 20 && 
                                    !text.includes('@') && 
                                    !text.includes('기자') &&
                                    !text.includes('특파원') &&
                                    !text.includes('통신원') &&
                                    !text.match(/^\\s*$/)) {
                                    contentLines.push(text);
                                }
                            });
                            
                            result.content = contentLines.join('\\n\\n');
                        }
                        
                        return result;
                    }
                """)
                
                await page.close()
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
        """기사 수집 - 병렬 처리로 최적화"""
        console.print(f"📄 {num_pages}개 페이지에서 기사 수집 시작...")
        
        # 병렬 처리로 페이지 수집
        tasks = [self._get_page_articles(page) for page in range(1, num_pages + 1)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 수집
        for i, result in enumerate(results, 1):
            if isinstance(result, Exception):
                console.print(f"❌ 페이지 {i} 수집 실패: {result}")
            else:
                self.articles.extend(result)
                console.print(f"✅ 페이지 {i} 수집 완료: {len(result)}개 기사")
        
        console.print(f"📊 수집 완료: {len(self.articles)}개 성공")
    
    async def collect_contents(self):
        """기사 본문 수집 - 병렬 처리로 최적화"""
        console.print(f"📖 본문 수집 시작: {len(self.articles)}개 기사")
        
        # 병렬 처리로 본문 수집
        tasks = [self._extract_content(article['url']) for article in self.articles]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 처리
        success_count = 0
        for i, (article, result) in enumerate(zip(self.articles, results), 1):
            if isinstance(result, Exception):
                console.print(f"❌ [{i}/{len(self.articles)}] 본문 수집 실패: {result}")
                article['content'] = ''
                article['published_at'] = article.get('published_at', '')
            else:
                article['content'] = result.get('content', '')
                article['published_at'] = result.get('published_at', article.get('published_at', ''))
                success_count += 1
                console.print(f"✅ [{i}/{len(self.articles)}] 본문 수집 성공")
        
        console.print(f"📊 본문 수집 완료: {success_count}/{len(self.articles)}개 성공")
    
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
    
    async def cleanup(self):
        """리소스 정리"""
        try:
            if self._browser:
                await self._browser.close()
                self._browser = None
            if self._playwright:
                await self._playwright.stop()
                self._playwright = None
            console.print("🧹 한겨레 크롤러 리소스 정리 완료")
        except Exception as e:
            console.print(f"⚠️ 리소스 정리 중 오류: {str(e)[:50]}")
    
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
        finally:
            await self.cleanup()

async def main():
    collector = HaniPoliticsCollector()
    await collector.run(num_pages=10)  # 10페이지에서 각각 15개씩 총 150개 수집

if __name__ == "__main__":
    asyncio.run(main())
