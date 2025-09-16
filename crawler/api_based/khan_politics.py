#!/usr/bin/env python3
"""
경향신문 정치 섹션 기사 크롤러
"""

import asyncio
import sys
import os
from datetime import datetime
import httpx
import pytz
from playwright.async_api import async_playwright
from rich.console import Console

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.supabase_manager import SupabaseManager

console = Console()

class KhanPoliticsCollector:
    def __init__(self):
        self.base_url = "https://www.khan.co.kr"
        self.api_url = "https://www.khan.co.kr/SecListData.html"
        self.media_name = "경향신문"
        self.articles = []
        self.supabase_manager = SupabaseManager()
        self._playwright = None
        self._browser = None
        self._semaphore = asyncio.Semaphore(4)  # 동시 처리 제한
        
    async def run(self, num_pages: int = 15):
        """크롤링 실행"""
        try:
            console.print(f"🚀 {self.media_name} 정치 기사 크롤링 시작")
            
            # 기사 목록 수집
            await self.collect_articles(num_pages)
            
            if not self.articles:
                console.print("❌ 수집된 기사가 없습니다.")
                return
            
            # 본문 수집
            await self.collect_contents()
            
            # 데이터베이스 저장
            await self.save_articles()
            
            console.print("\\n🎉 크롤링 완료!")
            
        except KeyboardInterrupt:
            console.print("⏹️ 사용자에 의해 중단되었습니다")
        except Exception as e:
            console.print(f"❌ 크롤링 중 오류 발생: {str(e)}")
        finally:
            await self.cleanup()
    
    async def collect_articles(self, num_pages: int = 15):
        """기사 목록 수집 - 병렬 처리로 최적화"""
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
        
        console.print(f"\\n📊 수집 완료: {len(self.articles)}개 성공")
    
    async def _get_page_articles(self, page_num: int):
        """특정 페이지에서 기사 목록을 가져옵니다."""
        try:
            # 현재 년월 계산
            now = datetime.now()
            year = now.year
            month = now.month
            
            # API 요청 데이터
            payload = {
                "syncType": "async",
                "type": "politics", 
                "year": str(year),
                "month": str(month).zfill(2),
                "page": str(page_num)
            }
            
            console.print(f"📡 페이지 {page_num} API 호출: {payload}")
            
            async with httpx.AsyncClient(timeout=10.0) as client:  # 타임아웃 단축
                response = await client.post(
                    self.api_url,
                    data=payload,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                    }
                )
                response.raise_for_status()
                
                data = response.json()
                items = data.get("items", [])
                console.print(f"📊 페이지 {page_num}에서 {len(items)}개 기사 발견")
                
                articles = []
                for i, item in enumerate(items[:10]):  # 각 페이지에서 최대 10개 수집
                    article = {
                        "art_id": item.get("art_id"),
                        "title": item.get("art_title", ""),
                        "summary": item.get("summary", ""),
                        "publish_date": item.get("publish_date", ""),
                        "url": item.get("url", f"{self.base_url}/article/{item.get('art_id')}")
                    }
                    articles.append(article)
                    console.print(f"📰 기사 {i+1}: {article['title'][:50]}...")
                
                return articles
                
        except Exception as e:
            console.print(f"❌ 페이지 {page_num} 수집 실패: {str(e)}")
            return []

    async def collect_contents(self):
        """기사 본문 수집 - 병렬 처리로 최적화"""
        console.print(f"📖 본문 수집 시작: {len(self.articles)}개 기사")
        
        # 브라우저 초기화
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
                    '--memory-pressure-off',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding'
                ]
            )
        
        # 병렬 처리로 본문 수집
        tasks = [self._extract_content_with_browser(article['url']) for article in self.articles]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 처리
        success_count = 0
        for i, (article, result) in enumerate(zip(self.articles, results), 1):
            if isinstance(result, Exception):
                console.print(f"❌ [{i}/{len(self.articles)}] 본문 수집 실패: {result}")
                article['content'] = ''
                article['published_at'] = article.get('publish_date', '')
            else:
                article['content'] = result.get('content', '')
                article['published_at'] = result.get('published_at', article.get('publish_date', ''))
                success_count += 1
                console.print(f"✅ [{i}/{len(self.articles)}] 본문 수집 성공")
        
        console.print(f"📊 본문 수집 완료: {success_count}/{len(self.articles)}개 성공")
        
    async def _extract_content_with_browser(self, url: str):
        """브라우저 재사용하여 기사 내용 추출 - 최적화된 버전"""
        async with self._semaphore:  # 동시 처리 제한
            page = None
            try:
                page = await self._browser.new_page()
                await page.goto(url, wait_until='domcontentloaded', timeout=10000)  # 타임아웃 단축
                
                # 본문 영역 대기 (우선순위별)
                content_selectors = [
                    'div.art_body#articleBody',
                    'div.art_body',
                    'div[class*="art_body"]',
                    '.article-body',
                    'article'
                ]
                
                content_loaded = False
                for selector in content_selectors:
                    try:
                        await page.wait_for_selector(selector, timeout=2000)
                        content_loaded = True
                        break
                    except:
                        continue
                
                if not content_loaded:
                    console.print(f"⚠️ 본문 영역 로드 실패: {url}")
                    return {"content": "", "published_at": ""}
                
                # JavaScript로 데이터 추출 - 최적화된 버전
                content_data = await page.evaluate("""
                    () => {
                        const result = { published_at: '', content: '' };
                        
                        // 1. 발행시각 추출 (우선순위별)
                        const timeSelectors = [
                            'a[title*="기사 입력/수정일"]',
                            '.article-date',
                            '.date',
                            'time'
                        ];
                        
                        for (const selector of timeSelectors) {
                            const element = document.querySelector(selector);
                            if (element) {
                                const paragraphs = element.querySelectorAll('p');
                                let inputTime = '';
                                let modifyTime = '';
                                
                                paragraphs.forEach(p => {
                                    const text = p.textContent || '';
                                    if (text.includes('입력')) {
                                        inputTime = text.replace('입력', '').trim();
                                    } else if (text.includes('수정')) {
                                        modifyTime = text.replace('수정', '').trim();
                                    }
                                });
                                
                                result.published_at = modifyTime || inputTime || element.textContent?.trim();
                                if (result.published_at) break;
                            }
                        }
                        
                        // 2. 본문 추출 (우선순위별)
                        const contentSelectors = [
                            'div.art_body#articleBody',
                            'div.art_body',
                            'div[class*="art_body"]',
                            'div[class*="article"]',
                            'div[class*="content"]'
                        ];
                        
                        let articleBody = null;
                        for (const selector of contentSelectors) {
                            articleBody = document.querySelector(selector);
                            if (articleBody) break;
                        }
                        
                        if (articleBody) {
                            // 광고/배너 제거
                            const unwantedSelectors = [
                                'div[class*="banner"]', 'div[class*="ad"]', 'div[class*="advertisement"]',
                                'script', 'style', 'noscript', 'iframe'
                            ];
                            
                            unwantedSelectors.forEach(selector => {
                                const elements = articleBody.querySelectorAll(selector);
                                elements.forEach(el => el.remove());
                            });
                            
                            // 본문 텍스트 추출 (우선순위별)
                            const paragraphSelectors = [
                                'p.content_text.text-l',
                                'p.content_text',
                                'p.text-l',
                                'p'
                            ];
                            
                            let contentParagraphs = null;
                            for (const selector of paragraphSelectors) {
                                contentParagraphs = articleBody.querySelectorAll(selector);
                                if (contentParagraphs.length > 0) break;
                            }
                            
                            const contentTexts = [];
                            
                            contentParagraphs?.forEach(p => {
                                let text = p.textContent?.trim() || '';
                                
                                // 필터링: 기자명, 이메일, 출처 제거
                                text = text.replace(/[가-힣]+\\s*기자/g, '')
                                          .replace(/[가-힣]+\\s*특파원/g, '')
                                          .replace(/[가-힣]+\\s*통신원/g, '')
                                          .replace(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/g, '')
                                          .replace(/\\[출처:[^\\]]+\\]/g, '')
                                          .replace(/\\[경향신문\\]/g, '');
                                
                                // 의미있는 텍스트만 추출
                                if (text && text.length > 20 && !text.match(/^\\s*$/)) {
                                    contentTexts.push(text);
                                }
                            });
                            
                            result.content = contentTexts.join('\\n\\n');
                        }
                        
                        return result;
                    }
                """)
                
                await page.close()
                return content_data
                
            except Exception as e:
                console.print(f"❌ 기사 내용 추출 실패 {url}: {str(e)}")
                return {"published_at": "", "content": ""}
            finally:
                if page:
                    try:
                        await page.close()
                    except:
                        pass
    
    
    def parse_published_time(self, time_str: str) -> datetime:
        """발행시간 문자열을 UTC datetime으로 변환합니다."""
        if not time_str or not time_str.strip():
            return datetime.now(pytz.UTC)
        
        try:
            # "2025.01.05 21:43" 형식 파싱
            clean_time = time_str.strip()
            if '.' in clean_time and ':' in clean_time:
                # KST 시간으로 파싱
                published_at = datetime.strptime(clean_time, "%Y.%m.%d %H:%M")
                kst = pytz.timezone("Asia/Seoul")
                published_at = kst.localize(published_at)
                # UTC로 변환
                return published_at.astimezone(pytz.UTC)
            else:
                console.print(f"⚠️ 알 수 없는 시간 형식: {clean_time}")
                return datetime.now(pytz.UTC)
            
        except Exception as e:
            console.print(f"⚠️ 발행시간 파싱 실패: {time_str} - {e}")
            return datetime.now(pytz.UTC)
    
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
                    console.print(f"⚠️ [{i}/{len(self.articles)}] 중복 기사 스킵: {article['title'][:50]}...")
                    skip_count += 1
                    continue
                
                # 발행시간 파싱
                published_at = self.parse_published_time(article["published_at"])
                
                # 기사 데이터 구성
                article_data = {
                    "title": article["title"],
                    "url": article["url"],
                    "content": article["content"],
                    "published_at": published_at.isoformat(),
                    "created_at": datetime.now(pytz.UTC).isoformat(),
                    "media_id": media_id
                }
                
                # 데이터베이스에 저장
                success = self.supabase_manager.insert_article(article_data)
                if success:
                    console.print(f"✅ [{i}/{len(self.articles)}] 저장 성공: {article['title'][:50]}...")
                    success_count += 1
                else:
                    console.print(f"❌ [{i}/{len(self.articles)}] 저장 실패: {article['title'][:50]}...")
                    
            except Exception as e:
                console.print(f"❌ [{i}/{len(self.articles)}] 처리 실패: {str(e)}")
        
        console.print(f"\\n📊 저장 결과:")
        console.print(f"  ✅ 성공: {success_count}개")
        console.print(f"  ⚠️ 중복 스킵: {skip_count}개")
        total_processed = success_count + skip_count
        success_rate = (success_count / total_processed) * 100 if total_processed > 0 else 0
        console.print(f"  📈 성공률: {success_rate:.1f}%")
    
    async def cleanup(self):
        """리소스 정리"""
        try:
            if self._browser:
                await self._browser.close()
                self._browser = None
            if self._playwright:
                await self._playwright.stop()
                self._playwright = None
            console.print("🧹 경향신문 크롤러 리소스 정리 완료")
        except Exception as e:
            console.print(f"⚠️ 리소스 정리 중 오류: {str(e)[:50]}")

async def main():
    collector = KhanPoliticsCollector()
    await collector.run(num_pages=15)  # 15페이지에서 각각 10개씩 총 150개 기사 수집

if __name__ == "__main__":
    asyncio.run(main())