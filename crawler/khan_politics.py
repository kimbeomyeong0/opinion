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
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.supabase_manager import SupabaseManager

console = Console()

class KhanPoliticsCollector:
    def __init__(self):
        self.base_url = "https://www.khan.co.kr"
        self.api_url = "https://www.khan.co.kr/SecListData.html"
        self.media_name = "경향신문"
        self.articles = []
        self.supabase_manager = SupabaseManager()
        
    async def run(self, num_pages: int = 5):
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
    
    async def collect_articles(self, num_pages: int = 5):
        """기사 목록 수집"""
        console.print(f"📄 {num_pages}개 페이지에서 기사 수집 시작...")
        
        for page in range(1, num_pages + 1):
            console.print(f"\\n📄 페이지 {page}/{num_pages} 처리 중...")
            articles = await self._get_page_articles(page)
            self.articles.extend(articles)
        
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
            
            async with httpx.AsyncClient(timeout=30.0) as client:
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
        """기사 본문 수집"""
        console.print(f"📖 본문 수집 시작: {len(self.articles)}개 기사")
        
        # 브라우저 재사용을 위해 한 번만 실행
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            for i, article in enumerate(self.articles, 1):
                console.print(f"📖 [{i}/{len(self.articles)}] 본문 수집 중: {article['title'][:50]}...")
                
                content_data = await self._extract_content_with_browser(page, article['url'])
                
                # 기사 데이터에 본문과 발행시간 추가
                article['content'] = content_data.get('content', '')
                article['published_at'] = content_data.get('published_at', article.get('publish_date', ''))
                
                console.print(f"✅ [{i}/{len(self.articles)}] 본문 수집 성공")
            
            await browser.close()
        
    async def _extract_content_with_browser(self, page, url: str):
        """브라우저 재사용하여 기사 내용 추출"""
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            # 본문 영역이 로드될 때까지 대기 (여러 선택자 시도)
            try:
                await page.wait_for_selector('div.art_body#articleBody', timeout=5000)
            except:
                try:
                    await page.wait_for_selector('div.art_body', timeout=5000)
                except:
                    await page.wait_for_selector('div[class*="art_body"]', timeout=5000)
            
            # JavaScript로 데이터 추출
            content_data = await page.evaluate("""
                () => {
                    const result = {
                        published_at: '',
                        content: ''
                    };
                    
                    // 1. 발행시각 추출 (수정 시간 우선)
                    const timeContainer = document.querySelector('a[title*="기사 입력/수정일"]');
                    if (timeContainer) {
                        const paragraphs = timeContainer.querySelectorAll('p');
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
                        
                        // 수정 시간이 있으면 사용, 없으면 입력 시간 사용
                        result.published_at = modifyTime || inputTime;
                    }
                    
                    // 2. 본문 추출 (여러 선택자 시도)
                    let articleBody = document.querySelector('div.art_body#articleBody');
                    if (!articleBody) {
                        articleBody = document.querySelector('div.art_body');
                    }
                    if (!articleBody) {
                        articleBody = document.querySelector('div[class*="art_body"]');
                    }
                    if (!articleBody) {
                        // 대체 선택자들 시도
                        articleBody = document.querySelector('div[class*="article"]');
                    }
                    if (!articleBody) {
                        articleBody = document.querySelector('div[class*="content"]');
                    }
                    
                    if (articleBody) {
                        // 광고/배너 제거
                        const banners = articleBody.querySelectorAll('div[class*="banner"], div[class*="ad"], div[class*="advertisement"]');
                        banners.forEach(banner => banner.remove());
                        
                        // 본문 텍스트 추출 (여러 선택자 시도)
                        let contentParagraphs = articleBody.querySelectorAll('p.content_text.text-l');
                        if (contentParagraphs.length === 0) {
                            contentParagraphs = articleBody.querySelectorAll('p.content_text');
                        }
                        if (contentParagraphs.length === 0) {
                            contentParagraphs = articleBody.querySelectorAll('p.text-l');
                        }
                        if (contentParagraphs.length === 0) {
                            contentParagraphs = articleBody.querySelectorAll('p');
                        }
                        
                        const contentTexts = [];
                        
                        contentParagraphs.forEach(p => {
                            let text = p.textContent || '';
                            
                            // 기자명, 이메일, 출처 제거
                            text = text.replace(/[가-힣]+\s*기자/g, '');
                            text = text.replace(/[가-힣]+\s*특파원/g, '');
                            text = text.replace(/[가-힣]+\s*통신원/g, '');
                            text = text.replace(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g, '');
                            text = text.replace(/\[출처:[^\]]+\]/g, '');
                            text = text.replace(/\[경향신문\]/g, '');
                            
                            text = text.trim();
                            if (text && text.length > 10) {  // 너무 짧은 텍스트 제외
                                contentTexts.push(text);
                            }
                        });
                        
                        result.content = contentTexts.join('\\n\\n');
                    } else {
                        // 디버깅을 위해 페이지 구조 확인
                        console.log('본문 영역을 찾을 수 없습니다. 사용 가능한 div들:');
                        const allDivs = document.querySelectorAll('div[class*="art"], div[class*="article"], div[class*="content"]');
                        allDivs.forEach(div => {
                            console.log('클래스:', div.className, 'ID:', div.id);
                        });
                    }
                    
                    return result;
                }
            """)
            
            return content_data
            
        except Exception as e:
            console.print(f"❌ 기사 내용 추출 실패 {url}: {str(e)}")
            return {"published_at": "", "content": ""}
    
    async def _extract_content(self, url: str):
        """기사 상세 페이지에서 제목, 발행시각, 본문을 추출합니다."""
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                await page.goto(url, wait_until='domcontentloaded', timeout=30000)
                # 본문 영역이 로드될 때까지 대기
                await page.wait_for_selector('div.art_body#articleBody', timeout=10000)
                
                # JavaScript로 데이터 추출
                content_data = await page.evaluate("""
                    () => {
                        const result = {
                            published_at: '',
                            content: ''
                        };
                        
                        // 1. 발행시각 추출 (수정 시간 우선)
                        const timeContainer = document.querySelector('a[title*="기사 입력/수정일"]');
                        if (timeContainer) {
                            const paragraphs = timeContainer.querySelectorAll('p');
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
                            
                            // 수정 시간이 있으면 사용, 없으면 입력 시간 사용
                            result.published_at = modifyTime || inputTime;
                        }
                        
                        // 2. 본문 추출
                        const articleBody = document.querySelector('div.art_body#articleBody');
                        if (articleBody) {
                            // 광고/배너 제거
                            const banners = articleBody.querySelectorAll('div[class*="banner"]');
                            banners.forEach(banner => banner.remove());
                            
                            // 본문 텍스트 추출
                            const contentParagraphs = articleBody.querySelectorAll('p.content_text.text-l');
                            const contentTexts = [];
                            
                            contentParagraphs.forEach(p => {
                                let text = p.textContent || '';
                                
                                // 기자명, 이메일, 출처 제거
                                text = text.replace(/[가-힣]+\s*기자/g, '');
                                text = text.replace(/[가-힣]+\s*특파원/g, '');
                                text = text.replace(/[가-힣]+\s*통신원/g, '');
                                text = text.replace(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g, '');
                                text = text.replace(/\[출처:[^\]]+\]/g, '');
                                text = text.replace(/\[경향신문\]/g, '');
                                
                                text = text.trim();
                                if (text && text.length > 10) {  // 너무 짧은 텍스트 제외
                                    contentTexts.push(text);
                                }
                            });
                            
                            result.content = contentTexts.join('\\n\\n');
                        }
                        
                        return result;
                    }
                """)
                
                await browser.close()
                return content_data
                
                        except Exception as e:
            console.print(f"❌ 기사 내용 추출 실패 {url}: {str(e)}")
            return {"published_at": "", "content": ""}
    
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

async def main():
    collector = KhanPoliticsCollector()
    await collector.run(num_pages=10)  # 10페이지에서 각각 10개씩 총 100개 기사 수집

if __name__ == "__main__":
    asyncio.run(main())