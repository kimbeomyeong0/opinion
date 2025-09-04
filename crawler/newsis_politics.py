#!/usr/bin/env python3
"""
뉴시스 정치 기사 크롤러 (병렬처리 최적화 버전)
"""
import asyncio
import sys
import os
import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from datetime import datetime
import pytz
from rich.console import Console

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_manager import SupabaseManager

console = Console()

BASE_URL = "https://www.newsis.com"
LIST_URL = "https://www.newsis.com/pol/list/"


class NewsisPoliticsCollector:
    def __init__(self):
        self.articles = []
        self.semaphore = asyncio.Semaphore(5)  # 동시 처리 제한
        self.supabase_manager = SupabaseManager()

    async def run(self, num_pages=1):
        console.print("🚀 뉴시스 정치 기사 크롤링 시작")
        await self.collect_articles(num_pages)
        await self.collect_contents_parallel()  # 병렬 처리!
        await self.save_articles()  # DB 저장
        console.print("🎉 완료")

    async def collect_articles(self, num_pages=1):
        for page in range(1, num_pages + 1):
            url = f"{LIST_URL}?cid=10300&scid=10301&page={page}"
            console.print(f"📡 목록 요청: {url}")

            async with httpx.AsyncClient() as client:
                r = await client.get(url)
                soup = BeautifulSoup(r.text, "html.parser")

                for el in soup.select(".txtCont")[:5]:  # 앞에서 5개만
                    a = el.select_one(".tit a")
                    if not a:
                        continue

                    title = a.get_text(strip=True)
                    href = a["href"]
                    if href.startswith("/"):
                        href = BASE_URL + href

                    self.articles.append(
                        {"title": title, "url": href, "content": "", "published_at": ""}
                    )
                    console.print(f"📰 {title[:50]}...")

    async def collect_contents_parallel(self):
        """병렬 처리로 본문 수집"""
        console.print(f"📖 {len(self.articles)}개 기사 병렬 본문 수집 시작...")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            # 병렬 작업 실행
            tasks = []
            for i, article in enumerate(self.articles):
                task = self._extract_single_content(browser, article, i + 1)
                tasks.append(task)
            
            # 모든 작업을 병렬로 실행
            await asyncio.gather(*tasks, return_exceptions=True)
            
            await browser.close()

    async def _extract_single_content(self, browser, article, index):
        """개별 기사 본문 추출 (세마포어로 동시 실행 제한)"""
        async with self.semaphore:  # 동시 실행 수 제한
            page = None
            try:
                page = await browser.new_page()
                console.print(f"📖 [{index}] 시작: {article['title'][:40]}...")
                
                # 페이지 로드 (타임아웃 단축)
                await page.goto(article["url"], wait_until="domcontentloaded", timeout=20000)

                # 발행 시간 추출
                try:
                    time_text = await page.inner_text("span:has-text('수정')", timeout=5000)
                    time_str = time_text.replace("수정", "").strip()
                    dt = datetime.strptime(time_str, "%Y.%m.%d %H:%M:%S")
                    kst = pytz.timezone("Asia/Seoul")
                    article["published_at"] = kst.localize(dt).astimezone(pytz.UTC).isoformat()
                except Exception:
                    article["published_at"] = datetime.now(pytz.UTC).isoformat()

                # 본문 추출
                content = await page.evaluate(
                    """
                    () => {
                        const article = document.querySelector("article");
                        if (!article) return "";

                        // 광고/이미지/불필요 요소 제거
                        article.querySelectorAll("iframe, script, .banner, img, .ad").forEach(el => el.remove());

                        // 순수 텍스트만 추출
                        const text = article.innerText || article.textContent || "";
                        
                        // 기자명, 이메일 등 제거
                        return text
                            .replace(/[가-힣]+\\s*기자/g, '')
                            .replace(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/g, '')
                            .replace(/\\[뉴시스\\]/g, '')
                            .trim();
                    }
                """
                )
                
                article["content"] = content
                console.print(f"✅ [{index}] 완료: {len(content)}자")
                
            except Exception as e:
                console.print(f"❌ [{index}] 실패: {str(e)[:50]}...")
                article["content"] = ""
                article["published_at"] = datetime.now(pytz.UTC).isoformat()
                
            finally:
                if page:
                    await page.close()

    async def save_articles(self):
        """수집한 기사들을 데이터베이스에 저장"""
        console.print(f"💾 Supabase에 {len(self.articles)}개 기사 저장 중...")
        
        # 미디어 아웃렛 ID 가져오기 또는 생성
        media_outlet = self.supabase_manager.get_media_outlet("뉴시스")
        if media_outlet:
            media_id = media_outlet['id']
        else:
            media_id = self.supabase_manager.create_media_outlet("뉴시스")
        
        # 기존 URL 목록 가져오기 (중복 체크용)
        existing_urls = set()
        try:
            result = self.supabase_manager.client.table('articles').select('url').eq('media_id', media_id).execute()
            existing_urls = {article['url'] for article in result.data}
        except Exception as e:
            console.print(f"⚠️ 기존 URL 조회 실패: {str(e)}")
        
        success_count = 0
        skip_count = 0
        
        for i, article in enumerate(self.articles, 1):
            try:
                # 중복 체크
                if article['url'] in existing_urls:
                    console.print(f"⚠️ [{i}/{len(self.articles)}] 중복 기사 스킵: {article['title'][:50]}...")
                    skip_count += 1
                    continue
                
                # 기사 데이터 구성
                article_data = {
                    'title': article['title'],
                    'url': article['url'],
                    'content': article.get('content', ''),
                    'published_at': article.get('published_at', datetime.now(pytz.UTC).isoformat()),
                    'created_at': datetime.now(pytz.UTC).isoformat(),
                    'media_id': media_id
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
        
        console.print(f"\n📊 저장 결과:")
        console.print(f"  ✅ 성공: {success_count}개")
        console.print(f"  ⚠️ 중복 스킵: {skip_count}개")
        total_processed = success_count + skip_count
        success_rate = (success_count / total_processed) * 100 if total_processed > 0 else 0
        console.print(f"  📈 성공률: {success_rate:.1f}%")


# 더 빠른 버전: httpx만 사용 (Playwright 없이)
class NewsisFastCollector:
    """httpx만 사용하는 초고속 버전"""
    
    def __init__(self):
        self.articles = []
        self.supabase_manager = SupabaseManager()

    async def run(self, num_pages=1):
        console.print("🚀 뉴시스 초고속 크롤링 시작")
        await self.collect_articles(num_pages)
        await self.collect_contents_httpx_only()
        await self.save_articles()  # DB 저장
        console.print("🎉 완료")

    async def collect_articles(self, num_pages=1):
        async with httpx.AsyncClient() as client:
            for page in range(1, num_pages + 1):
                url = f"{LIST_URL}?cid=10300&scid=10301&page={page}"
                console.print(f"📡 목록 요청: {url}")

                r = await client.get(url)
                soup = BeautifulSoup(r.text, "html.parser")

                for el in soup.select(".txtCont")[:5]:
                    a = el.select_one(".tit a")
                    if not a:
                        continue

                    title = a.get_text(strip=True)
                    href = a["href"]
                    if href.startswith("/"):
                        href = BASE_URL + href

                    self.articles.append({"title": title, "url": href})
                    console.print(f"📰 {title[:50]}...")

    async def collect_contents_httpx_only(self):
        """httpx만으로 병렬 본문 수집 - 초고속!"""
        console.print(f"📖 {len(self.articles)}개 기사 초고속 병렬 수집...")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            tasks = []
            for i, article in enumerate(self.articles):
                task = self._extract_with_httpx(client, article, i + 1)
                tasks.append(task)
            
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _extract_with_httpx(self, client, article, index):
        """httpx로 HTML 파싱만으로 초고속 추출"""
        try:
            console.print(f"📖 [{index}] 시작: {article['title'][:40]}...")
            
            response = await client.get(article["url"])
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 발행시간 추출
            time_elem = soup.select_one("span:-soup-contains('수정')")
            if time_elem:
                time_str = time_elem.get_text().replace("수정", "").strip()
                try:
                    dt = datetime.strptime(time_str, "%Y.%m.%d %H:%M:%S")
                    kst = pytz.timezone("Asia/Seoul")
                    article["published_at"] = kst.localize(dt).astimezone(pytz.UTC).isoformat()
                except:
                    article["published_at"] = datetime.now(pytz.UTC).isoformat()
            else:
                article["published_at"] = datetime.now(pytz.UTC).isoformat()
            
            # 본문 추출
            article_elem = soup.select_one("article")
            if article_elem:
                # 불필요한 태그 제거
                for tag in article_elem.find_all(["script", "iframe", "img"]):
                    tag.decompose()
                
                content = article_elem.get_text(separator='\n', strip=True)
                # 정리
                content = content.replace('기자', '').replace('[뉴시스]', '').strip()
                article["content"] = content
            else:
                article["content"] = ""
            
            console.print(f"✅ [{index}] 완료: {len(article.get('content', ''))}자")
            
        except Exception as e:
            console.print(f"❌ [{index}] 실패: {str(e)[:50]}...")
            article["content"] = ""
            article["published_at"] = datetime.now(pytz.UTC).isoformat()

    async def save_articles(self):
        """수집한 기사들을 데이터베이스에 저장"""
        console.print(f"💾 Supabase에 {len(self.articles)}개 기사 저장 중...")
        
        # 미디어 아웃렛 ID 가져오기 또는 생성
        media_outlet = self.supabase_manager.get_media_outlet("뉴시스")
        if media_outlet:
            media_id = media_outlet['id']
        else:
            media_id = self.supabase_manager.create_media_outlet("뉴시스")
        
        # 기존 URL 목록 가져오기 (중복 체크용)
        existing_urls = set()
        try:
            result = self.supabase_manager.client.table('articles').select('url').eq('media_id', media_id).execute()
            existing_urls = {article['url'] for article in result.data}
        except Exception as e:
            console.print(f"⚠️ 기존 URL 조회 실패: {str(e)}")
        
        success_count = 0
        skip_count = 0
        
        for i, article in enumerate(self.articles, 1):
            try:
                # 중복 체크
                if article['url'] in existing_urls:
                    console.print(f"⚠️ [{i}/{len(self.articles)}] 중복 기사 스킵: {article['title'][:50]}...")
                    skip_count += 1
                    continue
                
                # 기사 데이터 구성
                article_data = {
                    'title': article['title'],
                    'url': article['url'],
                    'content': article.get('content', ''),
                    'published_at': article.get('published_at', datetime.now(pytz.UTC).isoformat()),
                    'created_at': datetime.now(pytz.UTC).isoformat(),
                    'media_id': media_id
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
        
        console.print(f"\n📊 저장 결과:")
        console.print(f"  ✅ 성공: {success_count}개")
        console.print(f"  ⚠️ 중복 스킵: {skip_count}개")
        total_processed = success_count + skip_count
        success_rate = (success_count / total_processed) * 100 if total_processed > 0 else 0
        console.print(f"  📈 성공률: {success_rate:.1f}%")


async def main():
    console.print("선택하세요:")
    console.print("1. 병렬 처리 버전 (Playwright)")
    console.print("2. 초고속 버전 (httpx만 사용)")
    
    # 기본적으로 초고속 버전 실행
    collector = NewsisFastCollector()
    await collector.run(num_pages=1)
    
    # 결과 출력
    for i, art in enumerate(collector.articles, 1):
        console.print(f"\n=== 기사 {i} ===")
        console.print(f"제목: {art['title']}")
        console.print(f"URL: {art['url']}")
        console.print(f"발행시간: {art.get('published_at', 'N/A')}")
        console.print(f"본문 길이: {len(art.get('content', ''))}자")
        if art.get('content'):
            console.print(f"본문 미리보기: {art['content'][:100]}...")


if __name__ == "__main__":
    asyncio.run(main())