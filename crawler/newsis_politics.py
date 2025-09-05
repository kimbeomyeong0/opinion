#!/usr/bin/env python3
"""
뉴시스 정치 기사 크롤러 (본문 추출 개선 버전)
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
import re

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

                for el in soup.select(".txtCont")[:20]:  # 앞에서 20개만
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

                # 발행 시간 추출 - 메타 태그에서 추출
                try:
                    # article:published_time 메타 태그에서 추출
                    published_time = await page.get_attribute('meta[property="article:published_time"]', 'content')
                    if published_time:
                        # ISO 8601 형식 파싱 (예: 2025-09-05T13:47:51+09:00)
                        dt = datetime.fromisoformat(published_time.replace('Z', '+00:00'))
                        article["published_at"] = dt.astimezone(pytz.UTC).isoformat()
                        console.print(f"📅 [{index}] 발행시간: {published_time} -> {article['published_at']}")
                    else:
                        # 대안: 등록 시간에서 추출
                        time_text = await page.inner_text("span:has-text('등록')", timeout=5000)
                        time_str = time_text.replace("등록", "").strip()
                        dt = datetime.strptime(time_str, "%Y.%m.%d %H:%M:%S")
                        kst = pytz.timezone("Asia/Seoul")
                        article["published_at"] = kst.localize(dt).astimezone(pytz.UTC).isoformat()
                        console.print(f"📅 [{index}] 등록시간: {time_str} -> {article['published_at']}")
                except Exception as e:
                    console.print(f"⚠️ [{index}] 발행시간 추출 실패: {str(e)[:50]}...")
                    # 발행시간을 찾을 수 없는 경우, 기본값으로 설정
                    article["published_at"] = "2025-01-01T00:00:00Z"

                # 본문 추출 (개선된 로직)
                content = await page.evaluate(
                    """
                    () => {
                        const article = document.querySelector("article");
                        if (!article) return "";

                        // 불필요한 요소들 제거
                        const elementsToRemove = [
                            'div.summury',           // 요약 부분
                            'div#textBody',          // textBody div 전체
                            'iframe',                // 광고 iframe
                            'script',                // 스크립트
                            'div#view_ad',          // 광고
                            'div.thumCont img',     // 이미지
                            'p.photojournal'        // 사진 설명
                        ];
                        
                        elementsToRemove.forEach(selector => {
                            article.querySelectorAll(selector).forEach(el => el.remove());
                        });

                        // article 내용을 가져온 후 HTML 태그를 텍스트로 변환
                        let content = article.innerHTML;
                        
                        // <br> 태그를 개행문자로 변환
                        content = content.replace(/<br\s*\/?>/gi, '\\n');
                        
                        // 다른 HTML 태그들 제거
                        content = content.replace(/<[^>]*>/g, '');
                        
                        // HTML 엔티티 디코딩
                        const tempDiv = document.createElement('div');
                        tempDiv.innerHTML = content;
                        content = tempDiv.textContent || tempDiv.innerText || '';
                        
                        // 정리 작업
                        content = content
                            .replace(/\\n\\s*\\n/g, '\\n')  // 연속된 개행문자 제거
                            .replace(/^\\s+|\\s+$/g, '')    // 앞뒤 공백 제거
                            .replace(/\\t+/g, ' ')          // 탭을 공백으로
                            .replace(/\\s+/g, ' ')          // 연속된 공백을 하나로
                            .replace(/\\n /g, '\\n')        // 개행 후 공백 제거
                            .trim();
                        
                        return content;
                    }
                """
                )
                
                # 추가 정리 작업 (Python에서)
                if content:
                    # 기자명, 이메일 등 정리
                    content = re.sub(r'[가-힣]+\s*기자\s*=?\s*', '', content)
                    content = re.sub(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '', content)
                    content = re.sub(r'\[뉴시스\]', '', content)
                    content = re.sub(r'◎공감언론\s*뉴시스.*', '', content)
                    content = re.sub(r'\*재판매.*', '', content)
                    content = content.strip()
                
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


# 더 빠른 버전: httpx만 사용 (개선된 파싱)
class NewsisFastCollector:
    """httpx만 사용하는 초고속 버전 (개선된 본문 추출)"""
    
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

                for el in soup.select(".txtCont")[:20]:
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
        """httpx만으로 병렬 본문 수집 - 개선된 파싱!"""
        console.print(f"📖 {len(self.articles)}개 기사 초고속 병렬 수집...")
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            tasks = []
            for i, article in enumerate(self.articles):
                task = self._extract_with_httpx(client, article, i + 1)
                tasks.append(task)
            
            await asyncio.gather(*tasks, return_exceptions=True)

    def _clean_content(self, content):
        """본문 텍스트 정리 함수"""
        if not content:
            return ""
        
        # 기자명, 이메일 등 제거
        content = re.sub(r'[가-힣]+\s*기자\s*=?\s*', '', content)
        content = re.sub(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '', content)
        content = re.sub(r'\[뉴시스\]', '', content)
        content = re.sub(r'◎공감언론\s*뉴시스.*', '', content)
        content = re.sub(r'\*재판매.*', '', content)
        content = re.sub(r'photo@newsis\.com.*', '', content)
        
        # 연속된 공백과 개행 정리
        content = re.sub(r'\n\s*\n', '\n', content)
        content = re.sub(r'\s+', ' ', content)
        content = content.strip()
        
        return content

    async def _extract_with_httpx(self, client, article, index):
        """httpx로 HTML 파싱 - 개선된 본문 추출"""
        try:
            console.print(f"📖 [{index}] 시작: {article['title'][:40]}...")
            
            response = await client.get(article["url"])
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 발행시간 추출 - 메타 태그에서 추출
            published_time = None
            
            # 1. article:published_time 메타 태그에서 추출
            meta_elem = soup.select_one('meta[property="article:published_time"]')
            if meta_elem:
                published_time = meta_elem.get('content', '')
                console.print(f"📅 [{index}] 메타 태그에서 발행시간 발견: {published_time}")
            
            # 2. 대안: 등록 시간에서 추출
            if not published_time:
                for span in soup.find_all('span'):
                    if span.get_text() and '등록' in span.get_text():
                        time_str = span.get_text().replace("등록", "").strip()
                        published_time = time_str
                        console.print(f"📅 [{index}] 등록 시간에서 발견: {time_str}")
                        break
            
            if published_time:
                try:
                    if published_time.startswith('2025'):  # ISO 8601 형식
                        # ISO 8601 형식 파싱 (예: 2025-09-05T13:47:51+09:00)
                        dt = datetime.fromisoformat(published_time.replace('Z', '+00:00'))
                        article["published_at"] = dt.astimezone(pytz.UTC).isoformat()
                    else:
                        # 일반 형식 파싱 (예: 2025.09.05 13:47:51)
                        dt = datetime.strptime(published_time, "%Y.%m.%d %H:%M:%S")
                        kst = pytz.timezone("Asia/Seoul")
                        article["published_at"] = kst.localize(dt).astimezone(pytz.UTC).isoformat()
                    console.print(f"📅 [{index}] 발행시간: {published_time} -> {article['published_at']}")
                except Exception as e:
                    console.print(f"⚠️ [{index}] 시간 파싱 실패: {str(e)}")
                    article["published_at"] = "2025-01-01T00:00:00Z"
            else:
                console.print(f"⚠️ [{index}] 시간 정보를 찾을 수 없음")
                article["published_at"] = "2025-01-01T00:00:00Z"
            
            # 본문 추출 - 개선된 로직
            article_elem = soup.select_one("article")
            if article_elem:
                # 불필요한 요소들 제거
                for selector in [
                    'div.summury',      # 요약
                    'div#textBody',     # textBody div 전체
                    'iframe',           # 광고
                    'script',           # 스크립트
                    'div#view_ad',      # 광고
                    'img',              # 이미지
                    'p.photojournal'    # 사진 설명
                ]:
                    for elem in article_elem.select(selector):
                        elem.decompose()
                
                # article의 텍스트 추출 (br 태그 고려)
                # br 태그를 개행문자로 변환
                for br in article_elem.find_all('br'):
                    br.replace_with('\n')
                
                # 텍스트 추출
                content = article_elem.get_text(separator=' ', strip=True)
                
                # 정리 작업
                content = self._clean_content(content)
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
    console.print("🚀 뉴시스 초고속 크롤링 시작 (httpx만 사용)")
    collector = NewsisFastCollector()
    await collector.run(num_pages=10)
    
    # 결과 출력
    console.print(f"\n📋 수집된 기사 {len(collector.articles)}개:")
    for i, art in enumerate(collector.articles, 1):
        console.print(f"\n=== 기사 {i} ===")
        console.print(f"제목: {art['title']}")
        console.print(f"URL: {art['url']}")
        console.print(f"발행시간: {art.get('published_at', 'N/A')}")
        console.print(f"본문 길이: {len(art.get('content', ''))}자")
        if art.get('content'):
            preview = art['content'][:200].replace('\n', ' ')
            console.print(f"본문 미리보기: {preview}...")


if __name__ == "__main__":
    asyncio.run(main())