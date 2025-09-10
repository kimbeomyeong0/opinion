#!/usr/bin/env python3
"""
중앙일보 정치 기사 크롤러 (성능 최적화 버전)
개선사항:
- 동시성 처리로 성능 대폭 개선
- 배치 DB 저장으로 효율성 향상
- httpx 기반으로 전환하여 속도 향상
- 연결 풀 최적화
"""

import asyncio
import httpx
from datetime import datetime
from urllib.parse import urljoin
from rich.console import Console
import pytz
from bs4 import BeautifulSoup
import re
from typing import List, Dict, Optional

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.supabase_manager import SupabaseManager

console = Console()

class JoongangPoliticsCollector:
    """중앙일보 정치 기사 수집기 (성능 최적화)"""
    
    def __init__(self):
        self.media_name = "중앙일보"
        self.base_url = "https://www.joongang.co.kr"
        self.articles = []
        self.supabase_manager = SupabaseManager()
        
        # HTTP 클라이언트 설정 (최적화)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        
        # 동시성 제한 설정
        self.semaphore = asyncio.Semaphore(10)  # 최대 10개 동시 요청
        self.batch_size = 20  # DB 배치 저장 크기
        
    async def _get_page_articles(self, page_num: int) -> list:
        """특정 페이지에서 기사 목록 수집"""
        try:
            url = f"{self.base_url}/politics?page={page_num}"
            console.print(f"📡 페이지 수집: {url}")
            
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 무시해야 하는 영역 제거
                showcase = soup.find('section', class_='showcase_general')
                if showcase:
                    showcase.decompose()
                    console.print("🗑️ showcase_general 영역 제거")
                
                rank_list = soup.find('ul', class_='card_right_list rank_list')
                if rank_list:
                    rank_list.decompose()
                    console.print("🗑️ rank_list 영역 제거")
                
                # 수집 대상: <ul id="story_list"> 안의 <li class="card">
                story_list = soup.find('ul', id='story_list')
                if not story_list:
                    console.print("❌ story_list를 찾을 수 없습니다")
                    return []
                
                cards = story_list.find_all('li', class_='card')
                console.print(f"🔍 story_list에서 {len(cards)}개 카드 발견")
                
                articles = []
                max_articles_per_page = 24  # 각 페이지에서 24개 수집
                collected_count = 0
                
                for i, card in enumerate(cards):
                    if collected_count >= max_articles_per_page:
                        break
                        
                    try:
                        # 제목과 URL 추출
                        headline = card.find('h2', class_='headline')
                        if not headline:
                            continue
                            
                        link = headline.find('a')
                        if not link:
                            continue
                        
                        title = link.get_text(strip=True)
                        article_url = link.get('href', '')
                        
                        if title and article_url:
                            # 상대 URL을 절대 URL로 변환
                            if article_url.startswith('/'):
                                full_url = urljoin(self.base_url, article_url)
                            else:
                                full_url = article_url
                            
                            articles.append({
                                'title': title,
                                'url': full_url
                            })
                            collected_count += 1
                            console.print(f"📰 기사 발견 [{collected_count}]: {title[:50]}...")
                    
                    except Exception as e:
                        console.print(f"⚠️ 카드 [{i}] 처리 중 오류: {e}")
                        continue
                
                console.print(f"📊 페이지에서 {len(articles)}개 기사 발견")
                return articles
                
        except Exception as e:
            console.print(f"❌ 페이지 수집 실패: {e}")
            return []
    
    async def _extract_content(self, url: str) -> dict:
        """기사 본문 및 발행시간 추출"""
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                await page.goto(url, wait_until='domcontentloaded', timeout=60000)
                
                # 기사 본문 및 발행시간 추출
                content_data = await page.evaluate("""
                    () => {
                        // 발행시간 추출 - <time itemprop="datePublished">의 datetime 속성 사용
                        let published_at = '';
                        
                        // 1. time[itemprop="datePublished"] datetime 속성 시도
                        const timeElement = document.querySelector('time[itemprop="datePublished"]');
                        if (timeElement) {
                            published_at = timeElement.getAttribute('datetime');
                        }
                        
                        // 2. 다른 가능한 시간 선택자들 시도
                        if (!published_at) {
                            const timeSelectors = [
                                'time[datetime]',
                                'button.btn_datetime span',
                                '.article_info .date',
                                '.article_info .time',
                                '.date_info',
                                '.article_date',
                                '.publish_date'
                            ];
                            
                            for (const selector of timeSelectors) {
                                const element = document.querySelector(selector);
                                if (element) {
                                    // datetime 속성이 있으면 우선 사용
                                    const datetime = element.getAttribute('datetime');
                                    if (datetime) {
                                        published_at = datetime;
                                        break;
                                    }
                                    
                                    // 없으면 텍스트에서 날짜 형식 찾기
                                    const text = element.textContent || element.innerText || '';
                                    const trimmed = text.trim();
                                    if (trimmed.match(/\\d{4}-\\d{2}-\\d{2}/) || 
                                        trimmed.match(/\\d{4}\\.\\d{2}\\.\\d{2}/) ||
                                        trimmed.match(/\\d{2}:\\d{2}/)) {
                                        published_at = trimmed;
                                        break;
                                    }
                                }
                            }
                        }
                        
                        // 본문 영역 찾기
                        const articleBody = document.getElementById('article_body');
                        if (!articleBody) return { content: '', published_at: published_at };
                        
                        // 광고 영역 제거
                        const adElements = articleBody.querySelectorAll('#ad_art_content_mid, .ad, .advertisement');
                        adElements.forEach(el => el.remove());
                        
                        // <p> 태그들의 텍스트만 추출
                        const paragraphs = articleBody.querySelectorAll('p');
                        const contentLines = [];
                        
                        paragraphs.forEach(p => {
                            const text = p.textContent || p.innerText || '';
                            const trimmedText = text.trim();
                            
                            // 기자명/출처 부분 제거
                            if (trimmedText && 
                                !trimmedText.includes('기자') && 
                                !trimmedText.includes('@') &&
                                !trimmedText.includes('[출처:') &&
                                !trimmedText.includes('출처:') &&
                                !trimmedText.includes('정재홍') &&
                                !trimmedText.includes('hongj@joongang.co.kr') &&
                                trimmedText.length > 10) {
                                contentLines.push(trimmedText);
                            }
                        });
                        
                        // 각 문단을 개행으로 구분하여 결합
                        const content = contentLines.join('\\n\\n');
                        
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
            # 발행 시간 처리 (기사 실제 발행시간)
            published_at_str = content_data.get('published_at', '')
            
            if published_at_str and published_at_str.strip():
                try:
                    clean_time = published_at_str.strip()
                    
                    # "업데이트 정보 더보기" 같은 텍스트 제거
                    if '업데이트' in clean_time or '더보기' in clean_time:
                        clean_time = ''
                    
                    if clean_time:
                        if 'T' in clean_time and '+' in clean_time:
                            # ISO 형식 with timezone (예: "2025-09-05T01:17:00+09:00")
                            published_at = datetime.fromisoformat(clean_time)
                            # UTC로 변환
                            published_at = published_at.astimezone(pytz.UTC)
                        elif 'T' in clean_time:
                            # ISO 형식 without timezone (UTC로 가정)
                            published_at = datetime.fromisoformat(clean_time.replace('Z', '+00:00'))
                        elif '-' in clean_time and ':' in clean_time:
                            # "YYYY-MM-DD HH:MM" 형식인 경우 (KST 기준)
                            published_at = datetime.strptime(clean_time, "%Y-%m-%d %H:%M")
                            kst = pytz.timezone("Asia/Seoul")
                            published_at = kst.localize(published_at).astimezone(pytz.UTC)
                        elif '.' in clean_time and ':' in clean_time:
                            # "YYYY.MM.DD HH:MM" 형식인 경우 (KST 기준)
                            published_at = datetime.strptime(clean_time, "%Y.%m.%d %H:%M")
                            kst = pytz.timezone("Asia/Seoul")
                            published_at = kst.localize(published_at).astimezone(pytz.UTC)
                        else:
                            # 다른 형식 시도
                            console.print(f"⚠️ 알 수 없는 시간 형식: {clean_time}")
                            published_at = datetime.now(pytz.UTC)
                    else:
                        published_at = datetime.now(pytz.UTC)
                        
                except Exception as e:
                    console.print(f"⚠️ 발행시간 파싱 실패: {published_at_str} - {e}")
                    published_at = datetime.now(pytz.UTC)
            else:
                published_at = datetime.now(pytz.UTC)
            
            # 생성 시간 (크롤링 시점의 현재 시각)
            created_at = datetime.now(pytz.UTC)
            
            return {
                'title': article['title'],
                'url': article['url'],
                'content': content_data.get('content', ''),
                'published_at': published_at.isoformat(),
                'created_at': created_at.isoformat(),
                'media_outlet': self.media_name
            }
            
        except Exception as e:
            console.print(f"❌ 기사 데이터 파싱 실패: {e}")
            return None
    
    async def collect_articles_parallel(self, num_pages: int = 7):
        """기사 수집 (병렬 처리)"""
        console.print(f"📄 {num_pages}개 페이지에서 기사 수집 시작 (병렬 처리)...")
        
        # 모든 페이지를 동시에 처리
        tasks = [self._collect_page_articles_parallel(page_num) for page_num in range(1, num_pages + 1)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 수집
        total_articles = 0
        for i, result in enumerate(results, 1):
            if isinstance(result, Exception):
                console.print(f"❌ 페이지 {i} 처리 중 오류: {str(result)}")
            else:
                total_articles += result
                
        console.print(f"📊 총 {total_articles}개 기사 수집")

    async def _collect_page_articles_parallel(self, page_num: int) -> int:
        """단일 페이지에서 기사 수집 (병렬 처리용)"""
        url = f"{self.base_url}/politics?page={page_num}"
        console.print(f"📡 페이지 {page_num}: {url}")

        async with self.semaphore:  # 동시성 제한
            try:
                async with httpx.AsyncClient(
                    timeout=15.0,
                    follow_redirects=True,
                    limits=httpx.Limits(max_keepalive_connections=20, max_connections=50)
                ) as client:
                    response = await client.get(url, headers=self.headers)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # 무시해야 하는 영역 제거
                    showcase = soup.find('section', class_='showcase_general')
                    if showcase:
                        showcase.decompose()
                    
                    rank_list = soup.find('ul', class_='card_right_list rank_list')
                    if rank_list:
                        rank_list.decompose()
                    
                    # 수집 대상: <ul id="story_list"> 안의 <li class="card">
                    story_list = soup.find('ul', id='story_list')
                    if not story_list:
                        console.print(f"❌ 페이지 {page_num}: story_list를 찾을 수 없습니다")
                        return 0
                    
                    cards = story_list.find_all('li', class_='card')
                    console.print(f"🔍 페이지 {page_num}: {len(cards)}개 카드 발견")
                    
                    articles = []
                    max_articles_per_page = 24  # 각 페이지에서 24개 수집
                    collected_count = 0
                    
                    for i, card in enumerate(cards):
                        if collected_count >= max_articles_per_page:
                            break
                            
                        try:
                            # 제목과 URL 추출
                            headline = card.find('h2', class_='headline')
                            if not headline:
                                continue
                                
                            link = headline.find('a')
                            if not link:
                                continue
                            
                            title = link.get_text(strip=True)
                            article_url = link.get('href', '')
                            
                            if title and article_url:
                                # 상대 URL을 절대 URL로 변환
                                if article_url.startswith('/'):
                                    full_url = urljoin(self.base_url, article_url)
                                else:
                                    full_url = article_url
                                
                                article = {
                                    'title': title,
                                    'url': full_url,
                                    'content': '',
                                    'published_at': ''
                                }
                                articles.append(article)
                                collected_count += 1
                                console.print(f"📰 발견: {title[:50]}...")
                        
                        except Exception as e:
                            console.print(f"⚠️ 카드 [{i}] 처리 중 오류: {e}")
                            continue
                    
                    self.articles.extend(articles)
                    console.print(f"📄 페이지 {page_num}: {len(articles)}개 기사 수집")
                    return len(articles)

            except Exception as e:
                console.print(f"❌ 페이지 {page_num} 처리 중 오류: {str(e)}")
                return 0
    
    async def collect_contents_parallel(self):
        """기사 본문 수집 (병렬 처리)"""
        console.print(f"📖 {len(self.articles)}개 기사 본문 수집 시작 (병렬 처리)...")
        
        # 모든 기사를 동시에 처리 (배치로 나누어서)
        batch_size = 20  # 한 번에 처리할 기사 수
        total_batches = (len(self.articles) + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(self.articles))
            batch_articles = self.articles[start_idx:end_idx]
            
            console.print(f"📖 배치 {batch_num + 1}/{total_batches}: {len(batch_articles)}개 기사 처리 중...")
            
            # 배치 내에서 병렬 처리
            async with httpx.AsyncClient(
                timeout=15.0,
                follow_redirects=True,
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=50)
            ) as client:
                tasks = [self._extract_content_httpx(client, article, i + start_idx + 1) for i, article in enumerate(batch_articles)]
                await asyncio.gather(*tasks, return_exceptions=True)
            
            # 배치 간 짧은 딜레이 (서버 부하 방지)
            if batch_num < total_batches - 1:
                await asyncio.sleep(0.5)

    async def _extract_content_httpx(self, client: httpx.AsyncClient, article: dict, index: int):
        """httpx로 기사 본문 및 발행시간 추출"""
        try:
            console.print(f"📖 [{index}] 시작: {article['title'][:40]}...")
            
            response = await client.get(article["url"], headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 발행시간 추출
            published_at = self._extract_published_at(soup)
            article["published_at"] = published_at
            
            # 본문 추출
            content = self._extract_content_text(soup)
            article["content"] = content
            
            console.print(f"✅ [{index}] 완료: {len(content)}자")
            
        except Exception as e:
            console.print(f"❌ [{index}] 실패: {str(e)[:50]}...")
            article["content"] = ""
            article["published_at"] = datetime.now(pytz.UTC).isoformat()

    def _extract_published_at(self, soup: BeautifulSoup) -> str:
        """발행시간 추출"""
        try:
            # 1. time[itemprop="datePublished"] datetime 속성 시도
            time_element = soup.select_one('time[itemprop="datePublished"]')
            if time_element:
                published_at = time_element.get('datetime', '')
                if published_at:
                    return self._parse_datetime(published_at)
            
            # 2. 다른 가능한 시간 선택자들 시도
            time_selectors = [
                'time[datetime]',
                'button.btn_datetime span',
                '.article_info .date',
                '.article_info .time',
                '.date_info',
                '.article_date',
                '.publish_date'
            ]
            
            for selector in time_selectors:
                element = soup.select_one(selector)
                if element:
                    # datetime 속성이 있으면 우선 사용
                    datetime_attr = element.get('datetime')
                    if datetime_attr:
                        return self._parse_datetime(datetime_attr)
                    
                    # 없으면 텍스트에서 날짜 형식 찾기
                    text = element.get_text(strip=True)
                    if text and (re.match(r'\d{4}-\d{2}-\d{2}', text) or 
                               re.match(r'\d{4}\.\d{2}\.\d{2}', text) or
                               re.match(r'\d{2}:\d{2}', text)):
                        return self._parse_datetime(text)
            
            return datetime.now(pytz.UTC).isoformat()
            
        except Exception as e:
            console.print(f"⚠️ 발행시간 추출 실패: {str(e)}")
            return datetime.now(pytz.UTC).isoformat()

    def _extract_content_text(self, soup: BeautifulSoup) -> str:
        """본문 텍스트 추출"""
        try:
            # 본문 영역 찾기
            article_body = soup.find('div', id='article_body')
            if not article_body:
                return ""
            
            # 광고 영역 제거
            ad_elements = article_body.select('#ad_art_content_mid, .ad, .advertisement')
            for el in ad_elements:
                el.decompose()
            
            # <p> 태그들의 텍스트만 추출
            paragraphs = article_body.select('p')
            content_lines = []
            
            for p in paragraphs:
                text = p.get_text(strip=True)
                
                # 기자명/출처 부분 제거
                if (text and 
                    not text.find('기자') >= 0 and 
                    not text.find('@') >= 0 and
                    not text.find('[출처:') >= 0 and
                    not text.find('출처:') >= 0 and
                    not text.find('정재홍') >= 0 and
                    not text.find('hongj@joongang.co.kr') >= 0 and
                    len(text) > 10):
                    content_lines.append(text)
            
            # 각 문단을 개행으로 구분하여 결합
            return '\n\n'.join(content_lines)
            
        except Exception as e:
            console.print(f"⚠️ 본문 추출 실패: {str(e)}")
            return ""

    def _parse_datetime(self, datetime_str: str) -> str:
        """날짜시간 문자열 파싱"""
        try:
            clean_time = datetime_str.strip()
            
            # "업데이트 정보 더보기" 같은 텍스트 제거
            if '업데이트' in clean_time or '더보기' in clean_time:
                return datetime.now(pytz.UTC).isoformat()
            
            if 'T' in clean_time and '+' in clean_time:
                # ISO 형식 with timezone (예: "2025-09-05T01:17:00+09:00")
                published_at = datetime.fromisoformat(clean_time)
                return published_at.astimezone(pytz.UTC).isoformat()
            elif 'T' in clean_time:
                # ISO 형식 without timezone (UTC로 가정)
                published_at = datetime.fromisoformat(clean_time.replace('Z', '+00:00'))
                return published_at.isoformat()
            elif '-' in clean_time and ':' in clean_time:
                # "YYYY-MM-DD HH:MM" 형식인 경우 (KST 기준)
                published_at = datetime.strptime(clean_time, "%Y-%m-%d %H:%M")
                kst = pytz.timezone("Asia/Seoul")
                return kst.localize(published_at).astimezone(pytz.UTC).isoformat()
            elif '.' in clean_time and ':' in clean_time:
                # "YYYY.MM.DD HH:MM" 형식인 경우 (KST 기준)
                published_at = datetime.strptime(clean_time, "%Y.%m.%d %H:%M")
                kst = pytz.timezone("Asia/Seoul")
                return kst.localize(published_at).astimezone(pytz.UTC).isoformat()
            else:
                return datetime.now(pytz.UTC).isoformat()
                
        except Exception as e:
            console.print(f"⚠️ 날짜 파싱 실패: {clean_time} - {str(e)}")
            return datetime.now(pytz.UTC).isoformat()
    
    async def save_articles_batch(self):
        """DB 배치 저장 (최적화)"""
        if not self.articles:
            console.print("⚠️ 저장할 기사가 없습니다.")
            return
            
        console.print(f"💾 Supabase에 {len(self.articles)}개 기사 배치 저장 중...")

        try:
            # 언론사 확인
            media = self.supabase_manager.get_media_outlet(self.media_name)
            if not media:
                media_id = self.supabase_manager.create_media_outlet(
                    name=self.media_name,
                    bias="center-right",
                    website=self.base_url
                )
            else:
                media_id = media["id"]

            # 기존 URL 가져오기 (중복 체크)
            existing_urls = set()
            try:
                result = self.supabase_manager.client.table("articles").select("url").eq("media_id", media_id).execute()
                existing_urls = {article["url"] for article in result.data}
            except Exception as e:
                console.print(f"⚠️ 기존 URL 조회 실패: {str(e)}")

            # 중복 제거 및 배치 준비
            new_articles = []
            skip_count = 0
            
            for article in self.articles:
                if article["url"] in existing_urls:
                    skip_count += 1
                    continue
                    
                # 기사 데이터 파싱
                parsed_article = self._parse_article_data_simple(article, media_id)
                if parsed_article:
                    new_articles.append(parsed_article)

            # 배치 저장
            if new_articles:
                success_count = self._batch_insert_articles(new_articles)
                console.print(f"✅ 배치 저장 완료: {success_count}개 성공")
            else:
                console.print("⚠️ 저장할 새 기사가 없습니다.")
                
            console.print(f"\n📊 저장 결과: 성공 {len(new_articles)}, 스킵 {skip_count}")
            
        except Exception as e:
            console.print(f"❌ DB 저장 중 치명적 오류: {str(e)}")

    def _parse_article_data_simple(self, article: dict, media_id: str) -> Optional[dict]:
        """기사 데이터 간단 파싱 (배치 저장용)"""
        try:
            return {
                'title': article['title'],
                'url': article['url'],
                'content': article.get('content', ''),
                'published_at': article.get('published_at', datetime.now(pytz.UTC).isoformat()),
                'created_at': datetime.now(pytz.UTC).isoformat(),
                'media_id': media_id
            }
        except Exception as e:
            console.print(f"❌ 기사 데이터 파싱 실패: {e}")
            return None

    def _batch_insert_articles(self, articles: List[Dict]) -> int:
        """배치로 기사 삽입"""
        try:
            # Supabase의 upsert 기능 사용
            result = self.supabase_manager.client.table("articles").upsert(articles).execute()
            return len(result.data) if result.data else 0
        except Exception as e:
            console.print(f"❌ 배치 저장 실패: {str(e)}")
            # 개별 저장으로 폴백
            success_count = 0
            for article in articles:
                try:
                    if self.supabase_manager.insert_article(article):
                        success_count += 1
                except:
                    continue
            return success_count
    
    async def run(self, num_pages: int = 7):
        """크롤러 실행 (최적화 버전)"""
        try:
            console.print("🚀 중앙일보 정치 기사 크롤링 시작 (최적화 버전)")
            
            # 1. 기사 목록 수집 (병렬 처리)
            await self.collect_articles_parallel(num_pages)
            
            if not self.articles:
                console.print("❌ 수집된 기사가 없습니다")
                return
            
            # 2. 기사 본문 수집 (병렬 처리)
            await self.collect_contents_parallel()
            
            # 3. 기사 저장 (배치 처리)
            await self.save_articles_batch()
            
            console.print("🎉 크롤링 완료!")
            
        except KeyboardInterrupt:
            console.print("⏹️ 사용자에 의해 중단되었습니다")
        except Exception as e:
            console.print(f"❌ 크롤링 중 오류 발생: {str(e)}")

async def main():
    collector = JoongangPoliticsCollector()
    await collector.run(num_pages=7)  # 7페이지에서 각각 24개씩 총 168개 기사 수집 (150개 목표)

if __name__ == "__main__":
    asyncio.run(main())
