#!/usr/bin/env python3
"""
동아일보 정치 기사 크롤러 (성능 최적화 버전)
개선사항:
- 동시성 처리로 성능 대폭 개선
- 배치 DB 저장으로 효율성 향상
- httpx 기반으로 전환하여 속도 향상
- 연결 풀 최적화
"""

import asyncio
import sys
import os
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin
import httpx
import pytz
from rich.console import Console
from bs4 import BeautifulSoup
import re

# 프로젝트 루트 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 내부 모듈
from utils.supabase_manager import SupabaseManager

console = Console()
KST = pytz.timezone("Asia/Seoul")


class DongaPoliticsCollector:
    def __init__(self):
        self.base_url = "https://www.donga.com"
        self.politics_url = "https://www.donga.com/news/Politics"
        self.media_name = "동아일보"
        self.media_bias = "center"
        self.supabase_manager = SupabaseManager()
        self.articles: List[Dict] = []
        
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

    def _get_page_urls(self, num_pages: int = 15) -> List[str]:
        """페이지 URL 목록 생성 (p=1, 11, 21, 31...)"""
        urls = []
        for i in range(num_pages):
            page_num = i * 10 + 1  # 1, 11, 21, 31...
            url = f"{self.politics_url}?p={page_num}&prod=news&ymd=&m="
            urls.append(url)
        return urls

    async def _get_page_articles(self, page_url: str) -> List[Dict]:
        """특정 페이지에서 기사 목록 수집"""
        console.print(f"📡 페이지 수집: {page_url}")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                resp = await client.get(page_url)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                articles = []
                
                # 동아일보 정치 섹션의 기사 링크 추출
                # 디버깅을 위해 HTML 구조 확인
                console.print("🔍 HTML 구조 분석 중...")
                
                # divide_area 찾기
                divide_area = soup.find('div', class_='divide_area')
                console.print(f"divide_area 발견: {divide_area is not None}")
                
                if divide_area:
                    # 올바른 구조: divide_area > section.sub_news_sec > ul.row_list > li > article.news_card
                    sub_news_sec = divide_area.find('section', class_='sub_news_sec')
                    console.print(f"sub_news_sec (section) 발견: {sub_news_sec is not None}")
                    
                    if sub_news_sec:
                        row_list = sub_news_sec.find('ul', class_='row_list')
                        console.print(f"row_list 발견: {row_list is not None}")
                        
                        if row_list:
                            li_items = row_list.find_all('li')
                            console.print(f"🔍 row_list에서 {len(li_items)}개 li 요소 발견")
                            
                            # 각 페이지에서 최대 10개 기사 수집
                            collected_count = 0
                            max_articles_per_page = 10
                            
                            for i, li in enumerate(li_items):
                                if collected_count >= max_articles_per_page:
                                    break
                                    
                                # li 안에서 article.news_card 찾기
                                news_card = li.find('article', class_='news_card')
                                console.print(f"li[{i}]에서 news_card 발견: {news_card is not None}")
                                
                                if news_card:
                                    # news_card 안의 구조 확인
                                    news_head = news_card.find('header', class_='news_head')
                                    news_body = news_card.find('div', class_='news_body')
                                    console.print(f"  news_head: {news_head is not None}, news_body: {news_body is not None}")
                                    
                                    # news_card 안에서 링크 찾기 (news_body의 .tit a에서)
                                    link = None
                                    
                                    # news_body의 .tit a에서 링크 찾기
                                    if news_body:
                                        tit_link = news_body.find('h4', class_='tit')
                                        if tit_link:
                                            link = tit_link.find('a', href=True)
                                            console.print(f"  .tit a에서 링크 발견: {link is not None}")
                                    
                                    # 대안: news_head에서 링크 찾기
                                    if not link and news_head:
                                        link = news_head.find('a', href=True)
                                        console.print(f"  news_head에서 링크 발견: {link is not None}")
                                    
                                    # 대안: news_card에서 직접 링크 찾기
                                    if not link:
                                        link = news_card.find('a', href=True)
                                        console.print(f"  news_card에서 직접 링크 발견: {link is not None}")
                                    
                                    if link:
                                        href = link.get('href')
                                        category = link.get('data-ep_button_category')
                                        console.print(f"  링크 href: {href}")
                                        console.print(f"  카테고리: {category}")
                                        
                                        # 정치 카테고리만 필터링 (data 속성으로)
                                        is_politics = False
                                        
                                        if href and '/news/' in href and '/article/' in href:
                                            # data 속성으로 정치 카테고리 확인
                                            if category == '정치':
                                                is_politics = True
                                                console.print(f"  data 속성으로 정치 기사 확인: {category}")
                                        
                                        if is_politics:
                                            console.print(f"  정치 카테고리 기사 발견!")
                                            # 상대 URL을 절대 URL로 변환
                                            if href.startswith('/'):
                                                full_url = urljoin(self.base_url, href)
                                            else:
                                                full_url = href
                                            
                                            # 제목 추출 (data-ep_button_name 속성 우선 사용)
                                            title = link.get('data-ep_button_name', '').strip()
                                            console.print(f"  제목 추출 시도 1 - data-ep_button_name: '{title}'")
                                            
                                            # 대안: data-ep_contentdata_content_title 속성
                                            if not title:
                                                title = link.get('data-ep_contentdata_content_title', '').strip()
                                                console.print(f"  제목 추출 시도 2 - data-ep_contentdata_content_title: '{title}'")
                                            
                                            # 대안: a 태그의 직접 텍스트 노드
                                            if not title:
                                                title_text = link.find(text=True, recursive=False)
                                                if title_text:
                                                    title = title_text.strip()
                                                console.print(f"  a 태그 직접 텍스트: '{title}'")
                                            
                                            # 대안: img 태그의 alt 속성
                                            if not title:
                                                img_tag = link.find('img')
                                                if img_tag:
                                                    title = img_tag.get('alt', '').strip()
                                                console.print(f"  img alt 속성: '{title}'")
                                            
                                            console.print(f"  최종 제목: '{title[:50]}...'")
                                            
                                            if title and len(title) > 10:  # 의미있는 제목만
                                                articles.append({
                                                    'title': title,
                                                    'url': full_url
                                                })
                                                collected_count += 1
                                                console.print(f"📰 기사 발견 [{collected_count}]: {title[:50]}...")
                                            else:
                                                if i == 0:
                                                    console.print(f"  제목이 너무 짧거나 비어있음: '{title}'")
                                else:
                                    console.print(f"  li[{i}]에서 news_card를 찾을 수 없습니다.")
                else:
                    console.print("⚠️ divide_area를 찾을 수 없습니다.")
                    # 전체 페이지에서 기사 링크 찾기
                    links = soup.find_all('a', href=True)
                    news_links = [link for link in links if link.get('href') and '/news/article/' in link.get('href')]
                    console.print(f"전체 페이지에서 {len(news_links)}개 뉴스 링크 발견")
                    
                    for link in news_links:
                        href = link.get('href')
                        if href.startswith('/'):
                            full_url = urljoin(self.base_url, href)
                        else:
                            full_url = href
                        
                        title = link.get_text(strip=True)
                        if title and len(title) > 10:
                            articles.append({
                                'title': title,
                                'url': full_url
                            })
                            console.print(f"📰 기사 발견: {title[:50]}...")
                            break
                
                console.print(f"📊 페이지에서 {len(articles)}개 기사 발견")
                return articles
                
            except Exception as e:
                console.print(f"❌ 페이지 수집 실패: {e}")
                return []

    async def collect_articles_parallel(self, num_pages: int = 15):
        """기사 수집 (병렬 처리)"""
        console.print(f"📄 {num_pages}개 페이지에서 기사 수집 시작 (병렬 처리)...")
        
        page_urls = self._get_page_urls(num_pages)
        
        # 모든 페이지를 동시에 처리
        tasks = [self._collect_page_articles_parallel(page_url, i + 1) for i, page_url in enumerate(page_urls)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 수집
        total_articles = 0
        for i, result in enumerate(results, 1):
            if isinstance(result, Exception):
                console.print(f"❌ 페이지 {i} 처리 중 오류: {str(result)}")
            else:
                total_articles += result
                
        console.print(f"📊 총 {total_articles}개 기사 수집")

    async def _collect_page_articles_parallel(self, page_url: str, page_num: int) -> int:
        """단일 페이지에서 기사 수집 (병렬 처리용)"""
        console.print(f"📡 페이지 {page_num}: {page_url}")

        async with self.semaphore:  # 동시성 제한
            try:
                async with httpx.AsyncClient(
                    timeout=15.0,
                    follow_redirects=True,
                    limits=httpx.Limits(max_keepalive_connections=20, max_connections=50)
                ) as client:
                    response = await client.get(page_url, headers=self.headers)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    articles = []
                    
                    # 동아일보 정치 섹션의 기사 링크 추출
                    divide_area = soup.find('div', class_='divide_area')
                    
                    if divide_area:
                        sub_news_sec = divide_area.find('section', class_='sub_news_sec')
                        if sub_news_sec:
                            row_list = sub_news_sec.find('ul', class_='row_list')
                            if row_list:
                                li_items = row_list.find_all('li')
                                
                                # 각 페이지에서 최대 10개 기사 수집
                                collected_count = 0
                                max_articles_per_page = 10
                                
                                for i, li in enumerate(li_items):
                                    if collected_count >= max_articles_per_page:
                                        break
                                        
                                    news_card = li.find('article', class_='news_card')
                                    
                                    if news_card:
                                        # 링크 찾기
                                        link = None
                                        
                                        # news_body의 .tit a에서 링크 찾기
                                        news_body = news_card.find('div', class_='news_body')
                                        if news_body:
                                            tit_link = news_body.find('h4', class_='tit')
                                            if tit_link:
                                                link = tit_link.find('a', href=True)
                                        
                                        # 대안: news_head에서 링크 찾기
                                        if not link:
                                            news_head = news_card.find('header', class_='news_head')
                                            if news_head:
                                                link = news_head.find('a', href=True)
                                        
                                        # 대안: news_card에서 직접 링크 찾기
                                        if not link:
                                            link = news_card.find('a', href=True)
                                        
                                        if link:
                                            href = link.get('href')
                                            category = link.get('data-ep_button_category')
                                            
                                            # 정치 카테고리만 필터링
                                            is_politics = False
                                            if href and '/news/' in href and '/article/' in href:
                                                if category == '정치':
                                                    is_politics = True
                                            
                                            if is_politics:
                                                # 상대 URL을 절대 URL로 변환
                                                if href.startswith('/'):
                                                    full_url = urljoin(self.base_url, href)
                                                else:
                                                    full_url = href
                                                
                                                # 제목 추출
                                                title = link.get('data-ep_button_name', '').strip()
                                                if not title:
                                                    title = link.get('data-ep_contentdata_content_title', '').strip()
                                                if not title:
                                                    title_text = link.find(text=True, recursive=False)
                                                    if title_text:
                                                        title = title_text.strip()
                                                if not title:
                                                    img_tag = link.find('img')
                                                    if img_tag:
                                                        title = img_tag.get('alt', '').strip()
                                                
                                                if title and len(title) > 10:
                                                    article = {
                                                        'title': title,
                                                        'url': full_url,
                                                        'content': '',
                                                        'published_at': ''
                                                    }
                                                    articles.append(article)
                                                    collected_count += 1
                                                    console.print(f"📰 발견: {title[:50]}...")
                    else:
                        # 전체 페이지에서 기사 링크 찾기
                        links = soup.find_all('a', href=True)
                        news_links = [link for link in links if link.get('href') and '/news/article/' in link.get('href')]
                        
                        for link in news_links:
                            href = link.get('href')
                            if href.startswith('/'):
                                full_url = urljoin(self.base_url, href)
                            else:
                                full_url = href
                            
                            title = link.get_text(strip=True)
                            if title and len(title) > 10:
                                article = {
                                    'title': title,
                                    'url': full_url,
                                    'content': '',
                                    'published_at': ''
                                }
                                articles.append(article)
                                console.print(f"📰 발견: {title[:50]}...")
                                break
                    
                    self.articles.extend(articles)
                    console.print(f"📄 페이지 {page_num}: {len(articles)}개 기사 수집")
                    return len(articles)

            except Exception as e:
                console.print(f"❌ 페이지 {page_num} 처리 중 오류: {str(e)}")
                return 0

    def _parse_article_data(self, article_data: Dict) -> Optional[Dict]:
        """기사 데이터 파싱"""
        try:
            title = article_data.get("title")
            url = article_data.get("url")
            content_data = article_data.get("content_data", {})
            
            if not title or not url:
                return None

            # 발행 시간 파싱 및 UTC 변환
            published_at = None
            if content_data.get("published_at"):
                try:
                    # KST 시간 파싱
                    kst_time = datetime.strptime(content_data["published_at"], "%Y-%m-%d %H:%M")
                    # KST 타임존 적용
                    kst_tz = pytz.timezone("Asia/Seoul")
                    kst_dt = kst_tz.localize(kst_time)
                    # UTC로 변환
                    published_at = kst_dt.astimezone(pytz.UTC).isoformat()
                except Exception as e:
                    console.print(f"⚠️ 발행 시간 파싱 실패: {e}")
                    published_at = None

            return {
                "title": title.strip(),
                "url": url,
                "content": content_data.get("content", ""),
                "published_at": published_at,
                "created_at": datetime.now(KST).isoformat(),
                "author": "",  # 나중에 본문에서 추출
                "section": "정치",
                "tags": [],
                "description": "",
            }
        except Exception as e:
            console.print(f"❌ 데이터 파싱 실패: {e}")
            return None

    async def _extract_content(self, url: str) -> Dict[str, str]:
        """Playwright로 본문 전문 추출"""
        page = None
        try:
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
            await page.set_viewport_size({"width": 1280, "height": 720})
            await page.goto(url, wait_until="domcontentloaded", timeout=10000)

            # 동아일보 본문 및 발행 시간 추출
            result = {"content": "", "published_at": ""}
            
            try:
                result = await page.evaluate('''() => {
                    // <section class="news_view"> 찾기
                    const newsView = document.querySelector('section.news_view');
                    if (!newsView) {
                        return {content: '', published_at: ''};
                    }
                    
                    // 발행 시간 추출 (<span aria-hidden="true">2025-09-04 15:33</span>)
                    let publishedAt = '';
                    const timeSpan = document.querySelector('span[aria-hidden="true"]');
                    if (timeSpan) {
                        const timeText = timeSpan.textContent.trim();
                        // YYYY-MM-DD HH:MM 형식인지 확인
                        if (/^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}$/.test(timeText)) {
                            publishedAt = timeText;
                        }
                    }
                    
                    // 제외할 요소들 제거
                    const excludeSelectors = [
                        'h2.sub_tit',  // 기사 제목
                        'figure',      // 이미지 관련
                        'img',         // 이미지
                        'figcaption',  // 이미지 캡션
                        '.view_m_adK', // 광고 영역
                        '.view_ad06',  // 광고 영역
                        '.view_m_adA', // 광고 영역
                        '.view_m_adB', // 광고 영역
                        '.a1'          // 광고 영역
                    ];
                    
                    // 제외할 요소들 제거
                    excludeSelectors.forEach(selector => {
                        const elements = newsView.querySelectorAll(selector);
                        elements.forEach(el => el.remove());
                    });
                    
                    // 텍스트 노드들을 순서대로 수집
                    const textNodes = [];
                    const walker = document.createTreeWalker(
                        newsView,
                        NodeFilter.SHOW_TEXT,
                        {
                            acceptNode: function(node) {
                                const text = node.textContent.trim();
                                if (text.length === 0) {
                                    return NodeFilter.FILTER_REJECT;
                                }
                                return NodeFilter.FILTER_ACCEPT;
                            }
                        }
                    );
                    
                    let node;
                    while (node = walker.nextNode()) {
                        textNodes.push(node.textContent.trim());
                    }
                    
                    // <br> 태그를 문단 구분자로 처리하고 텍스트 연결
                    let content = textNodes.join(' ').replace(/\\s+/g, ' ').trim();
                    
                    // <br> 태그를 문단 구분자로 변환
                    content = content.replace(/<br\\s*\\/?>/gi, '\\n\\n');
                    
                    // 연속된 공백 정리
                    content = content.replace(/\\s+/g, ' ').trim();
                    
                    return {content: content, published_at: publishedAt};
                }''')
                
                if result.get("content") and len(result["content"].strip()) > 50:
                    return result
                    
            except Exception as e:
                console.print(f"⚠️ JavaScript 본문 추출 실패: {str(e)[:50]}")
            
            return result
            
        except Exception as e:
            console.print(f"❌ 본문 추출 실패 ({url[:50]}...): {str(e)[:50]}")
            return {"content": "", "published_at": ""}
        finally:
            if page:
                try:
                    await page.close()
                except:
                    pass

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
            # <span aria-hidden="true">2025-09-04 15:33</span> 형식 찾기
            time_span = soup.select_one('span[aria-hidden="true"]')
            if time_span:
                time_text = time_span.get_text(strip=True)
                if re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$', time_text):
                    return self._parse_datetime(time_text)
            
            # 다른 시간 선택자들 시도
            time_selectors = [
                'time[datetime]',
                '.date',
                '.time',
                '.publish_date',
                '.article_date'
            ]
            
            for selector in time_selectors:
                element = soup.select_one(selector)
                if element:
                    datetime_attr = element.get('datetime')
                    if datetime_attr:
                        return self._parse_datetime(datetime_attr)
                    
                    text = element.get_text(strip=True)
                    if text and re.match(r'\d{4}-\d{2}-\d{2}', text):
                        return self._parse_datetime(text)
            
            return datetime.now(pytz.UTC).isoformat()
            
        except Exception as e:
            console.print(f"⚠️ 발행시간 추출 실패: {str(e)}")
            return datetime.now(pytz.UTC).isoformat()

    def _extract_content_text(self, soup: BeautifulSoup) -> str:
        """본문 텍스트 추출 (개선된 버전)"""
        try:
            # <section class="news_view"> 찾기
            news_view = soup.find('section', class_='news_view')
            if not news_view:
                # 대안: .view_body 찾기
                news_view = soup.find('div', class_='view_body')
                if not news_view:
                    return ""
            
            # 제외할 요소들 제거
            exclude_selectors = [
                'h2.sub_tit',  # 기사 제목
                'figure',      # 이미지 관련
                'img',         # 이미지
                'figcaption',  # 이미지 캡션
                '.view_m_adK', # 광고 영역
                '.view_ad06',  # 광고 영역
                '.view_m_adA', # 광고 영역
                '.view_m_adB', # 광고 영역
                '.a1',         # 광고 영역
                '.view_series', # 관련 기사
                '.view_trend',  # 트렌드 뉴스
                'script',      # 스크립트
                'style'        # 스타일
            ]
            
            for selector in exclude_selectors:
                elements = news_view.select(selector)
                for el in elements:
                    el.decompose()
            
            # 본문 텍스트 추출 - 더 정확한 방법
            content_parts = []
            
            # 1. <p> 태그에서 텍스트 추출
            paragraphs = news_view.find_all('p')
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text and len(text) > 20:  # 20자 이상인 문단만
                    content_parts.append(text)
            
            # 2. <p> 태그가 없으면 <div>에서 추출
            if not content_parts:
                divs = news_view.find_all('div')
                for div in divs:
                    text = div.get_text(strip=True)
                    if text and len(text) > 50:  # 50자 이상인 div만
                        # 중복 제거를 위해 이미 포함된 텍스트인지 확인
                        is_duplicate = any(text in existing for existing in content_parts)
                        if not is_duplicate:
                            content_parts.append(text)
            
            # 3. 텍스트 연결 및 정리
            if content_parts:
                content = ' '.join(content_parts)
                # 연속된 공백 정리
                content = re.sub(r'\s+', ' ', content).strip()
                return content
            else:
                # 4. 마지막 수단: 전체 텍스트에서 추출
                full_text = news_view.get_text(strip=True)
                if len(full_text) > 100:
                    return re.sub(r'\s+', ' ', full_text).strip()
                
            return ""
            
        except Exception as e:
            console.print(f"⚠️ 본문 추출 실패: {str(e)}")
            return ""

    def _parse_datetime(self, datetime_str: str) -> str:
        """날짜시간 문자열 파싱"""
        try:
            clean_time = datetime_str.strip()
            
            if re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$', clean_time):
                # "YYYY-MM-DD HH:MM" 형식인 경우 (KST 기준)
                kst_time = datetime.strptime(clean_time, "%Y-%m-%d %H:%M")
                kst_tz = pytz.timezone("Asia/Seoul")
                kst_dt = kst_tz.localize(kst_time)
                return kst_dt.astimezone(pytz.UTC).isoformat()
            elif 'T' in clean_time:
                # ISO 형식
                if '+' in clean_time:
                    published_at = datetime.fromisoformat(clean_time)
                    return published_at.astimezone(pytz.UTC).isoformat()
                else:
                    published_at = datetime.fromisoformat(clean_time.replace('Z', '+00:00'))
                    return published_at.isoformat()
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
                media_id = self.supabase_manager.create_media_outlet(self.media_name, self.media_bias)
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
            short_content_count = 0
            
            for article in self.articles:
                if article["url"] in existing_urls:
                    skip_count += 1
                    continue
                
                # 본문 길이 체크 (20자 미만 제외)
                content = article.get('content', '')
                if len(content.strip()) < 20:
                    short_content_count += 1
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
                
            console.print(f"\n📊 저장 결과: 성공 {len(new_articles)}, 스킵 {skip_count}, 짧은본문 제외 {short_content_count}")
            
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

    async def cleanup(self):
        """리소스 정리"""
        try:
            if self._browser:
                await self._browser.close()
                self._browser = None
            if self._playwright:
                await self._playwright.stop()
                self._playwright = None
            console.print("🧹 Playwright 리소스 정리 완료")
        except Exception as e:
            console.print(f"⚠️ 리소스 정리 중 오류: {str(e)[:50]}")

    async def run(self, num_pages: int = 15):
        """실행 (최적화 버전)"""
        try:
            console.print(f"🚀 동아일보 정치 기사 크롤링 시작 (최적화 버전, 최대 {num_pages}페이지)")
            
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
    collector = DongaPoliticsCollector()
    await collector.run(num_pages=15)  # 15페이지에서 각각 10개씩 총 150개 수집

if __name__ == "__main__":
    asyncio.run(main())