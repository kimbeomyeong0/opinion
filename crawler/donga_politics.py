#!/usr/bin/env python3
"""
동아일보 정치 기사 크롤러 (웹 스크래핑 기반)
- 동아일보 정치 섹션에서 페이지네이션을 통한 기사 수집
- 각 페이지에서 최상단 기사 1개씩 수집
- 페이지 규칙: p=1, 11, 21, 31... (10씩 증가)
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
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# 프로젝트 루트 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
        self._playwright = None
        self._browser = None

    def _get_page_urls(self, num_pages: int = 4) -> List[str]:
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

    async def _collect_articles(self, num_pages: int = 4):
        """기사 수집 (페이지네이션)"""
        console.print(f"🚀 동아일보 정치 기사 수집 시작 (최대 {num_pages}페이지)")
        
        page_urls = self._get_page_urls(num_pages)
        all_articles = []
        
        for i, page_url in enumerate(page_urls, 1):
            console.print(f"📄 페이지 {i}/{len(page_urls)} 처리 중...")
            articles = await self._get_page_articles(page_url)
            all_articles.extend(articles)
            
            # 페이지 간 대기
            await asyncio.sleep(0.5)
        
        # 각 기사 데이터 파싱
        success_count = 0
        for i, article_data in enumerate(all_articles, 1):
            parsed_article = self._parse_article_data(article_data)
            if parsed_article:
                self.articles.append(parsed_article)
                success_count += 1
                console.print(f"✅ [{i}/{len(all_articles)}] {parsed_article['title'][:50]}...")
            else:
                console.print(f"❌ [{i}/{len(all_articles)}] 기사 파싱 실패")

        console.print(f"📊 수집 완료: {success_count}/{len(all_articles)}개 성공")

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

    async def collect_contents(self):
        """본문 전문 수집"""
        if not self.articles:
            return

        console.print(f"📖 본문 수집 시작: {len(self.articles)}개 기사")
        
        success_count = 0
        for i, art in enumerate(self.articles, 1):
            content_data = await self._extract_content(art["url"])
            if content_data and content_data.get("content"):
                # 본문과 발행 시간 업데이트
                self.articles[i-1]["content"] = content_data["content"]
                if content_data.get("published_at"):
                    # 발행 시간 파싱 및 UTC 변환
                    try:
                        kst_time = datetime.strptime(content_data["published_at"], "%Y-%m-%d %H:%M")
                        kst_tz = pytz.timezone("Asia/Seoul")
                        kst_dt = kst_tz.localize(kst_time)
                        self.articles[i-1]["published_at"] = kst_dt.astimezone(pytz.UTC).isoformat()
                    except Exception as e:
                        console.print(f"⚠️ [{i}/{len(self.articles)}] 발행 시간 파싱 실패: {e}")
                
                success_count += 1
                console.print(f"✅ [{i}/{len(self.articles)}] 본문 수집 성공")
            else:
                console.print(f"⚠️ [{i}/{len(self.articles)}] 본문 수집 실패")

        console.print(f"✅ 본문 수집 완료: {success_count}/{len(self.articles)}개 성공")

    async def save_to_supabase(self):
        """DB 저장"""
        if not self.articles:
            console.print("❌ 저장할 기사가 없습니다.")
            return

        console.print(f"💾 Supabase에 {len(self.articles)}개 기사 저장 중...")

        # 언론사 확인
        media = self.supabase_manager.get_media_outlet(self.media_name)
        if not media:
            media_id = self.supabase_manager.create_media_outlet(self.media_name, self.media_bias)
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

        success, failed, skipped = 0, 0, 0
        
        for i, art in enumerate(self.articles, 1):
            try:
                if art["url"] in existing_urls:
                    console.print(f"⚠️ [{i}/{len(self.articles)}] 중복 기사 스킵: {art['title'][:30]}...")
                    skipped += 1
                    continue

                published_at_str = art.get("published_at")
                created_at_str = art.get("created_at", published_at_str)

                article_data = {
                    "media_id": media_id,
                    "title": art["title"],
                    "content": art["content"],
                    "url": art["url"],
                    "published_at": published_at_str,
                    "created_at": created_at_str,
                }

                if self.supabase_manager.insert_article(article_data):
                    success += 1
                    console.print(f"✅ [{i}/{len(self.articles)}] 저장 성공: {art['title'][:30]}...")
                else:
                    failed += 1
                    console.print(f"❌ [{i}/{len(self.articles)}] 저장 실패: {art['title'][:30]}...")
                    
            except Exception as e:
                failed += 1
                console.print(f"❌ [{i}/{len(self.articles)}] 저장 오류: {str(e)[:50]}")

        console.print(f"\n📊 저장 결과:")
        console.print(f"  ✅ 성공: {success}개")
        console.print(f"  ❌ 실패: {failed}개") 
        console.print(f"  ⚠️ 중복 스킵: {skipped}개")
        console.print(f"  📈 성공률: {(success / len(self.articles) * 100):.1f}%")

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

    async def run(self, num_pages: int = 4):
        """실행"""
        try:
            console.print(f"🚀 동아일보 정치 기사 크롤링 시작 (최대 {num_pages}페이지)")
            
            await self._collect_articles(num_pages)
            await self.collect_contents()
            await self.save_to_supabase()
            
            console.print("🎉 크롤링 완료!")
            
        except KeyboardInterrupt:
            console.print("⏹️ 사용자에 의해 중단되었습니다")
        except Exception as e:
            console.print(f"❌ 크롤링 중 오류 발생: {str(e)}")
        finally:
            await self.cleanup()


async def main():
    collector = DongaPoliticsCollector()
    await collector.run(num_pages=10)  # 10페이지에서 각각 10개씩 총 100개 수집

if __name__ == "__main__":
    asyncio.run(main())