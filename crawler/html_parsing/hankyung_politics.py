#!/usr/bin/env python3
"""
한국경제 정치 기사 크롤러 (HTML 파싱 기반)
- 정치 섹션 기사 수집
- 본문은 httpx로 별도 수집
"""

import asyncio
import sys
import os
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
import httpx
import pytz
from bs4 import BeautifulSoup
import re
import html
from rich.console import Console

# 프로젝트 루트 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 내부 모듈
from utils.supabase_manager import SupabaseManager

console = Console()
KST = pytz.timezone("Asia/Seoul")


class HankyungPoliticsCollector:
    def __init__(self):
        self.base_url = "https://www.hankyung.com"
        self.media_name = "한국경제"
        self.media_bias = "left"  # 진보 성향
        self.supabase_manager = SupabaseManager()
        self.articles: List[Dict] = []
        
        # HTTP 클라이언트 설정
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        
        # 동시성 제한 설정
        self.semaphore = asyncio.Semaphore(10)

    def _get_page_urls(self, num_pages: int = 8) -> List[str]:
        """페이지 URL 목록 생성 (page=1, 2, 3...)"""
        urls = []
        for page in range(1, num_pages + 1):
            url = f"{self.base_url}/all-news-politics?page={page}"
            urls.append(url)
        return urls

    async def _get_page_articles(self, page_url: str, page_num: int) -> List[Dict]:
        """특정 페이지에서 기사 목록 수집"""
        console.print(f"📡 페이지 {page_num}: HTML 파싱 중...")

        async with self.semaphore:
            try:
                async with httpx.AsyncClient(
                    timeout=15.0,
                    follow_redirects=True,
                    limits=httpx.Limits(max_keepalive_connections=20, max_connections=50)
                ) as client:
                    response = await client.get(page_url, headers=self.headers)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, "html.parser")
                    
                    articles = []
                    
                    # 기사 목록 추출 (.allnews-wrap .allnews-panel ul.allnews-list > li[data-aid])
                    list_items = soup.select('.allnews-wrap .allnews-panel ul.allnews-list > li[data-aid]')
                    
                    for li in list_items:
                        try:
                            # data-aid에서 article_id 추출
                            data_aid = li.get('data-aid', '')
                            article_id = data_aid
                            
                            # 제목과 URL 추출 (h2.news-tit a[href])
                            title_link = li.select_one('h2.news-tit a[href]')
                            if not title_link:
                                continue
                                
                            href = title_link.get('href')
                            if not href:
                                continue
                            
                            # 상대 URL을 절대 URL로 변환
                            if href.startswith('http'):
                                full_url = href
                            else:
                                full_url = urljoin(self.base_url, href)
                            
                            title = title_link.get_text(strip=True)
                            
                            # article_id 보정 (/article/(\d+)에서 숫자만 추출)
                            article_id_match = re.search(r'/article/(\d+)', href)
                            if article_id_match:
                                article_id = article_id_match.group(1)
                            
                            # 발행시각 추출 (.txt-date)
                            published_date, published_at_kst, published_at_utc = self._extract_published_time(li)
                            
                            # 썸네일 추출 (.thumb img)
                            image_url, image_alt = self._extract_thumbnail(li)
                            
                            # 텍스트 정리 (HTML 엔티티 디코드 후 공백 정리)
                            title = self._clean_text(title)
                            
                            if title and len(title) > 10:
                                article = {
                                    'article_id': article_id,
                                    'title': title,
                                    'url': full_url,
                                    'content': '',
                                    'published_date': published_date,
                                    'published_at': published_at_utc,
                                    'image_url': image_url,
                                    'image_alt': image_alt
                                }
                                articles.append(article)
                                console.print(f"📰 발견: {title[:50]}...")
                        
                        except Exception as e:
                            console.print(f"⚠️ 기사 카드 처리 중 오류: {str(e)}")
                            continue
                    
                    # 중복 제거 (article_id 기준)
                    unique_articles = []
                    seen_ids = set()
                    for article in articles:
                        if article['article_id'] not in seen_ids:
                            unique_articles.append(article)
                            seen_ids.add(article['article_id'])
                    
                    console.print(f"📄 페이지 {page_num}: {len(unique_articles)}개 기사 수집")
                    return unique_articles
                    
            except Exception as e:
                console.print(f"❌ 페이지 {page_num} 처리 중 오류: {str(e)}")
                return []

    def _extract_published_time(self, li) -> tuple:
        """발행시각 추출 (.txt-date)"""
        try:
            date_element = li.select_one('.txt-date')
            if not date_element:
                return "", "", ""
            
            date_text = date_element.get_text(strip=True)
            # YYYY.MM.DD HH:MM 형식 파싱
            date_match = re.search(r'(\d{4})\.(\d{2})\.(\d{2})\s+(\d{2}):(\d{2})', date_text)
            if not date_match:
                return "", "", ""
            
            year, month, day, hour, minute = date_match.groups()
            published_date = f"{year}-{month}-{day}"
            published_at_kst = f"{year}-{month}-{day}T{hour}:{minute}:00+09:00"
            
            # KST를 UTC로 변환
            try:
                kst_dt = datetime.strptime(f"{year}-{month}-{day} {hour}:{minute}", "%Y-%m-%d %H:%M")
                kst_dt = KST.localize(kst_dt)
                utc_dt = kst_dt.astimezone(pytz.UTC)
                published_at_utc = utc_dt.isoformat()
            except:
                published_at_utc = datetime.now(pytz.UTC).isoformat()
            
            return published_date, published_at_kst, published_at_utc
                
        except Exception as e:
            console.print(f"⚠️ 발행시각 추출 실패: {str(e)}")
            return "", "", ""

    def _extract_thumbnail(self, li) -> tuple:
        """썸네일 추출 (.thumb img)"""
        try:
            img_element = li.select_one('.thumb img')
            if not img_element:
                return None, ""
            
            src = img_element.get('src', '')
            alt = img_element.get('alt', '')
            
            # 상대 URL을 절대 URL로 변환
            if src and not src.startswith('http'):
                src = urljoin(self.base_url, src)
            
            return src, alt
                
        except Exception as e:
            console.print(f"⚠️ 썸네일 추출 실패: {str(e)}")
            return None, ""

    def _clean_text(self, text: str) -> str:
        """텍스트 정리 (HTML 엔티티 디코드 후 공백 정리)"""
        try:
            # HTML 엔티티 디코드
            text = html.unescape(text)
            # &nbsp; 제거
            text = re.sub(r'&nbsp;', ' ', text)
            # 다중 공백 축약
            text = re.sub(r'\s+', ' ', text)
            # trim
            return text.strip()
        except:
            return text

    async def collect_articles_parallel(self, num_pages: int = 8):
        """기사 수집 (병렬 처리)"""
        console.print(f"📄 {num_pages}개 페이지에서 기사 수집 시작 (병렬 처리)...")
        
        page_urls = self._get_page_urls(num_pages)
        
        # 모든 페이지를 동시에 처리
        tasks = [self._get_page_articles(url, i + 1) for i, url in enumerate(page_urls)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 수집
        total_articles = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                console.print(f"❌ 페이지 {i + 1} 처리 중 오류: {str(result)}")
            else:
                self.articles.extend(result)
                total_articles += len(result)
                
        console.print(f"📊 총 {total_articles}개 기사 수집")

    async def collect_contents_parallel(self):
        """기사 본문 수집 (병렬 처리)"""
        if not self.articles:
            return
            
        console.print(f"📖 {len(self.articles)}개 기사 본문 수집 시작 (병렬 처리)...")
        
        # 모든 기사를 동시에 처리 (배치로 나누어서)
        batch_size = 20
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
            
            # 배치 간 짧은 딜레이
            if batch_num < total_batches - 1:
                await asyncio.sleep(0.5)

    async def _extract_content_httpx(self, client: httpx.AsyncClient, article: dict, index: int):
        """httpx로 기사 본문 추출"""
        try:
            console.print(f"📖 [{index}] 시작: {article['title'][:40]}...")
            
            response = await client.get(article["url"], headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 본문 추출
            content_data = self._extract_content_text(soup)
            article["content"] = content_data.get("text", "")
            
            console.print(f"✅ [{index}] 완료: {len(article['content'])}자")
            
        except Exception as e:
            console.print(f"❌ [{index}] 실패: {str(e)[:50]}...")
            article["content"] = ""

    def _extract_content_text(self, soup: BeautifulSoup) -> Dict[str, any]:
        """한국경제 본문 텍스트 추출"""
        try:
            # .article-body#articletxt[itemprop="articleBody"] 찾기
            content_container = soup.select_one('.article-body#articletxt[itemprop="articleBody"]')
            
            if not content_container:
                console.print("⚠️ 본문 컨테이너를 찾을 수 없습니다")
                return {"text": "", "byline": {}, "lead_image": None}
            
            # 제외할 요소들 제거 (figure는 제외하고 나중에 별도 처리)
            exclude_selectors = [
                'script', 'style', 'noscript', 'iframe',
                '.ad-area-wrap', '[id^=div-gpt-ad]', 'ins'
            ]
            
            for selector in exclude_selectors:
                elements = content_container.select(selector)
                for el in elements:
                    el.decompose()
            
            # figure는 이미지만 제거하고 텍스트는 보존
            figures = content_container.find_all('figure')
            for figure in figures:
                # figure 내부의 img와 figcaption만 제거
                for img in figure.find_all('img'):
                    img.decompose()
                for caption in figure.find_all('figcaption'):
                    caption.decompose()
            
            # <br> 태그를 줄바꿈으로 변환
            for br in content_container.find_all('br'):
                br.replace_with('\n')
            
            # lead_image 추출 (첫 figure)
            lead_image = self._extract_lead_image(content_container)
            
            # paragraphs 수집
            paragraphs = []
            
            # 먼저 <p> 태그에서 수집
            for element in content_container.find_all('p'):
                text = element.get_text(strip=True)
                if text and len(text) > 10:
                    # 공백 정리
                    text = self._clean_text(text)
                    if text:
                        paragraphs.append(text)
            
            # <p> 태그가 없으면 직접 텍스트 노드에서 수집
            if not paragraphs:
                # 전체 텍스트에서 추출
                full_text = content_container.get_text()
                # 연속 줄바꿈(2개 이상)을 단락 경계로 간주
                text_blocks = re.split(r'\n\s*\n', full_text)
                for block in text_blocks:
                    text = block.strip()
                    if text and len(text) > 10:
                        # 공백 정리
                        text = self._clean_text(text)
                        if text:
                            paragraphs.append(text)
            
            # 마지막 문단이 기자 또는 이메일(@)을 포함하면 byline으로 분리
            byline = {"author": "", "email": ""}
            if paragraphs:
                last_paragraph = paragraphs[-1]
                if ('기자' in last_paragraph and len(last_paragraph) < 50) or '@' in last_paragraph:
                    # 기자 정보 추출
                    if '기자' in last_paragraph:
                        author_text = last_paragraph.replace('기자', '').strip()
                        byline["author"] = author_text
                    # 이메일 추출
                    email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', last_paragraph)
                    if email_match:
                        byline["email"] = email_match.group(1)
                    
                    paragraphs.pop()
            
            # 텍스트 결합
            combined_text = '\n\n'.join(paragraphs)
            
            # 공백 정리
            combined_text = re.sub(r'\n\s*\n', '\n\n', combined_text).strip()
            
            return {
                "text": combined_text,
                "byline": byline,
                "lead_image": lead_image
            }
            
        except Exception as e:
            console.print(f"⚠️ 본문 추출 실패: {str(e)}")
            return {"text": "", "byline": {}, "lead_image": None}

    def _extract_lead_image(self, container) -> Optional[Dict[str, str]]:
        """첫 figure에서 lead_image 추출"""
        try:
            first_figure = container.find('figure')
            if not first_figure:
                return None
            
            lead_image = {"src": "", "alt": "", "caption": ""}
            
            # img 태그에서 src, alt 추출
            img = first_figure.find('img')
            if img:
                lead_image["src"] = img.get('src', '')
                lead_image["alt"] = img.get('alt', '')
                
                # 상대 URL을 절대 URL로 변환
                if lead_image["src"] and not lead_image["src"].startswith('http'):
                    lead_image["src"] = urljoin(self.base_url, lead_image["src"])
            
            # caption 추출 (figcaption)
            caption = first_figure.find('figcaption')
            if caption:
                lead_image["caption"] = caption.get_text(strip=True)
            
            return lead_image if lead_image["src"] else None
                
        except Exception as e:
            console.print(f"⚠️ lead_image 추출 실패: {str(e)}")
            return None

    async def save_articles_batch(self):
        """DB 배치 저장"""
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

    async def run(self, num_pages: int = 8):
        """실행"""
        try:
            console.print(f"🚀 한국경제 정치 기사 크롤링 시작 (최대 {num_pages}페이지)")
            
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
    collector = HankyungPoliticsCollector()
    await collector.run(num_pages=4)  # 4페이지에서 각각 40개씩 총 160개 수집

if __name__ == "__main__":
    asyncio.run(main())
