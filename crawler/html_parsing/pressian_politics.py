#!/usr/bin/env python3
"""
프레시안 정치 기사 크롤러 (HTML 파싱 기반)
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
from rich.console import Console

# 프로젝트 루트 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 내부 모듈
from utils.supabase_manager import SupabaseManager

console = Console()
KST = pytz.timezone("Asia/Seoul")


class PressianPoliticsCollector:
    def __init__(self):
        self.base_url = "https://www.pressian.com"
        self.media_name = "프레시안"
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
            url = f"{self.base_url}/pages/news-politics-list?page={page}"
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
                    
                    # 기사 목록 추출 (.arl_022 ul.list > li)
                    list_items = soup.select('.arl_022 ul.list > li')
                    
                    for li in list_items:
                        try:
                            # 제목과 URL 추출 (p.title a[href])
                            title_link = li.select_one('p.title a[href]')
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
                            
                            # article_id 추출 (/pages/articles/<digits>에서 숫자만)
                            article_id_match = re.search(r'/pages/articles/(\d+)', href)
                            article_id = article_id_match.group(1) if article_id_match else None
                            
                            # subtitle 추출 (p.sub_title a)
                            subtitle = ""
                            subtitle_element = li.select_one('p.sub_title a')
                            if subtitle_element:
                                subtitle = subtitle_element.get_text(strip=True)
                            
                            # excerpt 추출 (p.body a)
                            excerpt = ""
                            excerpt_element = li.select_one('p.body a')
                            if excerpt_element:
                                excerpt = excerpt_element.get_text(strip=True)
                            
                            # 썸네일 추출 (.thumb .arl_img style의 background-image)
                            image_url = self._extract_thumbnail_url(li)
                            
                            # 바이라인 추출 (.byline .name과 .byline .date)
                            author, published_at_kst, published_date = self._extract_byline(li)
                            
                            # published_at_utc 계산
                            published_at_utc = ""
                            if published_at_kst:
                                try:
                                    kst_dt = datetime.fromisoformat(published_at_kst.replace('+09:00', ''))
                                    kst_dt = KST.localize(kst_dt)
                                    utc_dt = kst_dt.astimezone(pytz.UTC)
                                    published_at_utc = utc_dt.isoformat()
                                except:
                                    published_at_utc = datetime.now(pytz.UTC).isoformat()
                            
                            if title and len(title) > 10:
                                article = {
                                    'source': 'pressian',
                                    'article_id': article_id,
                                    'title': title,
                                    'url': full_url,
                                    'content': '',
                                    'subtitle': subtitle,
                                    'excerpt': excerpt,
                                    'author': author,
                                    'published_date': published_date,
                                    'published_at': published_at_utc,
                                    'image_url': image_url,
                                    'image_alt': None
                                }
                                articles.append(article)
                                console.print(f"📰 발견: {title[:50]}...")
                        
                        except Exception as e:
                            console.print(f"⚠️ 기사 카드 처리 중 오류: {str(e)}")
                            continue
                    
                    console.print(f"📄 페이지 {page_num}: {len(articles)}개 기사 수집")
                    return articles
                    
            except Exception as e:
                console.print(f"❌ 페이지 {page_num} 처리 중 오류: {str(e)}")
                return []

    def _extract_thumbnail_url(self, li) -> Optional[str]:
        """썸네일 URL 추출 (.thumb .arl_img style의 background-image)"""
        try:
            thumb_element = li.select_one('.thumb .arl_img')
            if not thumb_element:
                return None
            
            style = thumb_element.get('style', '')
            if not style:
                return None
            
            # background-image:url('...')에서 URL 추출
            url_match = re.search(r"background-image:\s*url\(['\"]?([^'\"]+)['\"]?\)", style)
            if not url_match:
                return None
            
            url = url_match.group(1)
            
            # 상대 URL을 절대 URL로 변환
            if not url.startswith('http'):
                url = urljoin(self.base_url, url)
            
            return url
                
        except Exception as e:
            console.print(f"⚠️ 썸네일 URL 추출 실패: {str(e)}")
            return None

    def _extract_byline(self, li) -> tuple:
        """바이라인 추출 (.byline .name과 .byline .date)"""
        try:
            # author 추출 (.byline .name)
            author = ""
            name_element = li.select_one('.byline .name')
            if name_element:
                author_text = name_element.get_text(strip=True)
                # 끝의 "기자" 토큰 제거
                if author_text.endswith('기자'):
                    author = author_text[:-2].strip()
                else:
                    author = author_text
            
            # 날짜 추출 (.byline .date)
            published_at_kst = ""
            published_date = ""
            date_element = li.select_one('.byline .date')
            if date_element:
                date_text = date_element.get_text(strip=True)
                # YYYY.MM.DD HH:MM:SS 형식 파싱
                date_match = re.search(r'(\d{4})\.(\d{2})\.(\d{2})\s+(\d{2}):(\d{2}):(\d{2})', date_text)
                if date_match:
                    year, month, day, hour, minute, second = date_match.groups()
                    published_date = f"{year}-{month}-{day}"
                    published_at_kst = f"{year}-{month}-{day}T{hour}:{minute}:{second}+09:00"
            
            return author, published_at_kst, published_date
                
        except Exception as e:
            console.print(f"⚠️ 바이라인 추출 실패: {str(e)}")
            return "", "", ""

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
            
            # 바이라인 정보 업데이트
            byline_data = content_data.get("byline", {})
            if byline_data.get("author"):
                article["author"] = byline_data["author"]
            
            console.print(f"✅ [{index}] 완료: {len(article['content'])}자")
            
        except Exception as e:
            console.print(f"❌ [{index}] 실패: {str(e)[:50]}...")
            article["content"] = ""

    def _extract_content_text(self, soup: BeautifulSoup) -> Dict[str, any]:
        """프레시안 본문 텍스트 추출"""
        try:
            # 1차: .section .article_body[itemprop="articleBody"] 찾기
            content_container = soup.select_one('.section .article_body[itemprop="articleBody"]')
            
            # 2차 폴백: .article_body
            if not content_container:
                content_container = soup.select_one('.article_body')
            
            if not content_container:
                console.print("⚠️ 본문 컨테이너를 찾을 수 없습니다")
                return {"text": "", "byline": {}}
            
            # 제외할 요소들 제거
            exclude_selectors = [
                'script', 'style', 'noscript', 'figure', 'figcaption', 'img',
                '.article_ad', '.article_ad2', '[class^=ads]', 'ins.adsbygoogle', 
                'iframe', '[id^=google_ads_]'
            ]
            
            for selector in exclude_selectors:
                elements = content_container.select(selector)
                for el in elements:
                    el.decompose()
            
            # <br> 태그를 줄바꿈으로 변환
            for br in content_container.find_all('br'):
                br.replace_with('\n')
            
            # paragraphs 수집 (컨테이너 후손의 <p>)
            paragraphs = []
            p_elements = content_container.find_all('p')
            for element in p_elements:
                text = element.get_text(strip=True)
                # 비어 있는 단락과 광고/모듈 내부 단락 제외
                if text and len(text) > 10:
                    # &nbsp; 제거 및 다중 공백/개행 축약
                    text = re.sub(r'&nbsp;', ' ', text)
                    text = re.sub(r'\s+', ' ', text).strip()
                    if text:
                        paragraphs.append(text)
            
            # 텍스트 결합
            combined_text = '\n\n'.join(paragraphs)
            
            # 공백 정리
            combined_text = re.sub(r'\n\s*\n', '\n\n', combined_text).strip()
            
            # 바이라인 추출 (컨테이너 밖의 .list_author .byline)
            byline_data = self._extract_article_byline(soup)
            
            return {
                "text": combined_text,
                "byline": byline_data
            }
            
        except Exception as e:
            console.print(f"⚠️ 본문 추출 실패: {str(e)}")
            return {"text": "", "byline": {}}

    def _extract_article_byline(self, soup: BeautifulSoup) -> Dict[str, str]:
        """기사 하단 바이라인 추출 (.list_author .byline)"""
        try:
            byline_data = {"author": "", "author_email": ""}
            
            byline_container = soup.select_one('.list_author .byline')
            if not byline_container:
                return byline_data
            
            # author 추출 (.name)
            name_element = byline_container.select_one('.name')
            if name_element:
                author_text = name_element.get_text(strip=True)
                # 끝의 "기자" 토큰 제거
                if author_text.endswith('기자'):
                    byline_data["author"] = author_text[:-2].strip()
                else:
                    byline_data["author"] = author_text
            
            # author_email 추출 (.mail .tooltip 또는 a[href^="mailto:"])
            email_element = byline_container.select_one('.mail .tooltip')
            if not email_element:
                email_element = byline_container.select_one('a[href^="mailto:"]')
            
            if email_element:
                if email_element.name == 'a':
                    href = email_element.get('href', '')
                    email_match = re.search(r'mailto:([^"]+)', href)
                    if email_match:
                        byline_data["author_email"] = email_match.group(1)
                else:
                    byline_data["author_email"] = email_element.get_text(strip=True)
            
            return byline_data
            
        except Exception as e:
            console.print(f"⚠️ 바이라인 추출 실패: {str(e)}")
            return {"author": "", "author_email": ""}

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
            console.print(f"🚀 프레시안 정치 기사 크롤링 시작 (최대 {num_pages}페이지)")
            
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
    collector = PressianPoliticsCollector()
    await collector.run(num_pages=16)  # 16페이지에서 각각 10개씩 총 160개 수집

if __name__ == "__main__":
    asyncio.run(main())
