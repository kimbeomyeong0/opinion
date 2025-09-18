#!/usr/bin/env python3
"""
문화일보 정치 기사 크롤러 (HTML 파싱 기반)
- _CP/43 API를 사용하여 기사 목록 수집
- 정치 섹션 기사만 필터링
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


class MunhwaPoliticsCollector:
    def __init__(self):
        self.base_url = "https://www.munhwa.com"
        self.api_base = "https://www.munhwa.com/_CP/43"
        self.media_name = "문화일보"
        self.media_bias = "left"  # 진보 성향으로 변경
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

    def _get_page_urls(self, num_pages: int = 10) -> List[str]:
        """API 페이지 URL 목록 생성 (page=1, 2, 3...)"""
        urls = []
        for page in range(1, num_pages + 1):
            url = f"{self.api_base}?page={page}&domainId=1000&mKey=politicsAll&keyword=&term=2&type=C"
            urls.append(url)
        return urls

    async def _get_page_articles(self, page_url: str, page_num: int) -> List[Dict]:
        """특정 페이지에서 기사 목록 수집"""
        console.print(f"📡 페이지 {page_num}: API 호출 중...")

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
                    
                    # 기사 목록 추출
                    list_items = soup.find_all('li', attrs={'data-li': True})
                    
                    for li in list_items:
                        # a 태그 찾기
                        link = li.find('a', href=True)
                        if not link:
                            continue
                            
                        href = link.get('href')
                        if not href or not href.startswith('/article/'):
                            continue
                        
                        # 상대 URL을 절대 URL로 변환
                        full_url = urljoin(self.base_url, href)
                        
                        # 제목 추출
                        title_element = li.find('h4', class_='title')
                        if title_element:
                            title_link = title_element.find('a')
                            if title_link:
                                title = title_link.get_text(strip=True)
                            else:
                                title = title_element.get_text(strip=True)
                        else:
                            title = link.get_text(strip=True)
                        
                        # 날짜 추출
                        date_element = li.find('span', class_='date')
                        published_at = ""
                        if date_element:
                            date_text = date_element.get_text(strip=True)
                            published_at = self._parse_datetime(date_text)
                        
                        # 기자 정보 추출
                        writer_element = li.find('span', class_='writer')
                        author = writer_element.get_text(strip=True) if writer_element else ""
                        
                        # 요약 추출 (있는 경우)
                        desc_element = li.find('p', class_='description')
                        description = ""
                        if desc_element:
                            desc_link = desc_element.find('a')
                            if desc_link:
                                description = desc_link.get_text(strip=True)
                        
                        if title and len(title) > 10:
                            article = {
                                'title': title,
                                'url': full_url,
                                'content': '',
                                'published_at': published_at,
                                'author': author,
                                'description': description
                            }
                            articles.append(article)
                            console.print(f"📰 발견: {title[:50]}...")
                    
                    console.print(f"📄 페이지 {page_num}: {len(articles)}개 기사 수집")
                    return articles
                    
            except Exception as e:
                console.print(f"❌ 페이지 {page_num} 처리 중 오류: {str(e)}")
                return []

    async def collect_articles_parallel(self, num_pages: int = 10):
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
        """httpx로 기사 본문 및 발행시간 추출"""
        try:
            console.print(f"📖 [{index}] 시작: {article['title'][:40]}...")
            
            response = await client.get(article["url"], headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 발행시간 추출 (API에서 가져온 것이 없으면)
            if not article.get("published_at"):
                published_at = self._extract_published_at(soup)
                article["published_at"] = published_at
            
            # 본문 추출
            content = self._extract_content_text(soup)
            article["content"] = content
            
            console.print(f"✅ [{index}] 완료: {len(content)}자")
            
        except Exception as e:
            console.print(f"❌ [{index}] 실패: {str(e)[:50]}...")
            article["content"] = ""
            if not article.get("published_at"):
                article["published_at"] = datetime.now(pytz.UTC).isoformat()

    def _extract_published_at(self, soup: BeautifulSoup) -> str:
        """발행시간 추출"""
        try:
            # meta 태그에서 추출
            meta_date = soup.find('meta', property='article:published_time')
            if meta_date:
                return self._parse_datetime(meta_date.get('content', ''))
            
            # 기타 날짜 선택자들 시도
            date_selectors = [
                '.date',
                '.publish-date',
                '.article-date',
                '[class*="date"]'
            ]
            
            for selector in date_selectors:
                element = soup.select_one(selector)
                if element:
                    text = element.get_text(strip=True)
                    if text and re.search(r'\d{4}[-/]\d{2}[-/]\d{2}', text):
                        return self._parse_datetime(text)
            
            return datetime.now(pytz.UTC).isoformat()
            
        except Exception as e:
            console.print(f"⚠️ 발행시간 추출 실패: {str(e)}")
            return datetime.now(pytz.UTC).isoformat()

    def _extract_content_text(self, soup: BeautifulSoup) -> str:
        """문화일보 본문 텍스트 추출 (p.text-l만 추출)"""
        try:
            # #article-body 컨테이너 찾기
            content_container = soup.select_one('#article-body')
            
            if not content_container:
                console.print("⚠️ 본문 컨테이너를 찾을 수 없습니다")
                return ""
            
            # 제외할 요소들 완전 제거
            exclude_selectors = [
                'script', 'style', 'noscript', 
                '.article-photo-wrap', '.article-subtitle',  # 부제목 제외
                'figure', 'figcaption', 
                '[data-svcad]', '[id^=svcad_]', '[id*=svcad]',  # 광고 영역 강화
                '[class^=ad-]', '[class*=ad-]', 
                'ins', 'iframe', 'div[id^="svcad"]'
            ]
            
            for selector in exclude_selectors:
                elements = content_container.select(selector)
                for el in elements:
                    el.decompose()
            
            # p.text-l 태그만 선택적으로 추출
            paragraphs = []
            paragraph_elements = content_container.select('p.text-l')
            
            for element in paragraph_elements:
                # HTML 엔티티 처리를 위해 get_text() 사용
                text = element.get_text(strip=True)
                
                # 기자명 패턴 제거 (다양한 형태)
                text = re.sub(r'…\s*\w+\s*기자\s*$', '', text, flags=re.MULTILINE)
                text = re.sub(r'\w+\s*기자\s*$', '', text, flags=re.MULTILINE)
                text = re.sub(r'기자\s*\w+\s*$', '', text, flags=re.MULTILINE)
                
                # 의미있는 텍스트만 추가 (10자 이상)
                if text and len(text.strip()) > 10:
                    paragraphs.append(text.strip())
            
            # p.text-l이 없는 경우 일반 p 태그 시도 (fallback)
            if not paragraphs:
                console.print("⚠️ p.text-l을 찾을 수 없어 일반 p 태그 시도")
                all_p_elements = content_container.find_all('p')
                for element in all_p_elements:
                    text = element.get_text(strip=True)
                    # 기자명 패턴 제거
                    text = re.sub(r'…\s*\w+\s*기자\s*$', '', text, flags=re.MULTILINE)
                    text = re.sub(r'\w+\s*기자\s*$', '', text, flags=re.MULTILINE)
                    
                    if text and len(text.strip()) > 20:  # fallback은 더 긴 텍스트만
                        paragraphs.append(text.strip())
            
            if not paragraphs:
                console.print("⚠️ 추출할 본문이 없습니다")
                return ""
            
            # 문단들을 줄바꿈으로 연결
            combined_text = '\n\n'.join(paragraphs)
            
            # HTML 엔티티 및 공백 정규화
            combined_text = re.sub(r'&nbsp;', ' ', combined_text)  # &nbsp; 제거
            combined_text = re.sub(r'&[a-zA-Z]+;', ' ', combined_text)  # 기타 HTML 엔티티
            combined_text = re.sub(r'\s+', ' ', combined_text)  # 연속 공백 정규화
            combined_text = re.sub(r'\n\s*\n', '\n\n', combined_text)  # 연속 줄바꿈 정규화
            
            return combined_text.strip()
            
        except Exception as e:
            console.print(f"⚠️ 본문 추출 실패: {str(e)}")
            return ""

    def _parse_datetime(self, datetime_str: str) -> str:
        """날짜시간 문자열 파싱"""
        try:
            clean_time = datetime_str.strip()
            
            # ISO 형식 (2025-09-15T18:46:30+09:00)
            if 'T' in clean_time:
                if '+' in clean_time:
                    published_at = datetime.fromisoformat(clean_time)
                    return published_at.astimezone(pytz.UTC).isoformat()
                else:
                    published_at = datetime.fromisoformat(clean_time.replace('Z', '+00:00'))
                    return published_at.isoformat()
            
            # 일반 형식 (2025-09-15 18:46)
            if re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$', clean_time):
                kst_time = datetime.strptime(clean_time, "%Y-%m-%d %H:%M")
                kst_tz = pytz.timezone("Asia/Seoul")
                kst_dt = kst_tz.localize(kst_time)
                return kst_dt.astimezone(pytz.UTC).isoformat()
            
            return datetime.now(pytz.UTC).isoformat()
                
        except Exception as e:
            console.print(f"⚠️ 날짜 파싱 실패: {clean_time} - {str(e)}")
            return datetime.now(pytz.UTC).isoformat()

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

    async def run(self, num_pages: int = 10):
        """실행"""
        try:
            console.print(f"🚀 문화일보 정치 기사 크롤링 시작 (최대 {num_pages}페이지)")
            
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
    collector = MunhwaPoliticsCollector()
    await collector.run(num_pages=13)  # 13페이지에서 각각 12개씩 총 156개 수집

if __name__ == "__main__":
    asyncio.run(main())
