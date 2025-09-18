#!/usr/bin/env python3
"""
경향신문 정치 기사 크롤러 (HTML 파싱 기반)
- HTML 파싱으로 완전 재작성하여 속도 대폭 개선
- httpx + BeautifulSoup 사용
- 병렬 처리로 성능 최적화
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta
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

class KhanPoliticsCollector:
    def __init__(self):
        self.base_url = "https://www.khan.co.kr"
        self.politics_url = "https://www.khan.co.kr/politics"
        self.media_name = "경향신문"
        self.media_bias = "center-left"
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
    def _get_page_urls(self, num_pages: int = 15) -> List[str]:
        """페이지 URL 목록 생성"""
        urls = []
        for page in range(1, num_pages + 1):
            url = f"{self.politics_url}?page={page}"
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
                    
                    # 기사 목록 추출 (ul#recentList li article)
                    recent_list = soup.find('ul', id='recentList')
                    if not recent_list:
                        console.print(f"❌ 페이지 {page_num}: recentList를 찾을 수 없습니다")
                        return []
                    
                    article_items = recent_list.find_all('li')
                    
                    for li in article_items:
                        article_element = li.find('article')
                        if not article_element:
                            continue
                            
                        try:
                            # 제목과 URL 추출 (div > a)
                            link = article_element.find('a', href=True)
                            if not link:
                                continue
                                
                            href = link.get('href')
                            if not href:
                                continue
                            
                            # 상대 URL을 절대 URL로 변환
                            if href.startswith('http'):
                                full_url = href
                            else:
                                full_url = urljoin(self.base_url, href)
                            
                            title = link.get_text(strip=True)
                            
                            # 요약 추출 (p.desc)
                            desc_element = article_element.find('p', class_='desc')
                            description = desc_element.get_text(strip=True) if desc_element else ""
                            
                            # 날짜 추출 (p.date)
                            date_element = article_element.find('p', class_='date')
                            date_text = date_element.get_text(strip=True) if date_element else ""
                            published_at = self._parse_relative_time(date_text)
                            
                            # 이미지 정보 추출 (선택적)
                            img_element = article_element.find('img')
                            image_url = ""
                            image_alt = ""
                            if img_element:
                                image_url = img_element.get('src', '')
                                image_alt = img_element.get('alt', '')
                                if image_url and not image_url.startswith('http'):
                                    image_url = urljoin(self.base_url, image_url)
                            
                            if title and len(title) > 10:
                                article = {
                                    'title': title,
                                    'url': full_url,
                                    'content': '',
                                    'published_at': published_at,
                                    'description': description,
                                    'image_url': image_url,
                                    'image_alt': image_alt
                                }
                                articles.append(article)
                                console.print(f"📰 발견: {title[:50]}...")
                        
                        except Exception as e:
                            console.print(f"⚠️ 기사 아이템 처리 중 오류: {str(e)}")
                            continue
                    
                    console.print(f"📄 페이지 {page_num}: {len(articles)}개 기사 수집")
                    return articles
                    
            except Exception as e:
                console.print(f"❌ 페이지 {page_num} 처리 중 오류: {str(e)}")
                return []

    def _parse_relative_time(self, time_text: str) -> str:
        """상대 시간 텍스트를 UTC ISO 형식으로 변환"""
        try:
            if not time_text:
                return datetime.now(pytz.UTC).isoformat()
            
            now = datetime.now(KST)
            
            # "26분 전", "2시간 전", "1일 전" 등 처리
            if "분 전" in time_text:
                minutes = int(re.search(r'(\d+)분', time_text).group(1))
                target_time = now - timedelta(minutes=minutes)
            elif "시간 전" in time_text:
                hours = int(re.search(r'(\d+)시간', time_text).group(1))
                target_time = now - timedelta(hours=hours)
            elif "일 전" in time_text:
                days = int(re.search(r'(\d+)일', time_text).group(1))
                target_time = now - timedelta(days=days)
            elif "주 전" in time_text:
                weeks = int(re.search(r'(\d+)주', time_text).group(1))
                target_time = now - timedelta(weeks=weeks)
            else:
                # 절대 시간 형식 시도 (YYYY.MM.DD HH:MM)
                if re.match(r'\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}', time_text):
                    target_time = datetime.strptime(time_text, "%Y.%m.%d %H:%M")
                    target_time = KST.localize(target_time)
                else:
                    target_time = now
            
            return target_time.astimezone(pytz.UTC).isoformat()
            
        except Exception as e:
            console.print(f"⚠️ 시간 파싱 실패: {time_text} - {str(e)}")
            return datetime.now(pytz.UTC).isoformat()

    async def collect_articles_parallel(self, num_pages: int = 15):
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
            
            # 발행시간 추출 (더 정확한 시간이 있으면 업데이트)
            published_at = self._extract_published_at(soup)
            if published_at:
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
            # 1. time 태그에서 추출
            time_element = soup.find('time', datetime=True)
            if time_element:
                return self._parse_datetime(time_element.get('datetime', ''))
            
            # 2. meta 태그에서 추출
            meta_date = soup.find('meta', property='article:published_time')
            if meta_date:
                return self._parse_datetime(meta_date.get('content', ''))
            
            # 3. 기사 날짜 영역에서 추출
            date_selectors = [
                            'a[title*="기사 입력/수정일"]',
                            '.article-date',
                            '.date',
                '.publish_date'
            ]
            
            for selector in date_selectors:
                element = soup.select_one(selector)
                if element:
                    # 입력/수정 시간 구분하여 추출
                    paragraphs = element.find_all('p')
                    for p in paragraphs:
                        text = p.get_text(strip=True)
                        if '입력' in text:
                            time_text = text.replace('입력', '').strip()
                            if time_text:
                                return self._parse_datetime(time_text)
            
            return datetime.now(pytz.UTC).isoformat()
            
        except Exception as e:
            console.print(f"⚠️ 발행시간 추출 실패: {str(e)}")
            return datetime.now(pytz.UTC).isoformat()

    def _extract_content_text(self, soup: BeautifulSoup) -> str:
        """경향신문 본문 텍스트 추출 (p.content_text만 추출)"""
        try:
            # #articleBody 찾기
            article_body = soup.find('div', id='articleBody')
            if not article_body:
                console.print("⚠️ #articleBody를 찾을 수 없습니다")
                return ""
            
            # 제외할 요소들 완전 제거
            exclude_selectors = [
                'div.editor-subtitle',  # 부제목
                'div.art_photo',  # 사진 영역
                'p.caption',  # 사진 설명
                'div[class*="banner-article"]',  # 광고 배너 (banner-article-left, banner-article-right 등)
                'div.srch-kw',  # 관련 키워드
                'div[class*="banner"]', 'div[class*="ad"]', 'div[class*="advertisement"]',
                'script', 'style', 'noscript', 'iframe'
            ]
            
            for selector in exclude_selectors:
                elements = article_body.select(selector)
                for el in elements:
                    el.decompose()
            
            # p.content_text 태그만 선택적으로 추출
            paragraphs = []
            content_paragraphs = article_body.select('p.content_text')
            
            for p in content_paragraphs:
                text = p.get_text(strip=True)
                
                # HTML 엔티티 처리 및 정규화
                text = re.sub(r'&nbsp;', ' ', text)  # &nbsp; 제거
                text = re.sub(r'\s+', ' ', text)  # 연속 공백 정규화
                text = text.strip()
                
                # 필터링: 기자명, 이메일, 출처 제거
                if (text and 
                    len(text) > 10 and  # 10자 이상
                    not re.search(r'[가-힣]+\s*기자', text) and  # 기자명 제외
                    not re.search(r'[가-힣]+\s*특파원', text) and  # 특파원 제외
                    not re.search(r'[가-힣]+\s*통신원', text) and  # 통신원 제외
                    '@' not in text and  # 이메일 제외
                    '[출처:' not in text and  # 출처 제외
                    '[경향신문]' not in text):  # 출처 제외
                    paragraphs.append(text)
            
            if not paragraphs:
                console.print("⚠️ 추출할 본문이 없습니다")
                return ""
            
            # 문단들을 줄바꿈으로 연결
            combined_text = '\n\n'.join(paragraphs)
            
            # 최종 정규화
            combined_text = re.sub(r'\n\s*\n', '\n\n', combined_text)  # 연속 줄바꿈 정규화
            
            return combined_text.strip()
            
        except Exception as e:
            console.print(f"⚠️ 본문 추출 실패: {str(e)}")
            return ""

    def _parse_datetime(self, datetime_str: str) -> str:
        """날짜시간 문자열 파싱"""
        try:
            clean_time = datetime_str.strip()
            
            # ISO 형식 (2025-09-18T14:07:01+09:00)
            if 'T' in clean_time:
                if '+' in clean_time:
                    published_at = datetime.fromisoformat(clean_time)
                    return published_at.astimezone(pytz.UTC).isoformat()
            else:
                    published_at = datetime.fromisoformat(clean_time.replace('Z', '+00:00'))
                    return published_at.isoformat()
            
            # 일반 형식 (2025.09.18 14:07)
            if re.match(r'^\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}$', clean_time):
                kst_time = datetime.strptime(clean_time, "%Y.%m.%d %H:%M")
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
        
    async def run(self, num_pages: int = 15):
        """실행"""
        try:
            console.print(f"🚀 경향신문 정치 기사 크롤링 시작 (최대 {num_pages}페이지)")
            
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
    collector = KhanPoliticsCollector()
    await collector.run(num_pages=15)  # 15페이지에서 각각 10개씩 총 150개 기사 수집

if __name__ == "__main__":
    asyncio.run(main())