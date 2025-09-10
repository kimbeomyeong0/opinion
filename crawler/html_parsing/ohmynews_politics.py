#!/usr/bin/env python3
"""
오마이뉴스 정치 기사 크롤러 (성능 최적화 버전)
개선사항:
- 동시성 처리로 성능 대폭 개선
- 배치 DB 저장으로 효율성 향상
- 딜레이 최적화
- 연결 풀 최적화
"""

import asyncio
import sys
import os
from datetime import datetime
import pytz
import httpx
from bs4 import BeautifulSoup
from rich.console import Console
import time
import re
from typing import List, Dict, Optional

# 프로젝트 루트에서 utils 불러오기
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.supabase_manager import SupabaseManager

console = Console()

class OhmyNewsPoliticsCollector:
    def __init__(self):
        self.media_name = "오마이뉴스"
        self.base_url = "https://www.ohmynews.com"
        self.list_url = "https://www.ohmynews.com/NWS_Web/Articlepage/Total_Article.aspx?PAGE_CD=C0400&pageno={}"
        self.supabase_manager = SupabaseManager()
        self.articles = []
        
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

    async def run(self, num_pages=8):
        console.print(f"🚀 {self.media_name} 정치 기사 크롤링 시작 (최적화 버전)")

        # 1단계: 기사 목록 수집 (병렬 처리)
        await self.collect_articles_parallel(num_pages)
        
        # 2단계: 본문 수집 (병렬 처리)
        await self.collect_contents_parallel()
        
        # 3단계: 배치 저장
        await self.save_articles_batch()

        console.print("🎉 크롤링 완료!")

    async def collect_articles_parallel(self, num_pages):
        """목록에서 기사 수집 (병렬 처리)"""
        console.print(f"📄 {num_pages} 페이지에서 기사 수집 시작 (병렬 처리)...")
        
        # 모든 페이지를 동시에 처리
        tasks = [self._collect_page_articles(page_num) for page_num in range(1, num_pages + 1)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 수집
        total_articles = 0
        for i, result in enumerate(results, 1):
            if isinstance(result, Exception):
                console.print(f"❌ 페이지 {i} 처리 중 오류: {str(result)}")
            else:
                total_articles += result
                
        console.print(f"📊 총 {total_articles}개 기사 수집")

    async def _collect_page_articles(self, page_num: int) -> int:
        """단일 페이지에서 기사 수집"""
        url = self.list_url.format(page_num)
        console.print(f"📡 페이지 {page_num}: {url}")

        async with self.semaphore:  # 동시성 제한
            try:
                soup = await self._fetch_soup(url)
                if not soup:
                    console.print(f"⚠️ 페이지 {page_num} 로드 실패")
                    return 0

                # 뉴스 리스트에서 최대 20개 기사 가져오기
                news_list = soup.select(".news_list")
                if not news_list:
                    console.print(f"⚠️ 페이지 {page_num}에서 기사를 찾을 수 없음")
                    return 0

                page_articles = 0
                for news_item in news_list[:20]:  # 최대 20개
                    link = news_item.select_one("dt a")
                    if not link:
                        continue

                    title = link.get_text(strip=True)
                    href = link.get("href")

                    if href.startswith("/"):
                        href = f"{self.base_url}{href}"

                    article = {
                        "title": title,
                        "url": href,
                        "published_at": "",
                        "content": "",
                    }
                    self.articles.append(article)
                    page_articles += 1
                    console.print(f"📰 발견: {title[:50]}...")

                console.print(f"📄 페이지 {page_num}: {page_articles}개 기사 수집")
                return page_articles

            except Exception as e:
                console.print(f"❌ 페이지 {page_num} 처리 중 오류: {str(e)}")
                return 0

    async def collect_contents_parallel(self):
        """상세 페이지에서 본문 + 발행시간 수집 (병렬 처리)"""
        console.print(f"📖 상세 기사 수집 시작 ({len(self.articles)}개) - 병렬 처리")

        # 모든 기사를 동시에 처리 (배치로 나누어서)
        batch_size = 20  # 한 번에 처리할 기사 수
        total_batches = (len(self.articles) + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(self.articles))
            batch_articles = self.articles[start_idx:end_idx]
            
            console.print(f"📖 배치 {batch_num + 1}/{total_batches}: {len(batch_articles)}개 기사 처리 중...")
            
            # 배치 내에서 병렬 처리
            tasks = [self._collect_single_article(i + start_idx, article) for i, article in enumerate(batch_articles)]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # 배치 간 짧은 딜레이 (서버 부하 방지)
            if batch_num < total_batches - 1:
                await asyncio.sleep(0.5)

    async def _collect_single_article(self, index: int, article: Dict):
        """단일 기사 본문 수집"""
        console.print(f"📖 [{index + 1}/{len(self.articles)}] {article['title'][:40]}...")
        
        async with self.semaphore:  # 동시성 제한
            try:
                data = await self._get_article_content(article["url"])
                article["published_at"] = data.get("published_at", "")
                article["content"] = data.get("content", "")
                
            except Exception as e:
                console.print(f"❌ [{index + 1}] 기사 수집 실패: {str(e)}")
                # 실패해도 계속 진행

    async def _fetch_soup(self, url: str, max_retries=2) -> Optional[BeautifulSoup]:
        """URL에서 BeautifulSoup 객체를 가져오는 헬퍼 메서드 (최적화)"""
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(
                    timeout=15.0,  # 타임아웃 단축
                    follow_redirects=True,
                    limits=httpx.Limits(max_keepalive_connections=20, max_connections=50)  # 연결 풀 증가
                ) as client:
                    response = await client.get(url, headers=self.headers)
                    response.raise_for_status()
                    return BeautifulSoup(response.text, "html.parser")
                    
            except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.ConnectError) as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5)  # 재시도 딜레이 단축
                else:
                    console.print(f"❌ {url} 최대 재시도 횟수 초과")
                    return None
            except Exception as e:
                console.print(f"❌ 예상치 못한 오류: {str(e)}")
                return None

    async def _get_article_content(self, url: str):
        """본문과 발행시간 추출"""
        soup = await self._fetch_soup(url)
        if not soup:
            return {"published_at": "", "content": ""}

        result = {"published_at": "", "content": ""}

        try:
            # 1. 발행 시간 추출 (여러 패턴 시도)
            result["published_at"] = self._extract_publish_date(soup)

            # 2. 본문 추출
            result["content"] = self._extract_content(soup)
            
            # 디버깅을 위한 로그 (필요시 주석 해제)
            # if not result["content"]:
            #     console.print(f"⚠️ 본문 추출 실패: {url}")
            #     console.print(f"HTML 구조 확인 필요")

        except Exception as e:
            console.print(f"⚠️ 콘텐츠 추출 중 오류: {str(e)}")

        return result

    def _extract_publish_date(self, soup: BeautifulSoup) -> str:
        """발행 날짜 추출"""
        date_selectors = [
            "span.date",
            ".article_date",
            ".date_time",
            ".publish_time"
        ]
        
        for selector in date_selectors:
            date_elem = soup.select_one(selector)
            if date_elem:
                time_str = date_elem.get_text(strip=True)
                # "최종 업데이트", "기사입력" 등의 텍스트 제거
                time_str = re.sub(r'(최종\s*업데이트|기사입력|수정)', '', time_str).strip()
                
                # 다양한 날짜 형식 파싱 시도
                date_formats = [
                    "%y.%m.%d %H:%M",
                    "%Y.%m.%d %H:%M",
                    "%Y-%m-%d %H:%M",
                    "%m/%d %H:%M",
                ]
                
                for fmt in date_formats:
                    try:
                        dt = datetime.strptime(time_str, fmt)
                        # 연도가 없는 경우 현재 연도 사용
                        if dt.year == 1900:
                            dt = dt.replace(year=datetime.now().year)
                            
                        kst = pytz.timezone("Asia/Seoul")
                        dt = kst.localize(dt)
                        return dt.astimezone(pytz.UTC).isoformat()
                    except ValueError:
                        continue
                        
                console.print(f"⚠️ 시간 파싱 실패: {time_str}")
                break
                
        return ""

    def _extract_content(self, soup: BeautifulSoup) -> str:
        """본문 추출"""
        # 오마이뉴스 본문 영역 찾기
        content_div = soup.select_one('div.at_contents[itemprop="articleBody"]')
        
        if not content_div:
            return ""
        
        # 불필요한 태그 제거
        for tag in content_div.find_all(['figure', 'script', 'style', 'iframe', 'button', 'img', 'video', 'audio']):
            tag.decompose()
        
        # 광고성 div 제거
        for tag in content_div.find_all('div', class_=lambda x: x and ('ad' in x.lower() or 'advertisement' in x.lower())):
            tag.decompose()
        
        # 텍스트만 추출
        text = content_div.get_text(separator="\n", strip=True)
        
        # 불필요한 공백 정리
        text = "\n".join([line.strip() for line in text.splitlines() if line.strip()])
        
        return text


    async def save_articles_batch(self):
        """DB 배치 저장 (최적화)"""
        if not self.articles:
            console.print("⚠️ 저장할 기사가 없습니다.")
            return
            
        console.print(f"💾 Supabase에 {len(self.articles)}개 기사 배치 저장 중...")

        try:
            media_outlet = self.supabase_manager.get_media_outlet(self.media_name)
            if media_outlet:
                media_id = media_outlet["id"]
            else:
                media_id = self.supabase_manager.create_media_outlet(self.media_name)

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
                    
                article_data = {
                    "title": article["title"],
                    "url": article["url"],
                    "content": article["content"],
                    "published_at": article["published_at"] or datetime.now(pytz.UTC).isoformat(),
                    "created_at": datetime.now(pytz.UTC).isoformat(),
                    "media_id": media_id,
                }
                new_articles.append(article_data)

            # 배치 저장
            if new_articles:
                success_count = self._batch_insert_articles(new_articles)
                console.print(f"✅ 배치 저장 완료: {success_count}개 성공")
            else:
                console.print("⚠️ 저장할 새 기사가 없습니다.")
                
            console.print(f"\n📊 저장 결과: 성공 {len(new_articles)}, 스킵 {skip_count}")
            
        except Exception as e:
            console.print(f"❌ DB 저장 중 치명적 오류: {str(e)}")

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

async def main():
    collector = OhmyNewsPoliticsCollector()
    await collector.run(num_pages=8)  # 8페이지에서 각각 20개씩 총 160개 수집 (150개 목표)

if __name__ == "__main__":
    asyncio.run(main())
