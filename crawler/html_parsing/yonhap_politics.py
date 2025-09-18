#!/usr/bin/env python3
"""
연합뉴스 정치 기사 크롤러 (성능 최적화 버전)
 - 기사 목록: https://www.yna.co.kr/politics/all/{page}
 - 본문: .story-news.article 내부 <p>
 - 발행시간: p.txt-time01
개선사항:
- 동시성 처리로 성능 대폭 개선
- 배치 DB 저장으로 효율성 향상
- 연결 풀 최적화
"""

import asyncio
import re
import sys
import os
from datetime import datetime
import pytz
import httpx
from bs4 import BeautifulSoup
from rich.console import Console
from typing import List, Dict, Optional

# 상위 디렉토리의 utils 모듈 import
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.supabase_manager import SupabaseManager

console = Console()


class YonhapPoliticsCollector:
    def __init__(self):
        self.media_name = "연합뉴스"
        self.base_url = "https://www.yna.co.kr"
        self.list_url = "https://www.yna.co.kr/politics/all"
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

    async def run(self, num_pages=10):
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
        console.print(f"📄 {num_pages}개 페이지에서 기사 수집 시작 (병렬 처리)...")
        
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
        url = f"{self.list_url}/{page_num}"
        console.print(f"📡 페이지 {page_num}: {url}")

        async with self.semaphore:  # 동시성 제한
            try:
                soup = await self._fetch_soup(url)
                if not soup:
                    console.print(f"⚠️ 페이지 {page_num} 로드 실패")
                    return 0

                articles = []
                for item in soup.select("div.item-box01")[:15]:  # 각 페이지 15개
                    title_tag = item.select_one("a.tit-news span.title01")
                    link_tag = item.select_one("a.tit-news")

                    if not title_tag or not link_tag:
                        continue

                    title = title_tag.get_text(strip=True)
                    href = link_tag.get("href")
                    article_url = href if href.startswith("http") else self.base_url + href

                    article = {
                        "title": title,
                        "url": article_url,
                        "content": "",
                        "published_at": ""
                    }
                    articles.append(article)
                    console.print(f"📰 발견: {title[:50]}...")

                self.articles.extend(articles)
                console.print(f"📄 페이지 {page_num}: {len(articles)}개 기사 수집")
                return len(articles)

            except Exception as e:
                console.print(f"❌ 페이지 {page_num} 처리 중 오류: {str(e)}")
                return 0

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
                soup = await self._fetch_soup(article["url"])
                if soup:
                    article["content"] = self.extract_content(soup)
                    article["published_at"] = self.extract_published_at(soup)
                    console.print(f"✅ [{index + 1}] 본문 수집 성공: {article['title'][:40]}...")
                else:
                    console.print(f"❌ [{index + 1}] 본문 수집 실패: {article['title'][:40]}...")
                
            except Exception as e:
                console.print(f"❌ [{index + 1}] 기사 수집 실패: {str(e)}")
                # 실패해도 계속 진행

    def extract_content(self, soup: BeautifulSoup) -> str:
        """연합뉴스 기사 본문 추출"""
        article = soup.select_one(".story-news.article")
        if not article:
            return ""

        # 광고/사진/aside/저작권 제거
        for tag in article.select("aside, figure, div.comp-box, p.txt-copyright"):
            tag.decompose()

        paragraphs = []
        for p in article.find_all("p"):
            text = p.get_text(" ", strip=True)
            if not text:
                continue
            # 불필요한 문구 제거
            if "저작권자" in text or "무단 전재" in text:
                continue
            if "제보는 카카오톡" in text:
                continue
            if re.match(r"^\[.*\]$", text):
                continue
            if re.search(r"[a-zA-Z0-9._%+-]+@yna\.co\.kr", text):
                continue
            paragraphs.append(text)

        return "\n\n".join(paragraphs).strip()

    def extract_published_at(self, soup: BeautifulSoup) -> str:
        """송고 시각 UTC 변환"""
        tag = soup.select_one("p.txt-time01")
        if not tag:
            return datetime.now(pytz.UTC).isoformat()

        text = tag.get_text(" ", strip=True)
        m = re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", text)
        if not m:
            return datetime.now(pytz.UTC).isoformat()

        kst = pytz.timezone("Asia/Seoul")
        dt = datetime.strptime(m.group(), "%Y-%m-%d %H:%M")
        dt = kst.localize(dt)
        return dt.astimezone(pytz.UTC).isoformat()

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
                
            console.print(f"\n📊 저장 결과: 성공 {len(new_articles)}, 스킵 {skip_count}, 짧은본문 제외 {short_content_count}")
            
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
    collector = YonhapPoliticsCollector()
    await collector.run(num_pages=10)


if __name__ == "__main__":
    asyncio.run(main())
