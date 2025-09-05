#!/usr/bin/env python3
"""
연합뉴스 정치 기사 크롤러
 - 기사 목록: https://www.yna.co.kr/politics/all/{page}
 - 본문: .story-news.article 내부 <p>
 - 발행시간: p.txt-time01
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

# 상위 디렉토리의 utils 모듈 import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_manager import SupabaseManager

console = Console()


class YonhapPoliticsCollector:
    def __init__(self):
        self.media_name = "연합뉴스"
        self.base_url = "https://www.yna.co.kr"
        self.list_url = "https://www.yna.co.kr/politics/all"
        self.supabase_manager = SupabaseManager()
        self.articles = []

    async def run(self, num_pages=10):
        console.print(f"🚀 {self.media_name} 정치 기사 크롤링 시작")
        await self.collect_articles(num_pages)
        await self.collect_contents()
        await self.save_articles()
        console.print("🎉 크롤링 완료!")

    async def collect_articles(self, num_pages):
        console.print(f"📄 {num_pages}개 페이지 수집 시작...")
        for page in range(1, num_pages + 1):
            url = f"{self.list_url}/{page}"
            page_articles = await self._get_page_articles(url)
            self.articles.extend(page_articles)
            console.print(f"📊 페이지 {page}: {len(page_articles)}개 기사 수집")

    async def _get_page_articles(self, url: str):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            articles = []
            for item in soup.select("div.item-box01")[:15]:  # 각 페이지 15개
                title_tag = item.select_one("a.tit-news span.title01")
                link_tag = item.select_one("a.tit-news")

                if not title_tag or not link_tag:
                    continue

                title = title_tag.get_text(strip=True)
                href = link_tag.get("href")
                article_url = href if href.startswith("http") else self.base_url + href

                articles.append({
                    "title": title,
                    "url": article_url,
                    "content": "",
                    "published_at": ""
                })
                console.print(f"📰 기사 발견: {title[:40]}...")

            return articles
        except Exception as e:
            console.print(f"❌ 목록 수집 실패: {e}")
            return []

    async def collect_contents(self):
        console.print(f"📖 본문 수집 시작 ({len(self.articles)}개)")
        async with httpx.AsyncClient(timeout=30) as client:
            for i, article in enumerate(self.articles, 1):
                try:
                    r = await client.get(article["url"], headers={"User-Agent": "Mozilla/5.0"})
                    r.raise_for_status()
                    soup = BeautifulSoup(r.text, "html.parser")

                    article["content"] = self.extract_content(soup)
                    article["published_at"] = self.extract_published_at(soup)

                    console.print(f"✅ [{i}] 본문 수집 성공: {article['title'][:40]}...")
                except Exception as e:
                    console.print(f"❌ [{i}] 본문 수집 실패: {e}")

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

    async def save_articles(self):
        console.print(f"💾 DB 저장 시작 ({len(self.articles)}개)")
        media_outlet = self.supabase_manager.get_media_outlet(self.media_name)
        media_id = media_outlet["id"] if media_outlet else self.supabase_manager.create_media_outlet(self.media_name)

        existing_urls = set()
        try:
            result = self.supabase_manager.client.table("articles").select("url").eq("media_id", media_id).execute()
            existing_urls = {a["url"] for a in result.data}
        except Exception as e:
            console.print(f"⚠️ 기존 URL 조회 실패: {e}")

        for i, article in enumerate(self.articles, 1):
            if article["url"] in existing_urls:
                console.print(f"⚠️ 중복 스킵: {article['title'][:40]}")
                continue

            data = {
                "title": article["title"],
                "url": article["url"],
                "content": article["content"],
                "published_at": article["published_at"],
                "created_at": datetime.now(pytz.UTC).isoformat(),
                "media_id": media_id,
            }
            success = self.supabase_manager.insert_article(data)
            if success:
                console.print(f"✅ 저장 완료: {article['title'][:40]}")
            else:
                console.print(f"❌ 저장 실패: {article['title'][:40]}")


async def main():
    collector = YonhapPoliticsCollector()
    await collector.run(num_pages=10)


if __name__ == "__main__":
    asyncio.run(main())
