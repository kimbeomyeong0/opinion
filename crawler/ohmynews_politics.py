#!/usr/bin/env python3
"""
오마이뉴스 정치 기사 크롤러 (5페이지 × 각 1개 기사)
개선사항:
- 리다이렉트 자동 처리
- 더 견고한 에러 핸들링
- 재시도 로직 추가
- 로깅 개선
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

# 프로젝트 루트에서 utils 불러오기
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_manager import SupabaseManager

console = Console()

class OhmyNewsPoliticsCollector:
    def __init__(self):
        self.media_name = "오마이뉴스"
        self.base_url = "https://www.ohmynews.com"
        self.list_url = "https://www.ohmynews.com/NWS_Web/Articlepage/Total_Article.aspx?PAGE_CD=C0400&pageno={}"
        self.supabase_manager = SupabaseManager()
        self.articles = []
        
        # HTTP 클라이언트 설정
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

    async def run(self, num_pages=10):
        console.print(f"🚀 {self.media_name} 정치 기사 크롤링 시작")

        await self.collect_articles(num_pages)
        await self.collect_contents()
        await self.save_articles()

        console.print("🎉 크롤링 완료!")

    async def collect_articles(self, num_pages):
        """목록에서 기사 수집"""
        console.print(f"📄 {num_pages} 페이지에서 기사 수집 시작...")
        
        for page_num in range(1, num_pages + 1):
            url = self.list_url.format(page_num)
            console.print(f"📡 페이지 {page_num}: {url}")

            try:
                soup = await self._fetch_soup(url)
                if not soup:
                    console.print(f"⚠️ 페이지 {page_num} 로드 실패")
                    continue

                # 뉴스 리스트에서 최대 20개 기사 가져오기
                news_list = soup.select(".news_list")
                if not news_list:
                    console.print(f"⚠️ 페이지 {page_num}에서 기사를 찾을 수 없음")
                    continue

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
                
                # 요청 간 딜레이
                await asyncio.sleep(1)

            except Exception as e:
                console.print(f"❌ 페이지 {page_num} 처리 중 오류: {str(e)}")
                continue

        console.print(f"📊 총 {len(self.articles)}개 기사 수집")

    async def collect_contents(self):
        """상세 페이지에서 본문 + 발행시간 수집"""
        console.print(f"📖 상세 기사 수집 시작 ({len(self.articles)}개)")

        for i, article in enumerate(self.articles, 1):
            console.print(f"📖 [{i}/{len(self.articles)}] {article['title'][:40]}...")
            
            try:
                data = await self._get_article_content(article["url"])
                article["published_at"] = data.get("published_at", "")
                article["content"] = data.get("content", "")
                
                # 요청 간 딜레이
                await asyncio.sleep(1.5)
                
            except Exception as e:
                console.print(f"❌ [{i}] 기사 수집 실패: {str(e)}")
                # 실패해도 계속 진행
                continue

    async def _fetch_soup(self, url: str, max_retries=3) -> BeautifulSoup:
        """URL에서 BeautifulSoup 객체를 가져오는 헬퍼 메서드"""
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(
                    timeout=30.0,
                    follow_redirects=True,  # 리다이렉트 자동 처리
                    limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
                ) as client:
                    response = await client.get(url, headers=self.headers)
                    response.raise_for_status()
                    return BeautifulSoup(response.text, "html.parser")
                    
            except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.ConnectError) as e:
                console.print(f"⚠️ 시도 {attempt + 1}/{max_retries} 실패: {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # 지수 백오프
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

    def _is_unwanted_text(self, text: str) -> bool:
        """원하지 않는 텍스트 패턴 확인"""
        unwanted_patterns = [
            r'^AD\s*$',
            r'광고',
            r'▶.*더보기',
            r'Copyright.*',
            r'저작권.*',
            r'무단.*전재.*',
            r'구독.*신청',
        ]
        
        for pattern in unwanted_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        return False

    async def save_articles(self):
        """DB 저장"""
        if not self.articles:
            console.print("⚠️ 저장할 기사가 없습니다.")
            return
            
        console.print(f"💾 Supabase에 {len(self.articles)}개 기사 저장 중...")

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

            success_count, skip_count, fail_count = 0, 0, 0
            
            for i, article in enumerate(self.articles, 1):
                if article["url"] in existing_urls:
                    console.print(f"⚠️ [{i}] 중복 스킵: {article['title'][:40]}")
                    skip_count += 1
                    continue

                # 빈 콘텐츠도 저장 (스킵하지 않음)

                article_data = {
                    "title": article["title"],
                    "url": article["url"],
                    "content": article["content"],
                    "published_at": article["published_at"] or datetime.now(pytz.UTC).isoformat(),
                    "created_at": datetime.now(pytz.UTC).isoformat(),
                    "media_id": media_id,
                }

                success = self.supabase_manager.insert_article(article_data)
                if success:
                    console.print(f"✅ [{i}] 저장 성공: {article['title'][:40]}")
                    success_count += 1
                else:
                    console.print(f"❌ [{i}] 저장 실패: {article['title'][:40]}")
                    fail_count += 1

            console.print(f"\n📊 저장 결과: 성공 {success_count}, 스킵 {skip_count}, 실패 {fail_count}")
            
        except Exception as e:
            console.print(f"❌ DB 저장 중 치명적 오류: {str(e)}")

async def main():
    collector = OhmyNewsPoliticsCollector()
    await collector.run(num_pages=10)

if __name__ == "__main__":
    asyncio.run(main())
