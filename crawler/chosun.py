#!/usr/bin/env python3
"""
조선일보 정치 기사 크롤러 (최신 50개 + 본문 전문 수집)
- API로 최신 50개 가져옴
- Playwright로 본문 전문 크롤링
- published_at → 한국시간(KST) 변환
"""

import asyncio
import json
import sys
import os
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin
import httpx
import pytz
from bs4 import BeautifulSoup
from rich.console import Console
from rich.table import Table
from playwright.async_api import async_playwright

# 프로젝트 루트 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 내부 모듈
from utils.supabase_manager import SupabaseManager

console = Console()
KST = pytz.timezone("Asia/Seoul")


class ChosunPoliticsCollector:
    def __init__(self):
        self.base_url = "https://www.chosun.com"
        self.media_name = "조선일보"
        self.media_bias = "right"
        self.supabase_manager = SupabaseManager()
        self.articles: List[Dict] = []
        self._playwright = None
        self._browser = None

    async def _collect_latest_articles(self):
        """API 기반 최신 100개 기사 수집 (중복 제외)"""
        console.print("🔌 API를 통한 최신 기사 100개 수집 시작...")
        api_base = "https://www.chosun.com/pf/api/v3/content/fetch/story-feed"
        target_count = 100
        offset = 0
        size = 50
        max_attempts = 5  # 최대 5번 API 호출

        async with httpx.AsyncClient(timeout=10.0) as client:
            for attempt in range(max_attempts):
                if len(self.articles) >= target_count:
                    break
                    
                try:
                    console.print(f"📡 API 호출 {attempt + 1}/{max_attempts} (offset: {offset})")
                    
                    query_params = {
                        "query": json.dumps({
                            "excludeContentTypes": "gallery, video",
                            "includeContentTypes": "story",
                            "includeSections": "/politics",
                            "offset": offset,
                            "size": size
                        }),
                        "_website": "chosun"
                    }
                    
                    resp = await client.get(api_base, params=query_params)
                    resp.raise_for_status()
                    data = resp.json()

                    content_elements = data.get("content_elements", [])
                    console.print(f"📊 API 응답: {len(content_elements)}개 요소 수신")
                    
                    if not content_elements:
                        console.print("⚠️ 더 이상 기사가 없습니다")
                        break
                    
                    parsed_count = 0
                    added_count = 0
                    
                    for element in content_elements:
                        if len(self.articles) >= target_count:
                            break
                            
                        article = self._parse_api_article(element)
                        if article:
                            parsed_count += 1
                            if self._add_article(article):
                                added_count += 1
                    
                    console.print(f"📈 파싱 성공: {parsed_count}개, 최종 추가: {added_count}개")
                    console.print(f"📊 현재 수집된 기사: {len(self.articles)}개")
                    
                    # 다음 offset으로 이동
                    offset += size
                    
                    # API 호출 간 잠시 대기
                    await asyncio.sleep(0.5)

                except Exception as e:
                    console.print(f"❌ API 호출 오류 (시도 {attempt + 1}): {e}")
                    offset += size
                    continue

        console.print(f"🎯 수집 완료: {len(self.articles)}개 기사 (목표: {target_count}개)")

    def _parse_api_article(self, element: Dict) -> Optional[Dict]:
        """API 응답 파싱"""
        try:
            title = element.get("headlines", {}).get("basic")
            if not title:
                return None

            canonical_url = element.get("canonical_url")
            if not canonical_url:
                return None
            url = urljoin(self.base_url, canonical_url) if canonical_url.startswith("/") else canonical_url

            # 날짜 → 한국시간 변환 (조선일보 API 형식 대응)
            display_date = element.get("display_date")
            if display_date:
                try:
                    # 조선일보 API 날짜 형식 정규화
                    # 예: "2025-09-02T09:39:49.26Z" → "2025-09-02T09:39:49.260000Z"
                    normalized_date = display_date
                    
                    # Z로 끝나는 경우 처리
                    if normalized_date.endswith("Z"):
                        # 소수점 부분 정규화
                        if "." in normalized_date:
                            # T와 Z 사이의 부분 추출
                            t_index = normalized_date.find("T")
                            z_index = normalized_date.find("Z")
                            time_part = normalized_date[t_index+1:z_index]
                            
                            if "." in time_part:
                                # 소수점을 6자리로 맞춤
                                seconds_part = time_part.split(".")[0]
                                decimal_part = time_part.split(".")[1]
                                decimal_part = decimal_part.ljust(6, "0")[:6]  # 6자리로 맞춤
                                normalized_date = normalized_date[:t_index+1] + seconds_part + "." + decimal_part + "Z"
                        
                        # Z를 +00:00로 변환
                        normalized_date = normalized_date.replace("Z", "+00:00")
                    
                    # ISO 형식 파싱
                    dt_utc = datetime.fromisoformat(normalized_date)
                    
                    # 한국시간으로 변환 (naive datetime으로 저장)
                    published_at = dt_utc.astimezone(KST).replace(tzinfo=None)
                        
                except Exception as e:
                    console.print(f"⚠️ 날짜 변환 실패: {display_date} - {e}")
                    published_at = None
            else:
                published_at = None

            return {
                "title": title.strip(),
                "url": url,
                "content": "",  # 본문은 나중에 Playwright로 채움
                "published_at": published_at,
                "created_at": published_at,  # 발행 시간과 동일하게 설정
            }
        except Exception:
            return None

    def _add_article(self, article: Dict) -> bool:
        """중복 제거 후 추가"""
        if not article.get("url"):
            return False
        if any(a["url"] == article["url"] for a in self.articles):
            return False
        self.articles.append(article)
        return True

    async def _extract_content(self, url: str) -> str:
        """Playwright로 본문 전문 추출 (메모리 최적화)"""
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
            # 메모리 사용량 최소화
            await page.set_viewport_size({"width": 1280, "height": 720})
            await page.goto(url, wait_until="domcontentloaded", timeout=10000)

            # 조선일보 본문 추출 (더 유연한 방법)
            content = ""
            
            # 방법 1: JavaScript로 직접 본문 추출
            try:
                content = await page.evaluate('''() => {
                    // 조선일보 본문 선택자들
                    const selectors = [
                        'section.article-body p',
                        'div#article-body p',
                        'div.article-body p',
                        'article.article-body p',
                        '.story-news p',
                        '.article-content p',
                        'main p',
                        'article p'
                    ];
                    
                    for (const selector of selectors) {
                        const paragraphs = document.querySelectorAll(selector);
                        if (paragraphs.length > 0) {
                            const texts = Array.from(paragraphs)
                                .map(p => p.textContent.trim())
                                .filter(text => text.length > 20)
                                .slice(0, 20); // 최대 20개
                            
                            if (texts.length > 0) {
                                return texts.join('\\n\\n');
                            }
                        }
                    }
                    
                    // 모든 p 태그에서 본문 찾기 (마지막 수단)
                    const allP = document.querySelectorAll('p');
                    const texts = Array.from(allP)
                        .map(p => p.textContent.trim())
                        .filter(text => text.length > 50) // 더 긴 텍스트만
                        .slice(0, 15); // 최대 15개
                    
                    return texts.join('\\n\\n');
                }''')
                
                if content and len(content.strip()) > 50:
                    return content.strip()
                    
            except Exception as e:
                console.print(f"⚠️ JavaScript 본문 추출 실패: {str(e)[:50]}")
            
            # 방법 2: BeautifulSoup으로 HTML 파싱 (백업)
            try:
                html = await page.content()
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html, 'html.parser')
                
                # 조선일보 본문 후보들
                content_selectors = [
                    'section.article-body',
                    'div#article-body',
                    'div.article-body',
                    'article.article-body',
                    '.story-news',
                    '.article-content'
                ]
                
                for selector in content_selectors:
                    content_elem = soup.select_one(selector)
                    if content_elem:
                        paragraphs = content_elem.find_all('p')
                        if paragraphs:
                            texts = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20]
                            if texts:
                                content = '\n\n'.join(texts[:20])
                                break
                                
            except Exception as e:
                console.print(f"⚠️ BeautifulSoup 추출 실패: {str(e)[:50]}")

            return content.strip()
            
        except Exception as e:
            console.print(f"❌ 본문 추출 실패 ({url[:50]}...): {str(e)[:50]}")
            return ""
        finally:
            if page:
                try:
                    await page.close()
                except:
                    pass

    async def collect_contents(self):
        """본문 전문 수집 (배치 처리로 메모리 최적화)"""
        if not self.articles:
            return

        console.print(f"📖 본문 수집 시작: {len(self.articles)}개 기사 (배치 처리)")
        
        # 3개씩 배치로 처리 (맥북 에어 M2에 최적화)
        batch_size = 3
        success_count = 0
        
        for i in range(0, len(self.articles), batch_size):
            batch = self.articles[i:i + batch_size]
            console.print(f"📄 배치 {i//batch_size + 1}/{(len(self.articles) + batch_size - 1)//batch_size} 처리 중...")
            
            # 배치 내에서 순차 처리 (메모리 절약)
            for j, art in enumerate(batch):
                try:
                    content = await self._extract_content(art["url"])
                    if content:
                        self.articles[i + j]["content"] = content
                        success_count += 1
                        console.print(f"✅ [{i + j + 1}/{len(self.articles)}] 본문 수집 성공")
                    else:
                        console.print(f"⚠️ [{i + j + 1}/{len(self.articles)}] 본문 수집 실패")
                    
                    # 각 기사 간 잠시 대기 (시스템 부하 방지)
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    console.print(f"❌ [{i + j + 1}/{len(self.articles)}] 오류: {str(e)[:50]}")
            
            # 배치 간 대기 (메모리 정리 시간)
            if i + batch_size < len(self.articles):
                console.print("⏳ 배치 간 대기 중... (메모리 정리)")
                await asyncio.sleep(2)

        console.print(f"✅ 본문 수집 완료: {success_count}/{len(self.articles)}개 성공")

    async def save_to_supabase(self):
        """DB 저장 (중복 자동 처리)"""
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

        # 모든 URL을 한 번에 조회하여 중복 체크 (효율성 개선)
        urls = [art["url"] for art in self.articles]
        console.print("🔍 기존 기사 중복 체크 중...")
        
        try:
            existing_urls = set()
            # 배치로 중복 체크 (한 번에 너무 많이 조회하지 않도록)
            batch_size = 20
            for i in range(0, len(urls), batch_size):
                batch_urls = urls[i:i + batch_size]
                for url in batch_urls:
                    exists = self.supabase_manager.client.table("articles").select("url").eq("url", url).execute()
                    if exists.data:
                        existing_urls.add(url)
            
            console.print(f"📊 중복 체크 완료: {len(existing_urls)}개 중복 발견")
            
        except Exception as e:
            console.print(f"⚠️ 중복 체크 중 오류: {e}")
            existing_urls = set()

        success, failed, skipped = 0, 0, 0
        
        for i, art in enumerate(self.articles, 1):
            try:
                # 중복 체크
                if art["url"] in existing_urls:
                    console.print(f"⚠️ [{i}/{len(self.articles)}] 중복 기사 스킵: {art['title'][:30]}...")
                    skipped += 1
                    continue

                # 한국시간을 문자열로 변환 (시간대 정보 제거)
                published_at_str = None
                created_at_str = None
                
                if isinstance(art["published_at"], datetime):
                    # naive datetime을 문자열로 변환 (시간대 정보 없이)
                    published_at_str = art["published_at"].strftime('%Y-%m-%d %H:%M:%S')
                    created_at_str = art["created_at"].strftime('%Y-%m-%d %H:%M:%S') if art.get("created_at") else published_at_str
                elif art.get("published_at"):
                    # 이미 문자열인 경우
                    published_at_str = art["published_at"]
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
        """Playwright 리소스 정리"""
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

    async def run(self):
        """실행 (에러 처리 강화)"""
        try:
            console.print(f"🚀 조선일보 정치 기사 크롤링 시작 (최신 100개)")
            console.print("💡 맥북 에어 M2 최적화 모드로 실행됩니다")
            
            await self._collect_latest_articles()
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
    collector = ChosunPoliticsCollector()
    await collector.run()

if __name__ == "__main__":
    asyncio.run(main())
