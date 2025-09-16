#!/usr/bin/env python3
"""
뉴스원 정치 기사 크롤러 (API 기반)
- News1 API를 사용하여 정치 섹션 기사 수집
- 기사 1개 수집 기능
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
from rich.console import Console
from playwright.async_api import async_playwright

# 프로젝트 루트 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 내부 모듈
from utils.supabase_manager import SupabaseManager

console = Console()
KST = pytz.timezone("Asia/Seoul")


class NewsonePoliticsCollector:
    def __init__(self):
        self.base_url = "https://www.news1.kr"
        self.api_url = "https://rest.news1.kr/v6/section/politics/latest"
        self.media_name = "뉴스원"
        self.media_bias = "center"
        self.supabase_manager = SupabaseManager()
        self.articles: List[Dict] = []
        self._playwright = None
        self._browser = None
        self._semaphore = asyncio.Semaphore(5)  # 동시 처리 제한 증가

    async def _get_politics_articles(self, total_limit: int = 150) -> List[Dict]:
        """정치 섹션 기사 목록 수집 - 병렬 처리로 최적화"""
        console.print(f"🔌 뉴스원 정치 섹션 기사 수집 시작 (최대 {total_limit}개)")
        
        # 병렬 API 호출을 위한 태스크 생성
        async def fetch_page(start_page: int) -> List[Dict]:
            try:
                params = {"start": start_page, "limit": 10}
                async with httpx.AsyncClient(timeout=5.0) as client:  # 타임아웃 단축
                    resp = await client.get(self.api_url, params=params)
                    resp.raise_for_status()
                    data = resp.json()
                    console.print(f"📊 API 응답 (start={start_page}): {len(data)}개 기사 수신")
                    return data
            except Exception as e:
                console.print(f"❌ API 호출 오류 (start={start_page}): {e}")
                return []
        
        # 병렬 처리로 15개 페이지 동시 요청
        tasks = [fetch_page(start_page) for start_page in range(1, 16)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 수집
        all_articles = []
        for i, result in enumerate(results, 1):
            if isinstance(result, Exception):
                console.print(f"❌ 페이지 {i} 처리 실패: {result}")
            else:
                all_articles.extend(result)
        
        console.print(f"📈 총 수집된 기사: {len(all_articles)}개")
        return all_articles[:total_limit]  # 제한 적용

    def _parse_article_data(self, article_data: Dict) -> Optional[Dict]:
        """API 응답 데이터 파싱"""
        try:
            # 필수 필드 확인
            title = article_data.get("title")
            if not title:
                return None

            # URL 처리
            url_path = article_data.get("url", "")
            if url_path.startswith("/"):
                url = urljoin(self.base_url, url_path)
            else:
                url = url_path

            # 날짜 처리
            published_at = None
            pubdate = article_data.get("pubdate")
            if pubdate:
                try:
                    # "2025-09-04 22:31:19" 형식을 ISO 형식으로 변환
                    dt = datetime.strptime(pubdate, "%Y-%m-%d %H:%M:%S")
                    # KST 시간대로 변환
                    dt_kst = KST.localize(dt)
                    published_at = dt_kst.isoformat()
                except Exception as e:
                    console.print(f"⚠️ 날짜 변환 실패: {pubdate} - {e}")
                    published_at = None

            # 기자 정보
            author = article_data.get("author", "")

            # 섹션 정보 (정치로 고정)
            section = "정치"

            # 태그 정보 (없음)
            tag_list = []

            # 요약 정보
            summary = article_data.get("summary", "")

            return {
                "title": title.strip(),
                "url": url,
                "content": "",  # 본문은 나중에 Playwright로 채움
                "published_at": published_at,
                "created_at": datetime.now(KST).isoformat(),
                "author": author,
                "section": section,
                "tags": tag_list,
                "description": summary,
            }
        except Exception as e:
            console.print(f"❌ 데이터 파싱 실패: {e}")
            return None

    async def _collect_articles(self, total_limit: int = 150):
        """기사 수집"""
        console.print(f"🚀 뉴스원 정치 기사 수집 시작 (최대 {total_limit}개)")
        
        # API에서 기사 목록 수집
        articles_data = await self._get_politics_articles(total_limit)
        
        if not articles_data:
            console.print("❌ 수집할 기사가 없습니다.")
            return

        # 각 기사 데이터 파싱
        success_count = 0
        for i, article_data in enumerate(articles_data, 1):
            parsed_article = self._parse_article_data(article_data)
            if parsed_article:
                self.articles.append(parsed_article)
                success_count += 1
                # 진행률 표시 (10개마다)
                if i % 10 == 0 or i == len(articles_data):
                    console.print(f"✅ [{i}/{len(articles_data)}] {parsed_article['title'][:50]}...")
            else:
                console.print(f"❌ [{i}/{len(articles_data)}] 기사 파싱 실패")

        console.print(f"📊 수집 완료: {success_count}/{len(articles_data)}개 성공")

    async def _extract_content(self, url: str) -> str:
        """Playwright로 본문 전문 추출 - 최적화된 버전"""
        async with self._semaphore:  # 동시 처리 제한
            page = None
            try:
                # 브라우저 재사용 - 한 번만 초기화
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
                            '--memory-pressure-off',
                            '--disable-background-timer-throttling',
                            '--disable-backgrounding-occluded-windows',
                            '--disable-renderer-backgrounding'
                        ]
                    )

                page = await self._browser.new_page()
                await page.set_viewport_size({"width": 1280, "height": 720})
                await page.goto(url, wait_until="domcontentloaded", timeout=5000)  # 타임아웃 단축

                # 뉴스원 본문 추출
                content = ""
                
                try:
                    content = await page.evaluate('''() => {
                    const result = { content: '', success: false };
                    
                    // 우선순위별 선택자 (뉴스원 특화)
                    const contentSelectors = [
                        'div.article-body',
                        'div#article-body', 
                        'section.article-body',
                        'article.article-body',
                        '.story-news',
                        '.article-content',
                        'main',
                        'article'
                    ];
                    
                    let contentArea = null;
                    for (const selector of contentSelectors) {
                        contentArea = document.querySelector(selector);
                        if (contentArea) break;
                    }
                    
                    if (contentArea) {
                        // 광고 및 불필요한 요소 제거
                        const unwantedSelectors = [
                            '.ad', '.advertisement', '.banner',
                            '[class*="ad"]', '[id*="ad"]',
                            '.social-share', '.related-articles',
                            'script', 'style', 'noscript'
                        ];
                        
                        unwantedSelectors.forEach(selector => {
                            const elements = contentArea.querySelectorAll(selector);
                            elements.forEach(el => el.remove());
                        });
                        
                        // 본문 텍스트 추출
                        const paragraphs = contentArea.querySelectorAll('p, div.text, span.text');
                        const texts = [];
                        
                        paragraphs.forEach(p => {
                            const text = p.textContent?.trim() || '';
                            
                            // 필터링: 의미있는 텍스트만 추출
                            if (text && 
                                text.length > 30 && 
                                !text.includes('@') && 
                                !text.includes('기자') &&
                                !text.includes('특파원') &&
                                !text.includes('통신원') &&
                                !text.match(/^\\s*$/) &&
                                !text.match(/^[\\d\\s\\.:-]+$/)) {  // 시간/날짜 형식 제외
                                texts.push(text);
                            }
                        });
                        
                        if (texts.length > 0) {
                            result.content = texts.join('\\n\\n');
                            result.success = true;
                        }
                    }
                    
                    return result.content;
                }''')
                
                    if content and len(content.strip()) > 50:
                        return content.strip()
                        
                except Exception as e:
                    console.print(f"⚠️ JavaScript 본문 추출 실패: {str(e)[:50]}")
                
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
        """본문 전문 수집 - 병렬 처리로 최적화"""
        if not self.articles:
            return

        console.print(f"📖 본문 수집 시작: {len(self.articles)}개 기사")
        
        # 병렬 처리로 본문 수집
        tasks = [self._extract_content(art["url"]) for art in self.articles]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 처리
        success_count = 0
        for i, (article, result) in enumerate(zip(self.articles, results), 1):
            if isinstance(result, Exception):
                console.print(f"❌ [{i}/{len(self.articles)}] 본문 수집 실패: {result}")
                article["content"] = ""
            elif result and len(result.strip()) > 50:
                article["content"] = result.strip()
                success_count += 1
                console.print(f"✅ [{i}/{len(self.articles)}] 본문 수집 성공")
            else:
                console.print(f"⚠️ [{i}/{len(self.articles)}] 본문 수집 실패 (내용 부족)")

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

    async def run(self, total_limit: int = 150):
        """실행"""
        try:
            console.print(f"🚀 뉴스원 정치 기사 크롤링 시작 (최대 {total_limit}개)")
            
            await self._collect_articles(total_limit)
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
    collector = NewsonePoliticsCollector()
    await collector.run(total_limit=150)  # start=1부터 start=15까지 각각 10개씩 총 150개 수집

if __name__ == "__main__":
    asyncio.run(main())
