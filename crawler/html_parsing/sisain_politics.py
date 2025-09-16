#!/usr/bin/env python3
"""
시사IN 정치 기사 크롤러
무한스크롤 방식의 API를 통한 기사 수집
"""

import asyncio
import httpx
import re
import html
import sys
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from rich.console import Console

# 상위 디렉토리 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.supabase_manager import SupabaseManager

console = Console()


class SisainPoliticsCollector:
    def __init__(self):
        self.base_url = "https://www.sisain.co.kr"
        self.politics_url = "https://www.sisain.co.kr/news/articleList.html?sc_section_code=S1N6&view_type=sm"
        self.api_url = "https://www.sisain.co.kr/news/articleList.html"
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        
        self.supabase = SupabaseManager()
        self.media_outlet = None
        
    def initialize(self):
        """미디어 아웃렛 정보 초기화"""
        try:
            # 시사IN 미디어 아웃렛 ID 직접 설정
            self.media_outlet = {
                "id": "193bfb31-bd5b-49a1-ad19-e068070e5794",
                "name": "sisain"
            }
            
            console.print("✅ 시사IN 미디어 아웃렛 로드 완료")
            return True
            
        except Exception as e:
            console.print(f"❌ 초기화 실패: {str(e)}")
            return False
    
    def _get_api_params(self, page: int) -> Dict[str, str]:
        """API 파라미터 생성"""
        return {
            "sc_section_code": "S1N6",
            "view_type": "sm",
            "total": "5208",
            "list_per_page": "20",
            "page_per_page": "10",
            "page": str(page),
            "box_idxno": "0"
        }
    
    async def _get_page_articles(self, client: httpx.AsyncClient, page: int) -> List[Dict[str, Any]]:
        """특정 페이지의 기사 목록 수집"""
        try:
            params = self._get_api_params(page)
            
            response = await client.get(
                self.api_url,
                params=params,
                headers=self.headers,
                timeout=30.0
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")
            articles = []
            
            # 디버깅: 응답 내용 확인
            console.print(f"🔍 페이지 {page} 응답 길이: {len(response.text)}")
            
            # 기사 목록 파싱
            article_items = soup.select("ul.type li.items")
            console.print(f"🔍 페이지 {page} 기사 아이템 수: {len(article_items)}")
            
            for item in article_items:
                try:
                    # 제목과 링크
                    title_link = item.select_one("div.view-cont h2.titles a")
                    if not title_link:
                        continue
                    
                    title = title_link.get_text(strip=True)
                    relative_url = title_link.get("href", "")
                    if not relative_url:
                        continue
                    
                    article_url = urljoin(self.base_url, relative_url)
                    
                    # 요약
                    lead = item.select_one("p.lead.line-x2 a")
                    description = lead.get_text(strip=True) if lead else ""
                    
                    # 날짜
                    date_elem = item.select_one("em.replace-date")
                    published_date = ""
                    if date_elem:
                        date_text = date_elem.get_text(strip=True)
                        # YYYY.MM.DD HH:MM 형식을 파싱
                        date_match = re.search(r'(\d{4})\.(\d{2})\.(\d{2})\s+(\d{2}):(\d{2})', date_text)
                        if date_match:
                            year, month, day, hour, minute = date_match.groups()
                            published_date = f"{year}-{month}-{day}"
                    
                    # 썸네일
                    thumb_img = item.select_one("a.thumb img")
                    image_url = ""
                    image_alt = ""
                    if thumb_img:
                        image_url = thumb_img.get("src", "")
                        image_alt = thumb_img.get("alt", "")
                        if image_url and not image_url.startswith("http"):
                            image_url = urljoin(self.base_url, image_url)
                    
                    # article_id 추출 (URL에서)
                    article_id = ""
                    url_match = re.search(r'/(\d+)/?$', article_url)
                    if url_match:
                        article_id = url_match.group(1)
                    
                    article = {
                        "source": "sisain",
                        "article_id": article_id,
                        "url": article_url,
                        "title": title,
                        "description": description,
                        "published_date": published_date,
                        "image_url": image_url if image_url else None,
                        "image_alt": image_alt,
                        "byline": None,
                        "content": "",
                        "published_at": None,
                        "lead_image": None
                    }
                    
                    articles.append(article)
                    
                except Exception as e:
                    console.print(f"⚠️ 기사 파싱 실패: {str(e)}")
                    continue
            
            return articles
            
        except Exception as e:
            console.print(f"❌ 페이지 {page} 수집 실패: {str(e)}")
            return []
    
    async def _collect_page_articles_parallel(self, client: httpx.AsyncClient, num_pages: int) -> List[Dict[str, Any]]:
        """병렬로 여러 페이지의 기사 수집"""
        console.print(f"📄 {num_pages}개 페이지에서 기사 수집 시작 (병렬 처리)...")
        
        tasks = []
        for page in range(1, num_pages + 1):
            task = self._get_page_articles(client, page)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_articles = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                console.print(f"❌ 페이지 {i+1} 실패: {str(result)}")
            else:
                all_articles.extend(result)
                console.print(f"📰 발견: {len(result)}개 기사...")
        
        # 중복 제거 (URL 기준)
        seen_urls = set()
        unique_articles = []
        for article in all_articles:
            if article["url"] not in seen_urls:
                seen_urls.add(article["url"])
                unique_articles.append(article)
        
        console.print(f"📊 총 {len(unique_articles)}개 기사 수집")
        return unique_articles
    
    def _extract_content_text(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """시사IN 본문 텍스트 추출"""
        try:
            # 본문 컨테이너 찾기
            content_container = soup.select_one('article#article-view-content-div.article-veiw-body[itemprop="articleBody"]')
            if not content_container:
                content_container = soup.select_one('article.article-veiw-body[itemprop="articleBody"]')
            
            if not content_container:
                console.print("⚠️ 본문 컨테이너를 찾을 수 없습니다")
                return {"paragraphs": [], "text": "", "headings": [], "images": [], "links": [], "appendix": None}
            
            # 광고/장식 요소 제거
            exclude_selectors = [
                'script', 'style', 'noscript', 'iframe', 
                '.ad-template', 'ins.adsbygoogle', '[id^="AD"]', 
                '.IMGFLOATING', '[style*="display:none"]'
            ]
            
            for selector in exclude_selectors:
                elements = content_container.select(selector)
                for el in elements:
                    el.decompose()
            
            # 래퍼 div 풀기
            wrapper_divs = content_container.select('div[style*="text-align:center"]')
            for wrapper in wrapper_divs:
                # 자식 요소들을 부모로 이동
                parent = wrapper.parent
                if parent:
                    for child in list(wrapper.children):
                        parent.insert(wrapper.index(child), child)
                    wrapper.decompose()
            
            # <br> 태그를 줄바꿈으로 변환
            for br in content_container.find_all('br'):
                br.replace_with('\n')
            
            # 공백 정리 함수
            def clean_text(text: str) -> str:
                text = text.replace('&nbsp;', ' ')
                text = re.sub(r'\s+', ' ', text)
                return text.strip()
            
            # 제목 수집
            headings = []
            for i in range(1, 7):
                for h in content_container.find_all(f'h{i}'):
                    heading_text = h.get_text(strip=True)
                    if heading_text:
                        headings.append(f"h{i} {heading_text}")
            
            # 이미지 수집
            images = []
            lead_image = None
            figures = content_container.find_all('figure.photo-layout')
            
            for i, figure in enumerate(figures):
                img = figure.find('img')
                if img:
                    src = img.get('src', '')
                    alt = img.get('alt', '')
                    if src and not src.startswith('http'):
                        src = urljoin(self.base_url, src)
                    
                    figcaption = figure.find('figcaption')
                    caption = figcaption.get_text(strip=True) if figcaption else ""
                    
                    idxno = figure.get('data-idxno')
                    
                    image_data = {
                        "src": src,
                        "alt": alt,
                        "caption": caption,
                        "idxno": idxno
                    }
                    
                    images.append(image_data)
                    
                    # 첫 번째 이미지를 lead_image로 설정
                    if i == 0:
                        lead_image = image_data
            
            # 링크 수집
            links = []
            seen_hrefs = set()
            
            # 단락 수집
            paragraphs = []
            
            for p in content_container.find_all('p'):
                # 앵커 태그 처리
                for a in p.find_all('a'):
                    href = a.get('href', '')
                    text = a.get_text(strip=True)
                    
                    if href and href not in seen_hrefs:
                        seen_hrefs.add(href)
                        if not href.startswith('http'):
                            href = urljoin(self.base_url, href)
                        links.append({"text": text, "href": href})
                    
                    # 링크 텍스트만 남기고 href는 제거
                    a.replace_with(text)
                
                text = p.get_text(strip=True)
                if text and len(text) > 10:  # 의미 있는 단락만
                    text = clean_text(text)
                    if text:
                        paragraphs.append(text)
            
            # appendix 처리 ("■ 이렇게 조사했다" 블록)
            appendix = None
            for p in content_container.find_all('p'):
                span = p.find('span', style=lambda x: x and 'color:#2980b9' in x)
                if span and "■ 이렇게 조사했다" in span.get_text():
                    # <br> 기준으로 줄바꿈 유지
                    methodology_text = span.get_text(separator='\n')
                    appendix = {"methodology": methodology_text}
                    break
            
            # 텍스트 결합
            combined_text = '\n\n'.join(paragraphs)
            
            return {
                "paragraphs": paragraphs,
                "text": combined_text,
                "headings": headings if headings else None,
                "images": images if images else None,
                "lead_image": lead_image,
                "links": links if links else None,
                "appendix": appendix
            }
            
        except Exception as e:
            console.print(f"⚠️ 본문 추출 실패: {str(e)}")
            return {"paragraphs": [], "text": "", "headings": None, "images": None, "lead_image": None, "links": None, "appendix": None}
    
    def _should_skip_article(self, article: Dict[str, Any]) -> bool:
        """기사 필터링 조건 확인"""
        title = article.get("title", "")
        content = article.get("content", "")
        
        # 조건 1: 본문이 "■ 방송"으로 시작하는 경우
        if content.startswith("■ 방송"):
            return True
            
        # 조건 2: 본문이 "〈시사IN〉은"으로 시작하는 경우  
        if content.startswith("〈시사IN〉은"):
            return True
            
        # 조건 3: 타이틀이 "[김은지의 뉴스IN]"으로 끝나는 경우
        if title.endswith("[김은지의 뉴스IN]"):
            return True
            
        return False
    
    async def _extract_content_httpx(self, client: httpx.AsyncClient, article: Dict[str, Any], index: int):
        """httpx로 기사 본문 추출"""
        try:
            console.print(f"📖 [{index}] 시작: {article['title'][:40]}...")
            
            response = await client.get(article["url"], headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 본문 추출
            content_data = self._extract_content_text(soup)
            article["content"] = content_data.get("text", "")
            
            # 바이라인은 별도로 추출하지 않음 (본문에 포함)
            article["byline"] = None
            
            # lead_image 설정
            lead_image = content_data.get("lead_image")
            if lead_image:
                article["lead_image"] = lead_image
            
            # 필터링 조건 확인
            if self._should_skip_article(article):
                console.print(f"⏭️ [{index}] 스킵: 필터링 조건에 해당")
                article["content"] = ""  # 스킵된 기사로 표시
                return
            
            console.print(f"✅ [{index}] 완료: {len(article['content'])}자")
            
        except Exception as e:
            console.print(f"❌ [{index}] 실패: {str(e)[:50]}...")
            article["content"] = ""
            article["byline"] = None
            article["lead_image"] = None
    
    async def _extract_all_contents(self, articles: List[Dict[str, Any]]) -> None:
        """모든 기사의 본문 추출"""
        console.print(f"📖 {len(articles)}개 기사 본문 수집 시작 (병렬 처리)...")
        
        batch_size = 20
        total_batches = (len(articles) + batch_size - 1) // batch_size
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(articles))
                batch_articles = articles[start_idx:end_idx]
                
                console.print(f"📖 배치 {batch_idx + 1}/{total_batches}: {len(batch_articles)}개 기사 처리 중...")
                
                tasks = []
                for i, article in enumerate(batch_articles):
                    task = self._extract_content_httpx(client, article, start_idx + i + 1)
                    tasks.append(task)
                
                await asyncio.gather(*tasks, return_exceptions=True)
                
                # 배치 간 잠시 대기
                if batch_idx < total_batches - 1:
                    await asyncio.sleep(1)
    
    async def run(self, num_pages: int = 8):
        """크롤링 실행"""
        try:
            if not self.initialize():
                return
            
            console.print(f"🚀 시사IN 정치 기사 크롤링 시작 (최대 {num_pages}페이지)")
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                # 기사 목록 수집
                articles = await self._collect_page_articles_parallel(client, num_pages)
                
                if not articles:
                    console.print("❌ 수집된 기사가 없습니다")
                    return
                
                # 본문 추출
                await self._extract_all_contents(articles)
                
                # Supabase에 저장
                console.print("💾 Supabase에 기사 저장 중...")
                
                saved_count = 0
                skipped_count = 0
                
                for article in articles:
                    try:
                        # 필터링된 기사는 건너뛰기
                        if not article.get("content") or article["content"] == "":
                            skipped_count += 1
                            continue
                        
                        # published_at 설정 (KST 기준)
                        if article["published_date"]:
                            try:
                                date_obj = datetime.strptime(article["published_date"], "%Y-%m-%d")
                                kst_time = date_obj.replace(tzinfo=timezone(timedelta(hours=9)))
                                article["published_at"] = kst_time.isoformat()
                            except ValueError:
                                article["published_at"] = None
                        else:
                            article["published_at"] = None
                        
                        # Supabase에 저장
                        article_data = {
                            "media_id": self.media_outlet["id"],
                            "url": article["url"],
                            "title": article["title"],
                            "content": article["content"],
                            "published_at": article["published_at"],
                            "created_at": datetime.now(timezone.utc).isoformat()
                        }
                        
                        result = self.supabase.insert_article(article_data)
                        
                        if result:
                            saved_count += 1
                        else:
                            skipped_count += 1
                            
                    except Exception as e:
                        console.print(f"❌ 기사 저장 실패: {str(e)[:50]}...")
                        skipped_count += 1
                
                console.print(f"📊 저장 결과: 성공 {saved_count}, 스킵 {skipped_count}")
                console.print("🎉 크롤링 완료!")
                
        except KeyboardInterrupt:
            console.print("⏹️ 사용자에 의해 중단되었습니다")
        except Exception as e:
            console.print(f"❌ 크롤링 중 오류 발생: {str(e)}")


async def main():
    collector = SisainPoliticsCollector()
    await collector.run(num_pages=15)  # 15페이지에서 각각 20개씩 총 300개 수집 (필터링 고려)

if __name__ == "__main__":
    asyncio.run(main())
