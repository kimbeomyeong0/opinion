#!/usr/bin/env python3
"""
세계일보 정치 기사 크롤러 (API 기반)
- boxTemplate/politics/box/newsList.do API를 사용하여 기사 목록 수집
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


class SegyePoliticsCollector:
    def __init__(self):
        self.base_url = "https://www.segye.com"
        self.api_base = "https://www.segye.com/boxTemplate/politics/box/newsList.do"
        self.media_name = "세계일보"
        self.media_bias = "right"
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
        """API 페이지 URL 목록 생성 (page=0, 1, 2...)"""
        urls = []
        for page in range(num_pages):
            url = f"{self.api_base}?dataPath=&dataId=0101010000000&listSize=15&naviSize=10&page={page}&dataType=slist"
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
                    list_items = soup.find_all('li')
                    
                    for li in list_items:
                        # a 태그 찾기
                        link = li.find('a', href=True)
                        if not link:
                            continue
                            
                        href = link.get('href')
                        if not href or 'newsView' not in href:
                            continue
                        
                        # 상대 URL을 절대 URL로 변환
                        if href.startswith('http://'):
                            full_url = href.replace('http://', 'https://')
                        elif href.startswith('/'):
                            full_url = urljoin(self.base_url, href)
                        else:
                            full_url = urljoin(self.base_url, href)
                        
                        # 제목 추출
                        title_element = link.find('strong', class_='tit')
                        if title_element:
                            title = title_element.get_text(strip=True)
                        else:
                            title = link.get_text(strip=True)
                        
                        # 날짜 추출
                        date_element = li.find('small', class_='date')
                        published_at = ""
                        if date_element:
                            date_text = date_element.get_text(strip=True)
                            published_at = self._parse_datetime(date_text)
                        
                        # 요약 추출 (있는 경우)
                        cont_element = link.find('span', class_='cont')
                        description = ""
                        if cont_element:
                            description = cont_element.get_text(strip=True)
                        
                        if title and len(title) > 10:
                            article = {
                                'title': title,
                                'url': full_url,
                                'content': '',
                                'published_at': published_at,
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
        tasks = [self._get_page_articles(url, i) for i, url in enumerate(page_urls)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 수집
        total_articles = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                console.print(f"❌ 페이지 {i} 처리 중 오류: {str(result)}")
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
            # JSON-LD 구조화된 데이터에서 추출
            json_ld = soup.find('script', type='application/ld+json')
            if json_ld:
                import json
                try:
                    data = json.loads(json_ld.string)
                    if 'datePublished' in data:
                        return self._parse_datetime(data['datePublished'])
                except:
                    pass
            
            # meta 태그에서 추출
            meta_date = soup.find('meta', property='article:published_time')
            if meta_date:
                return self._parse_datetime(meta_date.get('content', ''))
            
            # time 태그에서 추출
            time_tag = soup.find('time', datetime=True)
            if time_tag:
                return self._parse_datetime(time_tag.get('datetime', ''))
            
            # 클래스 기반 추출
            date_selectors = [
                '.date',
                '.time',
                '.publish_date',
                '.article_date',
                '.news_date'
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
        """세계일보 본문 텍스트 추출 (개선된 버전)"""
        try:
            # 1차: #article_txt > article.viewBox2 찾기
            content_container = soup.select_one('#article_txt > article.viewBox2')
            
            # 2차 폴백: #article_txt
            if not content_container:
                content_container = soup.select_one('#article_txt')
            
            # 3차 폴백: [itemprop="articleBody"]
            if not content_container:
                content_container = soup.select_one('[itemprop="articleBody"]')
            
            if not content_container:
                console.print("⚠️ 본문 컨테이너를 찾을 수 없습니다")
                return ""
            
            # 제외할 요소들 제거
            exclude_selectors = [
                'script', 'style', 'noscript', 'aside', 
                'ins.adsbygoogle', 'figure', 'figcaption', 
                'iframe', '.newsct_journalist', '.copyright', 
                '#outerDiv'
            ]
            
            for selector in exclude_selectors:
                elements = content_container.select(selector)
                for el in elements:
                    el.decompose()
            
            # <br> 태그를 줄바꿈으로 변환
            for br in content_container.find_all('br'):
                br.replace_with('\n')
            
            # &nbsp; 등 비가시 공백 제거
            import html
            text_content = str(content_container)
            text_content = html.unescape(text_content)
            text_content = re.sub(r'&nbsp;', ' ', text_content)
            
            # 다시 BeautifulSoup으로 파싱
            clean_container = BeautifulSoup(text_content, 'html.parser')
            
            # lead 추출 (em.precis)
            lead_element = clean_container.find('em', class_='precis')
            lead_text = lead_element.get_text(strip=True) if lead_element else ""
            
            # 본문 텍스트 추출
            paragraphs = []
            
            # <p> 요소의 텍스트 수집
            for p in clean_container.find_all('p'):
                text = p.get_text(strip=True)
                if text and len(text) > 10:  # 10자 이상인 문단만
                    paragraphs.append(text)
            
            # 컨테이너 직계/후손의 텍스트 노드 수집
            if not paragraphs:
                # 전체 텍스트에서 추출
                full_text = clean_container.get_text()
                # 공백 정리 및 문단 분리
                lines = [line.strip() for line in full_text.split('\n') if line.strip()]
                paragraphs = [line for line in lines if len(line) > 10]
            
            # 텍스트 결합
            if paragraphs:
                # 문단 사이에 빈 줄 하나 추가
                combined_text = '\n\n'.join(paragraphs)
                # 연속된 공백 정리
                combined_text = re.sub(r'\s+', ' ', combined_text)
                # 빈 줄 정리
                combined_text = re.sub(r'\n\s*\n', '\n\n', combined_text).strip()
                
                # lead가 있으면 앞에 추가
                if lead_text:
                    final_text = f"{lead_text}\n\n{combined_text}"
                else:
                    final_text = combined_text
                
                return final_text
            
            return ""
            
        except Exception as e:
            console.print(f"⚠️ 본문 추출 실패: {str(e)}")
            return ""

    def _parse_datetime(self, datetime_str: str) -> str:
        """날짜시간 문자열 파싱"""
        try:
            clean_time = datetime_str.strip()
            
            if re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', clean_time):
                # "YYYY-MM-DD HH:MM:SS" 형식 (KST 기준)
                kst_time = datetime.strptime(clean_time, "%Y-%m-%d %H:%M:%S")
                kst_tz = pytz.timezone("Asia/Seoul")
                kst_dt = kst_tz.localize(kst_time)
                return kst_dt.astimezone(pytz.UTC).isoformat()
            elif re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$', clean_time):
                # "YYYY-MM-DD HH:MM" 형식 (KST 기준)
                kst_time = datetime.strptime(clean_time, "%Y-%m-%d %H:%M")
                kst_tz = pytz.timezone("Asia/Seoul")
                kst_dt = kst_tz.localize(kst_time)
                return kst_dt.astimezone(pytz.UTC).isoformat()
            elif 'T' in clean_time:
                # ISO 형식
                if '+' in clean_time:
                    published_at = datetime.fromisoformat(clean_time)
                    return published_at.astimezone(pytz.UTC).isoformat()
                else:
                    published_at = datetime.fromisoformat(clean_time.replace('Z', '+00:00'))
                    return published_at.isoformat()
            else:
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
            console.print(f"🚀 세계일보 정치 기사 크롤링 시작 (최대 {num_pages}페이지)")
            
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
    collector = SegyePoliticsCollector()
    await collector.run(num_pages=10)  # 10페이지에서 각각 15개씩 총 150개 수집

if __name__ == "__main__":
    asyncio.run(main())
