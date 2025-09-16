#!/usr/bin/env python3
"""
내일신문 정치 기사 크롤러 (HTML 파싱 기반)
- 정치 섹션 기사 수집
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


class NaeilPoliticsCollector:
    def __init__(self):
        self.base_url = "https://www.naeil.com"
        self.media_name = "내일신문"
        self.media_bias = "left"  # 진보 성향
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

    def _get_page_urls(self, num_pages: int = 8) -> List[str]:
        """페이지 URL 목록 생성 (page=1, 2, 3...)"""
        urls = []
        for page in range(1, num_pages + 1):
            url = f"{self.base_url}/politics?page={page}"
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
                    
                    # 기사 목록 추출 (.sub-news-list-wrap ul.story-list li.card.card-box)
                    story_list = soup.select('.sub-news-list-wrap ul.story-list li.card.card-box')
                    
                    for card in story_list:
                        try:
                            # 제목과 URL 추출 (.card-text .headline a)
                            headline_link = card.select_one('.card-text .headline a')
                            if not headline_link:
                                continue
                                
                            href = headline_link.get('href')
                            if not href:
                                continue
                            
                            # 상대 URL을 절대 URL로 변환
                            if href.startswith('http'):
                                full_url = href
                            else:
                                full_url = urljoin(self.base_url, href)
                            
                            title = headline_link.get_text(strip=True)
                            
                            # 요약 추출 (.card-text .description a)
                            description = ""
                            desc_element = card.select_one('.card-text .description a')
                            if desc_element:
                                description = desc_element.get_text(strip=True)
                            
                            # 날짜 추출 (.card-body .meta .year와 .card-body .meta .date)
                            published_at = self._extract_date(card)
                            
                            # 이미지 정보 추출 (.card-image img)
                            image_url, image_alt = self._extract_image_info(card)
                            
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
                            console.print(f"⚠️ 기사 카드 처리 중 오류: {str(e)}")
                            continue
                    
                    console.print(f"📄 페이지 {page_num}: {len(articles)}개 기사 수집")
                    return articles
                    
            except Exception as e:
                console.print(f"❌ 페이지 {page_num} 처리 중 오류: {str(e)}")
                return []

    def _extract_date(self, card) -> str:
        """기사 리스트에서 날짜 추출 (임시 - 본문에서 정확한 시간 추출)"""
        try:
            year_element = card.select_one('.card-body .meta .year')
            date_element = card.select_one('.card-body .meta .date')
            
            if year_element and date_element:
                year = year_element.get_text(strip=True)
                date_str = date_element.get_text(strip=True)
                
                # YYYY-MM-DD 형식으로 변환 (예: 2025, 09.15 -> 2025-09-15)
                if year and date_str and '.' in date_str:
                    month, day = date_str.split('.')
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            
            return datetime.now(pytz.UTC).isoformat()
                
        except Exception as e:
            console.print(f"⚠️ 날짜 추출 실패: {str(e)}")
            return datetime.now(pytz.UTC).isoformat()

    def _extract_image_info(self, card) -> tuple:
        """이미지 정보 추출 (.card-image img)"""
        try:
            img_element = card.select_one('.card-image img')
            if not img_element:
                return None, None
            
            # data-src가 있으면 우선, 없으면 src 사용
            image_url = img_element.get('data-src') or img_element.get('src')
            image_alt = img_element.get('alt', '')
            
            # https://static.naeil.com/img/1X1.png은 null 처리
            if image_url and '1X1.png' in image_url:
                return None, None
            
            # 상대 URL을 절대 URL로 변환
            if image_url and not image_url.startswith('http'):
                image_url = urljoin(self.base_url, image_url)
            
            return image_url, image_alt
                
        except Exception as e:
            console.print(f"⚠️ 이미지 정보 추출 실패: {str(e)}")
            return None, None

    async def collect_articles_parallel(self, num_pages: int = 8):
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
        """httpx로 기사 본문 추출"""
        try:
            console.print(f"📖 [{index}] 시작: {article['title'][:40]}...")
            
            response = await client.get(article["url"], headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 발행·수정 시각 추출
            date_data = self._extract_published_dates(soup)
            if date_data.get("published_at_utc"):
                article["published_at"] = date_data["published_at_utc"]
            
            # 본문 추출
            content_data = self._extract_content_text(soup)
            article["content"] = content_data.get("text", "")
            article["byline"] = content_data.get("byline", "")
            
            console.print(f"✅ [{index}] 완료: {len(article['content'])}자")
            
        except Exception as e:
            console.print(f"❌ [{index}] 실패: {str(e)[:50]}...")
            article["content"] = ""
            article["byline"] = ""

    def _extract_content_text(self, soup: BeautifulSoup) -> Dict[str, str]:
        """내일신문 본문 텍스트 추출 (개선된 버전)"""
        try:
            # 1차: .article-view.font-size03 찾기
            content_container = soup.select_one('.article-view.font-size03')
            
            # 2차 폴백: .article-view
            if not content_container:
                content_container = soup.select_one('.article-view')
            
            if not content_container:
                console.print("⚠️ 본문 컨테이너를 찾을 수 없습니다")
                return {"text": "", "byline": ""}
            
            # 제외할 요소들 제거
            exclude_selectors = [
                'script', 'style', 'noscript', 'figure', 'figcaption', 
                'iframe', 'aside', '[class^=ad-]', '[data-svcad]'
            ]
            
            for selector in exclude_selectors:
                elements = content_container.select(selector)
                for el in elements:
                    el.decompose()
            
            # <br> 태그를 줄바꿈으로 변환
            for br in content_container.find_all('br'):
                br.replace_with('\n')
            
            # subtitles 수집 (.article-subtitle.type01 > p)
            subtitles = []
            subtitle_elements = content_container.select('.article-subtitle.type01 > p')
            for element in subtitle_elements:
                text = element.get_text(strip=True)
                if text:
                    subtitles.append(text)
            
            # .article-subtitle 제거
            subtitle_containers = content_container.select('.article-subtitle')
            for container in subtitle_containers:
                container.decompose()
            
            # paragraphs 수집 (컨테이너 직계 <p>)
            paragraphs = []
            direct_p_elements = content_container.find_all('p', recursive=False)
            for element in direct_p_elements:
                text = element.get_text(strip=True)
                if text and len(text) > 10:  # 10자 이상인 문단만
                    paragraphs.append(text)
            
            # &nbsp;와 중복 공백/개행 정리
            for i, paragraph in enumerate(paragraphs):
                paragraphs[i] = re.sub(r'&nbsp;', ' ', paragraph)
                paragraphs[i] = re.sub(r'\s+', ' ', paragraphs[i]).strip()
            
            # 마지막 단락이 … 기자 또는 이메일(@)을 포함하면 byline으로 이동
            byline = ""
            if paragraphs:
                last_paragraph = paragraphs[-1]
                if ('…' in last_paragraph and '기자' in last_paragraph) or '@' in last_paragraph:
                    byline = last_paragraph
                    paragraphs.pop()
            
            # 텍스트 결합
            if subtitles and paragraphs:
                combined_text = '\n\n'.join(subtitles + [''] + paragraphs)
            elif subtitles:
                combined_text = '\n\n'.join(subtitles)
            elif paragraphs:
                combined_text = '\n\n'.join(paragraphs)
            else:
                combined_text = ""
            
            # 공백 정규화
            combined_text = re.sub(r'\n\s*\n', '\n\n', combined_text).strip()
            
            return {
                "text": combined_text,
                "byline": byline
            }
            
        except Exception as e:
            console.print(f"⚠️ 본문 추출 실패: {str(e)}")
            return {"text": "", "byline": ""}

    def _extract_published_dates(self, soup: BeautifulSoup) -> Dict[str, str]:
        """발행·수정 시각 추출"""
        try:
            result = {
                "published_at_kst": "",
                "published_at_utc": "",
                "updated_at_kst": "",
                "updated_at_utc": "",
                "raw_dates": []
            }
            
            # 1차: header.article-header .group .datetime .date에서 찾기
            datetime_elements = soup.select('header.article-header .group .datetime .date')
            
            for element in datetime_elements:
                text = element.get_text(strip=True)
                if not text:
                    continue
                
                result["raw_dates"].append(text)
                
                # YYYY-MM-DD HH:MM(:SS)? 패턴 추출
                date_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}(?::\d{2})?)', text)
                if not date_match:
                    continue
                
                date_str = date_match.group(1)
                
                # 초 단위가 없으면 :00 보완
                if len(date_str.split(':')) == 2:
                    date_str += ':00'
                
                # 꼬리표 제거하고 구분
                if any(keyword in text for keyword in ['게재', '입력', '등록']):
                    if not result["published_at_kst"]:
                        result["published_at_kst"] = f"{date_str}+09:00"
                        result["published_at_utc"] = self._convert_kst_to_utc(date_str)
                elif any(keyword in text for keyword in ['수정', '최종수정']):
                    if not result["updated_at_kst"]:
                        result["updated_at_kst"] = f"{date_str}+09:00"
                        result["updated_at_utc"] = self._convert_kst_to_utc(date_str)
            
            # 2차 폴백: 메타 태그들
            if not result["published_at_kst"]:
                fallback_dates = self._extract_meta_dates(soup)
                if fallback_dates.get("published_at_utc"):
                    result["published_at_kst"] = fallback_dates["published_at_kst"]
                    result["published_at_utc"] = fallback_dates["published_at_utc"]
            
            return result
            
        except Exception as e:
            console.print(f"⚠️ 발행·수정 시각 추출 실패: {str(e)}")
            return {
                "published_at_kst": "",
                "published_at_utc": "",
                "updated_at_kst": "",
                "updated_at_utc": "",
                "raw_dates": []
            }

    def _extract_meta_dates(self, soup: BeautifulSoup) -> Dict[str, str]:
        """메타 태그에서 발행 시각 추출 (폴백)"""
        try:
            # meta[property="article:published_time"]
            meta_published = soup.find('meta', property='article:published_time')
            if meta_published:
                content = meta_published.get('content', '')
                if content:
                    return self._parse_iso_date(content)
            
            # meta[name="pubdate"|"date"|"ptime"]
            for name in ['pubdate', 'date', 'ptime']:
                meta_element = soup.find('meta', attrs={'name': name})
                if meta_element:
                    content = meta_element.get('content', '')
                    if content:
                        return self._parse_iso_date(content)
            
            # time[datetime]
            time_element = soup.find('time', attrs={'datetime': True})
            if time_element:
                datetime_attr = time_element.get('datetime', '')
                if datetime_attr:
                    return self._parse_iso_date(datetime_attr)
            
            # JSON-LD
            json_scripts = soup.find_all('script', type='application/ld+json')
            for script in json_scripts:
                try:
                    import json
                    data = json.loads(script.string)
                    if isinstance(data, dict):
                        if 'datePublished' in data:
                            return self._parse_iso_date(data['datePublished'])
                        elif 'dateModified' in data:
                            return self._parse_iso_date(data['dateModified'])
                except:
                    continue
            
            return {"published_at_kst": "", "published_at_utc": ""}
            
        except Exception as e:
            console.print(f"⚠️ 메타 태그 날짜 추출 실패: {str(e)}")
            return {"published_at_kst": "", "published_at_utc": ""}

    def _parse_iso_date(self, date_str: str) -> Dict[str, str]:
        """ISO 날짜 문자열 파싱"""
        try:
            # ISO 형식 파싱
            if '+' in date_str or date_str.endswith('Z'):
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                utc_dt = dt.astimezone(pytz.UTC)
                kst_dt = dt.astimezone(KST)
                
                return {
                    "published_at_kst": kst_dt.isoformat(),
                    "published_at_utc": utc_dt.isoformat()
                }
            else:
                # 시간대 정보가 없으면 KST로 가정
                dt = datetime.fromisoformat(date_str)
                kst_dt = KST.localize(dt)
                utc_dt = kst_dt.astimezone(pytz.UTC)
                
                return {
                    "published_at_kst": kst_dt.isoformat(),
                    "published_at_utc": utc_dt.isoformat()
                }
                
        except Exception as e:
            console.print(f"⚠️ ISO 날짜 파싱 실패: {date_str} - {str(e)}")
            return {"published_at_kst": "", "published_at_utc": ""}

    def _convert_kst_to_utc(self, date_str: str) -> str:
        """KST 시간을 UTC로 변환"""
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            kst_dt = KST.localize(dt)
            utc_dt = kst_dt.astimezone(pytz.UTC)
            return utc_dt.isoformat()
        except Exception as e:
            console.print(f"⚠️ KST to UTC 변환 실패: {date_str} - {str(e)}")
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

    async def run(self, num_pages: int = 8):
        """실행"""
        try:
            console.print(f"🚀 내일신문 정치 기사 크롤링 시작 (최대 {num_pages}페이지)")
            
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
    collector = NaeilPoliticsCollector()
    await collector.run(num_pages=8)  # 8페이지에서 각각 20개씩 총 160개 수집

if __name__ == "__main__":
    asyncio.run(main())
