#!/usr/bin/env python3
"""
중앙일보 정치 기사 크롤러
"""

import asyncio
import httpx
from datetime import datetime
from urllib.parse import urljoin
from rich.console import Console
from playwright.async_api import async_playwright
import pytz
from bs4 import BeautifulSoup

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.supabase_manager import SupabaseManager

console = Console()

class JoongangPoliticsCollector:
    """중앙일보 정치 기사 수집기"""
    
    def __init__(self):
        self.media_name = "중앙일보"
        self.base_url = "https://www.joongang.co.kr"
        self.articles = []
        self.supabase_manager = SupabaseManager()
        
    async def _get_page_articles(self, page_num: int) -> list:
        """특정 페이지에서 기사 목록 수집"""
        try:
            url = f"{self.base_url}/politics?page={page_num}"
            console.print(f"📡 페이지 수집: {url}")
            
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 무시해야 하는 영역 제거
                showcase = soup.find('section', class_='showcase_general')
                if showcase:
                    showcase.decompose()
                    console.print("🗑️ showcase_general 영역 제거")
                
                rank_list = soup.find('ul', class_='card_right_list rank_list')
                if rank_list:
                    rank_list.decompose()
                    console.print("🗑️ rank_list 영역 제거")
                
                # 수집 대상: <ul id="story_list"> 안의 <li class="card">
                story_list = soup.find('ul', id='story_list')
                if not story_list:
                    console.print("❌ story_list를 찾을 수 없습니다")
                    return []
                
                cards = story_list.find_all('li', class_='card')
                console.print(f"🔍 story_list에서 {len(cards)}개 카드 발견")
                
                articles = []
                max_articles_per_page = 24  # 각 페이지에서 24개 수집
                collected_count = 0
                
                for i, card in enumerate(cards):
                    if collected_count >= max_articles_per_page:
                        break
                        
                    try:
                        # 제목과 URL 추출
                        headline = card.find('h2', class_='headline')
                        if not headline:
                            continue
                            
                        link = headline.find('a')
                        if not link:
                            continue
                        
                        title = link.get_text(strip=True)
                        article_url = link.get('href', '')
                        
                        if title and article_url:
                            # 상대 URL을 절대 URL로 변환
                            if article_url.startswith('/'):
                                full_url = urljoin(self.base_url, article_url)
                            else:
                                full_url = article_url
                            
                            articles.append({
                                'title': title,
                                'url': full_url
                            })
                            collected_count += 1
                            console.print(f"📰 기사 발견 [{collected_count}]: {title[:50]}...")
                    
                    except Exception as e:
                        console.print(f"⚠️ 카드 [{i}] 처리 중 오류: {e}")
                        continue
                
                console.print(f"📊 페이지에서 {len(articles)}개 기사 발견")
                return articles
                
        except Exception as e:
            console.print(f"❌ 페이지 수집 실패: {e}")
            return []
    
    async def _extract_content(self, url: str) -> dict:
        """기사 본문 및 발행시간 추출"""
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                await page.goto(url, wait_until='domcontentloaded', timeout=60000)
                
                # 기사 본문 및 발행시간 추출
                content_data = await page.evaluate("""
                    () => {
                        // 발행시간 추출 - <time itemprop="datePublished">의 datetime 속성 사용
                        let published_at = '';
                        
                        // 1. time[itemprop="datePublished"] datetime 속성 시도
                        const timeElement = document.querySelector('time[itemprop="datePublished"]');
                        if (timeElement) {
                            published_at = timeElement.getAttribute('datetime');
                        }
                        
                        // 2. 다른 가능한 시간 선택자들 시도
                        if (!published_at) {
                            const timeSelectors = [
                                'time[datetime]',
                                'button.btn_datetime span',
                                '.article_info .date',
                                '.article_info .time',
                                '.date_info',
                                '.article_date',
                                '.publish_date'
                            ];
                            
                            for (const selector of timeSelectors) {
                                const element = document.querySelector(selector);
                                if (element) {
                                    // datetime 속성이 있으면 우선 사용
                                    const datetime = element.getAttribute('datetime');
                                    if (datetime) {
                                        published_at = datetime;
                                        break;
                                    }
                                    
                                    // 없으면 텍스트에서 날짜 형식 찾기
                                    const text = element.textContent || element.innerText || '';
                                    const trimmed = text.trim();
                                    if (trimmed.match(/\\d{4}-\\d{2}-\\d{2}/) || 
                                        trimmed.match(/\\d{4}\\.\\d{2}\\.\\d{2}/) ||
                                        trimmed.match(/\\d{2}:\\d{2}/)) {
                                        published_at = trimmed;
                                        break;
                                    }
                                }
                            }
                        }
                        
                        // 본문 영역 찾기
                        const articleBody = document.getElementById('article_body');
                        if (!articleBody) return { content: '', published_at: published_at };
                        
                        // 광고 영역 제거
                        const adElements = articleBody.querySelectorAll('#ad_art_content_mid, .ad, .advertisement');
                        adElements.forEach(el => el.remove());
                        
                        // <p> 태그들의 텍스트만 추출
                        const paragraphs = articleBody.querySelectorAll('p');
                        const contentLines = [];
                        
                        paragraphs.forEach(p => {
                            const text = p.textContent || p.innerText || '';
                            const trimmedText = text.trim();
                            
                            // 기자명/출처 부분 제거
                            if (trimmedText && 
                                !trimmedText.includes('기자') && 
                                !trimmedText.includes('@') &&
                                !trimmedText.includes('[출처:') &&
                                !trimmedText.includes('출처:') &&
                                !trimmedText.includes('정재홍') &&
                                !trimmedText.includes('hongj@joongang.co.kr') &&
                                trimmedText.length > 10) {
                                contentLines.push(trimmedText);
                            }
                        });
                        
                        // 각 문단을 개행으로 구분하여 결합
                        const content = contentLines.join('\\n\\n');
                        
                        return {
                            content: content,
                            published_at: published_at
                        };
                    }
                """)
                
                await browser.close()
                return content_data
                
        except Exception as e:
            console.print(f"❌ 본문 추출 실패 ({url}): {e}")
            return {"content": "", "published_at": ""}
    
    async def _parse_article_data(self, article: dict, content_data: dict) -> dict:
        """기사 데이터 파싱 및 정리"""
        try:
            # 발행 시간 처리 (기사 실제 발행시간)
            published_at_str = content_data.get('published_at', '')
            
            if published_at_str and published_at_str.strip():
                try:
                    clean_time = published_at_str.strip()
                    
                    # "업데이트 정보 더보기" 같은 텍스트 제거
                    if '업데이트' in clean_time or '더보기' in clean_time:
                        clean_time = ''
                    
                    if clean_time:
                        if 'T' in clean_time and '+' in clean_time:
                            # ISO 형식 with timezone (예: "2025-09-05T01:17:00+09:00")
                            published_at = datetime.fromisoformat(clean_time)
                            # UTC로 변환
                            published_at = published_at.astimezone(pytz.UTC)
                        elif 'T' in clean_time:
                            # ISO 형식 without timezone (UTC로 가정)
                            published_at = datetime.fromisoformat(clean_time.replace('Z', '+00:00'))
                        elif '-' in clean_time and ':' in clean_time:
                            # "YYYY-MM-DD HH:MM" 형식인 경우 (KST 기준)
                            published_at = datetime.strptime(clean_time, "%Y-%m-%d %H:%M")
                            kst = pytz.timezone("Asia/Seoul")
                            published_at = kst.localize(published_at).astimezone(pytz.UTC)
                        elif '.' in clean_time and ':' in clean_time:
                            # "YYYY.MM.DD HH:MM" 형식인 경우 (KST 기준)
                            published_at = datetime.strptime(clean_time, "%Y.%m.%d %H:%M")
                            kst = pytz.timezone("Asia/Seoul")
                            published_at = kst.localize(published_at).astimezone(pytz.UTC)
                        else:
                            # 다른 형식 시도
                            console.print(f"⚠️ 알 수 없는 시간 형식: {clean_time}")
                            published_at = datetime.now(pytz.UTC)
                    else:
                        published_at = datetime.now(pytz.UTC)
                        
                except Exception as e:
                    console.print(f"⚠️ 발행시간 파싱 실패: {published_at_str} - {e}")
                    published_at = datetime.now(pytz.UTC)
            else:
                published_at = datetime.now(pytz.UTC)
            
            # 생성 시간 (크롤링 시점의 현재 시각)
            created_at = datetime.now(pytz.UTC)
            
            return {
                'title': article['title'],
                'url': article['url'],
                'content': content_data.get('content', ''),
                'published_at': published_at.isoformat(),
                'created_at': created_at.isoformat(),
                'media_outlet': self.media_name
            }
            
        except Exception as e:
            console.print(f"❌ 기사 데이터 파싱 실패: {e}")
            return None
    
    async def collect_articles(self, num_pages: int = 5):
        """기사 수집"""
        console.print(f"📄 {num_pages}개 페이지에서 기사 수집 시작...")
        
        for page in range(1, num_pages + 1):
            console.print(f"📄 페이지 {page}/{num_pages} 처리 중...")
            articles = await self._get_page_articles(page)
            self.articles.extend(articles)
        
        console.print(f"📊 수집 완료: {len(self.articles)}개 성공")
    
    async def collect_contents(self):
        """기사 본문 수집"""
        console.print(f"📖 본문 수집 시작: {len(self.articles)}개 기사")
        
        for i, article in enumerate(self.articles, 1):
            console.print(f"📖 [{i}/{len(self.articles)}] 본문 수집 중: {article['title'][:50]}...")
            
            content_data = await self._extract_content(article['url'])
            
            # 기사 데이터에 본문과 발행시간 추가
            article['content'] = content_data.get('content', '')
            article['published_at'] = content_data.get('published_at', '')
            
            console.print(f"✅ [{i}/{len(self.articles)}] 본문 수집 성공")
    
    async def save_articles(self):
        """기사 저장"""
        console.print(f"💾 Supabase에 {len(self.articles)}개 기사 저장 중...")
        
        # 언론사 확인
        media = self.supabase_manager.get_media_outlet(self.media_name)
        if not media:
            media_id = self.supabase_manager.create_media_outlet(
                name=self.media_name,
                bias="center-right",
                website=self.base_url
            )
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
        
        success_count = 0
        skip_count = 0
        
        for i, article in enumerate(self.articles, 1):
            try:
                # 중복 체크
                if article["url"] in existing_urls:
                    skip_count += 1
                    console.print(f"⚠️ [{i}/{len(self.articles)}] 중복 기사 스킵: {article['title'][:50]}...")
                    continue
                
                # 기사 데이터 파싱
                parsed_article = await self._parse_article_data(article, article)
                
                if not parsed_article:
                    continue
                
                # media_id 추가
                if media_id:
                    parsed_article['media_id'] = media_id
                
                # media_outlet 필드 제거 (스키마에 없음)
                if 'media_outlet' in parsed_article:
                    del parsed_article['media_outlet']
                
                # Supabase에 저장
                result = self.supabase_manager.insert_article(parsed_article)
                
                if result:
                    success_count += 1
                    console.print(f"✅ [{i}/{len(self.articles)}] 저장 성공: {parsed_article['title'][:50]}...")
                else:
                    console.print(f"❌ [{i}/{len(self.articles)}] 저장 실패")
                    
            except Exception as e:
                console.print(f"❌ [{i}/{len(self.articles)}] 저장 중 오류: {e}")
        
        console.print(f"\n📊 저장 결과:")
        console.print(f"  ✅ 성공: {success_count}개")
        console.print(f"  ❌ 실패: {len(self.articles) - success_count - skip_count}개")
        console.print(f"  ⚠️ 중복 스킵: {skip_count}개")
        console.print(f"  📈 성공률: {success_count/len(self.articles)*100:.1f}%")
    
    async def run(self, num_pages: int = 5):
        """크롤러 실행"""
        try:
            console.print("🚀 중앙일보 정치 기사 크롤링 시작")
            
            # 1. 기사 목록 수집
            await self.collect_articles(num_pages)
            
            if not self.articles:
                console.print("❌ 수집된 기사가 없습니다")
                return
            
            # 2. 기사 본문 수집
            await self.collect_contents()
            
            # 3. 기사 저장
            await self.save_articles()
            
            console.print("🎉 크롤링 완료!")
            
        except KeyboardInterrupt:
            console.print("⏹️ 사용자에 의해 중단되었습니다")
        except Exception as e:
            console.print(f"❌ 크롤링 중 오류 발생: {str(e)}")

async def main():
    collector = JoongangPoliticsCollector()
    await collector.run(num_pages=5)  # 5페이지에서 각각 24개씩 총 120개 기사 수집

if __name__ == "__main__":
    asyncio.run(main())
