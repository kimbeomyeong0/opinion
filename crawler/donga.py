#!/usr/bin/env python3
"""
동아일보 정치 기사 크롤러 (페이지네이션 방식)
조선일보/뉴스1과 동일한 규칙 적용: 100개, 빠르게, 새로운 articles 테이블, 시간 처리
"""

import asyncio
import json
import sys
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from urllib.parse import urljoin
import httpx
import pytz
from bs4 import BeautifulSoup
from rich.console import Console
from rich.table import Table
from playwright.async_api import async_playwright
from dotenv import load_dotenv

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.supabase_manager import SupabaseManager

load_dotenv()

console = Console()
KST = pytz.timezone("Asia/Seoul")

class DongaPoliticsCollector:
    """동아일보 정치 기사 수집기 (페이지네이션 방식)"""
    
    def __init__(self):
        self.base_url = "https://www.donga.com"
        self.politics_url = "https://www.donga.com/news/Politics"
        self.media_name = "동아일보"
        self.media_bias = "right"  # 동아일보는 우편향
        self.supabase_manager = SupabaseManager()
        self.articles: List[Dict] = []
        self._playwright = None
        self._browser = None

    async def _collect_latest_articles(self):
        """페이지네이션 기반 최신 100개 기사 수집 (중복 제외)"""
        console.print("🔌 페이지네이션을 통한 최신 기사 100개 수집 시작...")
        target_count = 100
        page = 1
        page_offset = 0
        max_attempts = 20  # 최대 20페이지 시도

        async with httpx.AsyncClient(timeout=10.0) as client:
            for attempt in range(max_attempts):
                if len(self.articles) >= target_count:
                    break
                
                # 페이지 URL 구성
                if page == 1:
                    url = self.politics_url
                else:
                    # 동아일보 페이지네이션: p=11, p=21, p=31...
                    page_offset = (page - 1) * 10
                    url = f"{self.politics_url}?p={page_offset + 1}&prod=news&ymd=&m="
                
                console.print(f"📡 페이지 {page}/20 처리 중 (offset: {page_offset})")
                
                try:
                    response = await client.get(url)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # 기사 카드 찾기
                    articles = soup.select('.news_card')
                    console.print(f"📊 페이지 {page} 응답: {len(articles)}개 요소 수신")
                    
                    parsed_count = 0
                    added_count = 0
                    
                    for article in articles:
                        if len(self.articles) >= target_count:
                            break
                            
                        parsed_article = self._parse_html_article(article)
                        if parsed_article:
                            parsed_count += 1
                            if self._add_article(parsed_article):
                                added_count += 1
                    
                    console.print(f"📈 페이지 {page} 파싱 성공: {parsed_count}개, 최종 추가: {added_count}개")
                    console.print(f"📊 현재 수집된 기사: {len(self.articles)}개")
                    
                    if len(articles) == 0:
                        console.print(f"⚠️ 페이지 {page}에서 기사를 찾을 수 없습니다. 중단합니다.")
                        break
                    
                    page += 1
                    await asyncio.sleep(0.5)  # 페이지 간 딜레이
                    
                except Exception as e:
                    console.print(f"❌ 페이지 {page} 처리 중 오류: {str(e)}")
                    break
        
        console.print(f"🎯 수집 완료: {len(self.articles)}개 기사 (목표: {target_count}개)")

    def _parse_html_article(self, article_element) -> Optional[Dict]:
        """HTML 기사 요소 파싱"""
        try:
            # 제목 링크 찾기
            title_link = article_element.select_one('.tit a')
            if not title_link or not title_link.get('href'):
                return None
            
            title = title_link.get_text(strip=True)
            if not title or len(title) < 5:
                return None
            
            # URL 처리
            href = title_link.get('href')
            if href.startswith('/'):
                url = urljoin(self.base_url, href)
            else:
                url = href
            
            # 동아일보 기사 URL 패턴 확인 (정치 섹션만)
            if '/article/' not in url or 'donga.com' not in url:
                return None
            
            # Opinion 섹션 제외 (정치 기사만)
            if '/Opinion/' in url:
                return None
            
            # 발행 시간 추출 (동아일보는 상대적 시간 사용)
            published_at = self._extract_published_time(article_element)
            
            return {
                "title": title.strip(),
                "url": url,
                "content": "",  # 본문은 나중에 Playwright로 채움
                "published_at": published_at,
                "created_at": published_at,  # 발행 시간과 동일하게 설정
            }
            
        except Exception as e:
            console.print(f"⚠️ 기사 파싱 실패: {str(e)}")
            return None

    def _extract_published_time(self, article_element) -> Optional[datetime]:
        """기사 요소에서 발행 시간 추출"""
        try:
            # 동아일보는 상대적 시간 표시 ("1시간 전", "2시간 전" 등)
            time_elem = article_element.select_one('.date, .time, .publish_date')
            if time_elem:
                time_text = time_elem.get_text(strip=True)
                
                # "1시간 전", "2시간 전" 등의 상대적 시간 처리
                if '시간 전' in time_text:
                    import re
                    hours_match = re.search(r'(\d+)시간 전', time_text)
                    if hours_match:
                        hours = int(hours_match.group(1))
                        return datetime.now(KST).replace(tzinfo=None) - timedelta(hours=hours)
                
                # "1일 전", "2일 전" 등의 처리
                elif '일 전' in time_text:
                    import re
                    days_match = re.search(r'(\d+)일 전', time_text)
                    if days_match:
                        days = int(days_match.group(1))
                        return datetime.now(KST).replace(tzinfo=None) - timedelta(days=days)
                
                # "1분 전", "2분 전" 등의 처리
                elif '분 전' in time_text:
                    import re
                    minutes_match = re.search(r'(\d+)분 전', time_text)
                    if minutes_match:
                        minutes = int(minutes_match.group(1))
                        return datetime.now(KST).replace(tzinfo=None) - timedelta(minutes=minutes)
            
            # 기본값: 현재 시간
            return datetime.now(KST).replace(tzinfo=None)
            
        except Exception as e:
            console.print(f"⚠️ 시간 추출 실패: {str(e)}")
            return datetime.now(KST).replace(tzinfo=None)

    def _add_article(self, article: Dict) -> bool:
        """기사를 목록에 추가 (중복 체크)"""
        url = article.get("url")
        if not url:
            return False
        
        # URL 기반 중복 체크
        for existing_article in self.articles:
            if existing_article.get("url") == url:
                return False
        
        self.articles.append(article)
        return True

    async def _extract_content(self, url: str) -> str:
        """Playwright를 사용한 본문 추출 (맥북 에어 M2 최적화)"""
        if not self._browser:
            return ""
        
        try:
            page = await self._browser.new_page()
            
            # 메모리 최적화 설정
            await page.set_viewport_size({"width": 1280, "height": 720})
            
            await page.goto(url, wait_until="domcontentloaded", timeout=10000)
            
            # JavaScript로 본문 추출 시도 (동아일보 특화)
            content = await page.evaluate("""
                () => {
                    // 동아일보 본문 선택자들 (우선순위 순) - section.news_view가 실제 본문
                    const selectors = [
                        'section.news_view',
                        'section.news_view .article_txt',
                        'section.news_view .content',
                        'section.news_view .article_body',
                        '.article_txt',
                        '.content',
                        '.article_body',
                        'main .article_txt',
                        'article .article_txt'
                    ];
                    
                    for (const selector of selectors) {
                        const element = document.querySelector(selector);
                        if (element) {
                            // 광고와 불필요한 요소 제거 (동아일보 특화)
                            const unwanted = element.querySelectorAll('.ad, .advertisement, .view_ad06, .view_m_adA, .view_m_adB, .view_m_adC, .view_m_adK, .a1, script, .related_news, .social_share, .recommend_keyword, .keyword_list, .company_info, .footer_info, .contact_info, .copyright, .publish_info, .img_cont, .articlePhotoC, .sub_tit');
                            unwanted.forEach(el => el.remove());
                            
                            // p 태그와 직접 텍스트 모두 추출
                            const paragraphs = element.querySelectorAll('p');
                            let content = '';
                            
                            if (paragraphs.length > 0) {
                                content = Array.from(paragraphs)
                                    .slice(0, 20)  // 최대 20개 문단으로 제한
                                    .map(p => p.textContent.trim())
                                    .filter(text => {
                                        // 불필요한 텍스트 필터링
                                        if (text.length < 10) return false;
                                        if (text.includes('추천 검색어')) return false;
                                        if (text.includes('입력 2025-')) return false;
                                        if (text.includes('글자크기 설정')) return false;
                                        if (text.includes('디지털랩 디지털뉴스팀')) return false;
                                        if (text.includes('사실만 쓰려고 노력하겠습니다')) return false;
                                        if (text.includes('댓글을 입력해 주세요')) return false;
                                        if (text.includes('주소 서울특별시')) return false;
                                        if (text.includes('전화번호 02-')) return false;
                                        if (text.includes('등록번호 서울아')) return false;
                                        if (text.includes('발행일자 1996')) return false;
                                        if (text.includes('등록일자 2009')) return false;
                                        if (text.includes('발행·편집인')) return false;
                                        if (text.includes('서울특별시 종로구')) return false;
                                        if (text.includes('서울특별시 서대문구')) return false;
                                        if (text.includes('기자 사진')) return false;
                                        if (text.includes('디지털랩 디지털뉴스팀')) return false;
                                        if (text.includes('구독')) return false;
                                        if (text.includes('추천')) return false;
                                        if (text.includes('일상이 역사가 되는 시간')) return false;
                                        if (text.includes('연이 닿아 시간을 공유해주신')) return false;
                                        if (text.includes('깊이 감사드립니다')) return false;
                                        return true;
                                    })
                                    .join('\\n\\n');
                            }
                            
                            // p 태그가 없으면 직접 텍스트 추출 (br 태그로 구분된 텍스트)
                            if (!content || content.length < 50) {
                                const textContent = element.textContent || element.innerText || '';
                                const lines = textContent.split(/\\n|\\r\\n|\\r/).map(line => line.trim()).filter(line => line.length > 10);
                                content = lines.join('\\n\\n');
                            }
                            
                            return content;
                        }
                    }
                    
                    // fallback: 모든 p 태그에서 추출 (더 관대한 필터링)
                    const allParagraphs = document.querySelectorAll('p');
                    return Array.from(allParagraphs)
                        .slice(0, 30)  // 더 많은 문단 허용
                        .map(p => p.textContent.trim())
                        .filter(text => {
                            // 불필요한 텍스트 필터링
                            if (text.length < 5) return false;
                            if (text.includes('추천 검색어')) return false;
                            if (text.includes('입력 2025-')) return false;
                            if (text.includes('글자크기 설정')) return false;
                            if (text.includes('디지털랩 디지털뉴스팀')) return false;
                            if (text.includes('사실만 쓰려고 노력하겠습니다')) return false;
                            if (text.includes('댓글을 입력해 주세요')) return false;
                            if (text.includes('주소 서울특별시')) return false;
                            if (text.includes('전화번호 02-')) return false;
                            if (text.includes('등록번호 서울아')) return false;
                            if (text.includes('발행일자 1996')) return false;
                            if (text.includes('등록일자 2009')) return false;
                            if (text.includes('발행·편집인')) return false;
                            if (text.includes('서울특별시 종로구')) return false;
                            if (text.includes('서울특별시 서대문구')) return false;
                            if (text.includes('기자 사진')) return false;
                            if (text.includes('디지털랩 디지털뉴스팀')) return false;
                            if (text.includes('구독')) return false;
                            if (text.includes('추천')) return false;
                            if (text.includes('일상이 역사가 되는 시간')) return false;
                            if (text.includes('연이 닿아 시간을 공유해주신')) return false;
                            if (text.includes('깊이 감사드립니다')) return false;
                            return true;
                        })
                        .join('\\n\\n');
                }
            """)
            
            await page.close()
            
            if content and len(content) > 30:  # 더 관대한 길이 기준
                return content.strip()
            else:
                # BeautifulSoup fallback 시도
                try:
                    html = await page.content()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # 동아일보 본문 선택자들 (section.news_view가 실제 본문)
                    content_selectors = [
                        'section.news_view',
                        'section.news_view .article_txt',
                        'section.news_view .content',
                        'section.news_view .article_body',
                        '.article_txt',
                        '.content',
                        '.article_body'
                    ]
                    
                    for selector in content_selectors:
                        element = soup.select_one(selector)
                        if element:
                            # 불필요한 요소 제거
                            for unwanted in element.select('.ad, .advertisement, .view_ad06, .view_m_adA, .view_m_adB, .view_m_adC, .view_m_adK, .a1, script, .related_news, .social_share, .recommend_keyword, .keyword_list, .company_info, .footer_info, .contact_info, .copyright, .publish_info, .img_cont, .articlePhotoC, .sub_tit'):
                                unwanted.decompose()
                            
                            content = element.get_text(separator='\n', strip=True)
                            if content and len(content) > 30:
                                # 텍스트 필터링
                                lines = content.split('\n')
                                filtered_lines = []
                                for line in lines:
                                    line = line.strip()
                                    if (len(line) > 10 and 
                                        '추천 검색어' not in line and
                                        '입력 2025-' not in line and
                                        '글자크기 설정' not in line and
                                        '디지털랩 디지털뉴스팀' not in line and
                                        '사실만 쓰려고 노력하겠습니다' not in line and
                                        '댓글을 입력해 주세요' not in line and
                                        '주소 서울특별시' not in line and
                                        '전화번호 02-' not in line and
                                        '등록번호 서울아' not in line and
                                        '발행일자 1996' not in line and
                                        '등록일자 2009' not in line and
                                        '발행·편집인' not in line and
                                        '기자 사진' not in line and
                                        '디지털랩 디지털뉴스팀' not in line and
                                        '구독' not in line and
                                        '추천' not in line and
                                        '일상이 역사가 되는 시간' not in line and
                                        '연이 닿아 시간을 공유해주신' not in line and
                                        '깊이 감사드립니다' not in line):
                                        filtered_lines.append(line)
                                
                                if filtered_lines:
                                    return '\n\n'.join(filtered_lines)
                    
                    return ""
                except:
                    return ""
                
        except Exception as e:
            console.print(f"⚠️ 본문 추출 실패 ({url}): {str(e)}")
            try:
                await page.close()
            except:
                pass
            return ""

    async def collect_contents(self):
        """본문 수집 (배치 처리로 메모리 최적화)"""
        if not self.articles:
            console.print("⚠️ 수집된 기사가 없습니다.")
            return
        
        console.print(f"📖 본문 수집 시작: {len(self.articles)}개 기사 (배치 처리)")
        
        # Playwright 초기화 (맥북 에어 M2 최적화)
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
        
        try:
            batch_size = 2  # 배치 크기 (메모리 절약, 안정성 향상)
            total_batches = (len(self.articles) + batch_size - 1) // batch_size
            
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(self.articles))
                batch_articles = self.articles[start_idx:end_idx]
                
                console.print(f"📄 배치 {batch_idx + 1}/{total_batches} 처리 중...")
                
                for i, article in enumerate(batch_articles):
                    article_idx = start_idx + i + 1
                    url = article["url"]
                    
                    content = await self._extract_content(url)
                    if content:
                        article["content"] = content
                        console.print(f"✅ [{article_idx}/{len(self.articles)}] 본문 수집 성공")
                    else:
                        console.print(f"⚠️ [{article_idx}/{len(self.articles)}] 본문 수집 실패")
                    
                    # 기사 간 딜레이
                    await asyncio.sleep(0.5)
                
                # 배치 간 딜레이 (메모리 정리)
                if batch_idx < total_batches - 1:
                    console.print("⏳ 배치 간 대기 중... (메모리 정리)")
                    await asyncio.sleep(2)
            
            console.print(f"✅ 본문 수집 완료: {len([a for a in self.articles if a.get('content')])}/{len(self.articles)}개 성공")
            
        finally:
            # Playwright 정리
            if self._browser:
                await self._browser.close()
            if self._playwright:
                await self._playwright.stop()
            self._browser = None
            self._playwright = None

    async def save_to_supabase(self):
        """Supabase에 기사 저장"""
        if not self.articles:
            console.print("⚠️ 저장할 기사가 없습니다.")
            return
        
        console.print(f"💾 Supabase에 {len(self.articles)}개 기사 저장 중...")
        
        # 언론사 정보 가져오기
        media_info = self.supabase_manager.get_media_outlet(self.media_name)
        if not media_info:
            media_id = self.supabase_manager.create_media_outlet(self.media_name, self.media_bias, self.base_url)
            if not media_id:
                console.print("❌ 언론사 정보를 가져올 수 없습니다.")
                return
        else:
            media_id = media_info['id']
        
        # 기존 기사 URL 중복 체크
        existing_urls = set()
        try:
            for article in self.articles:
                url = article.get("url")
                if url:
                    existing_urls.add(url)
            
            if existing_urls:
                # Supabase에서 기존 URL들 조회
                result = self.supabase_manager.client.table('articles').select('url').in_('url', list(existing_urls)).execute()
                existing_urls_in_db = {row['url'] for row in result.data}
                console.print(f"🔍 기존 기사 중복 체크 중...")
                console.print(f"📊 중복 체크 완료: {len(existing_urls_in_db)}개 중복 발견")
        except Exception as e:
            console.print(f"⚠️ 중복 체크 실패: {str(e)}")
            existing_urls_in_db = set()
        
        # 기사 저장
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        for i, article in enumerate(self.articles, 1):
            try:
                url = article.get("url")
                
                # 중복 체크
                if url in existing_urls_in_db:
                    console.print(f"⚠️ [{i}/{len(self.articles)}] 중복 기사 스킵: {article['title'][:50]}...")
                    skipped_count += 1
                    continue
                
                # 기사 데이터 준비
                article_data = {
                    "media_id": media_id,
                    "title": article["title"],
                    "content": article.get("content", ""),
                    "url": url,
                    "published_at": article["published_at"].strftime('%Y-%m-%d %H:%M:%S') if article["published_at"] else None,
                    "created_at": article["created_at"].strftime('%Y-%m-%d %H:%M:%S') if article["created_at"] else None
                }
                
                # Supabase에 저장
                if self.supabase_manager.insert_article(article_data):
                    console.print(f"✅ [{i}/{len(self.articles)}] 저장 성공: {article['title'][:50]}...")
                    success_count += 1
                else:
                    console.print(f"❌ [{i}/{len(self.articles)}] 저장 실패: {article['title'][:50]}...")
                    failed_count += 1
                    
            except Exception as e:
                console.print(f"❌ [{i}/{len(self.articles)}] 저장 오류: {str(e)}")
                failed_count += 1
        
        # 결과 출력
        console.print(f"\n📊 저장 결과:")
        console.print(f"  ✅ 성공: {success_count}개")
        console.print(f"  ❌ 실패: {failed_count}개")
        console.print(f"  ⚠️ 중복 스킵: {skipped_count}개")
        console.print(f"  📈 성공률: {success_count/(success_count+failed_count)*100:.1f}%" if (success_count+failed_count) > 0 else "  📈 성공률: 0.0%")

    async def cleanup(self):
        """리소스 정리"""
        try:
            if self._browser:
                await self._browser.close()
            if self._playwright:
                await self._playwright.stop()
        except Exception as e:
            console.print(f"⚠️ 리소스 정리 중 오류: {str(e)[:50]}")

    async def run(self):
        """실행 (에러 처리 강화)"""
        try:
            console.print(f"🚀 동아일보 정치 기사 크롤링 시작 (최신 100개)")
            console.print("💡 맥북 에어 M2 최적화 모드로 실행됩니다")
            
            await self._collect_latest_articles()
            await self.collect_contents()
            await self.save_to_supabase()
            
            console.print("🎉 크롤링 완료!")
            
        except KeyboardInterrupt:
            console.print("\n⚠️ 사용자에 의해 중단되었습니다.")
        except Exception as e:
            console.print(f"❌ 크롤링 중 오류 발생: {str(e)}")
        finally:
            await self.cleanup()
            console.print("🧹 Playwright 리소스 정리 완료")

async def main():
    """메인 함수"""
    collector = DongaPoliticsCollector()
    await collector.run()

if __name__ == "__main__":
    asyncio.run(main())