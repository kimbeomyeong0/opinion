#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
연합뉴스 정치 기사 크롤러 (최적화 버전)
- 최신 정치 기사 100개 수집
- 페이지네이션 활용
- 20초 내 크롤링 완료 목표
- 맥북 에어 M2 최적화
"""

import asyncio
import httpx
import time
import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
import logging
from playwright.async_api import async_playwright

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.supabase_manager import SupabaseManager

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YonhapPoliticsCrawler:
    """연합뉴스 정치 기사 크롤러"""
    
    def __init__(self, max_articles: int = 100):
        self.max_articles = max_articles
        self.console = Console()
        self.supabase_manager = SupabaseManager()
        
        # 연합뉴스 설정
        self.base_url = "https://www.yna.co.kr"
        self.politics_url = "https://www.yna.co.kr/politics/all"
        
        # 연합뉴스는 중립 성향
        self.media_name = "연합뉴스"
        self.media_bias = None  # 초기화 시점에는 None으로 설정
        
        # 통계
        self.total_articles = 0
        self.successful_articles = 0
        self.failed_articles = 0
        self.start_time: Optional[datetime] = None
        
        # media_id 초기화
        self.media_id = None
        
        # 페이지네이션 설정
        self.page_size = 20  # 페이지당 기사 수 (추정)
        self.max_pages = 10  # 최대 페이지 수
        
    async def _get_media_info(self) -> tuple[int, str]:
        """media_outlets 테이블에서 연합뉴스의 ID와 bias를 가져옵니다."""
        try:
            # media_outlets 테이블에서 연합뉴스 찾기
            result = self.supabase_manager.client.table('media_outlets').select('*').eq('name', self.media_name).execute()
            
            if result.data:
                media_id = result.data[0]['id']
                self.media_bias = result.data[0]['bias']  # media_outlets 테이블에서 bias 가져옴
                logger.info(f"✅ {self.media_name} media_id: {media_id}, bias: {self.media_bias}")
                return media_id, self.media_bias
            else:
                # 없으면 생성 (기본값으로 center 사용)
                media_id = self.supabase_manager.create_media_outlet(self.media_name, "center")
                self.media_bias = "center"
                logger.info(f"✅ {self.media_name} 생성됨 - media_id: {media_id}, bias: {self.media_bias}")
                return media_id, self.media_bias
                
        except Exception as e:
            logger.error(f"❌ media_info 가져오기 실패: {str(e)}")
            # 기본값 사용 (media_outlets 테이블 기준)
            self.media_bias = "center"
            return 17, self.media_bias  # 연합뉴스

    async def _get_media_id(self) -> int:
        """media_outlets 테이블에서 연합뉴스의 ID를 가져옵니다. (호환성 유지)"""
        media_id, _ = await self._get_media_info()
        return media_id
        
    async def _collect_latest_articles(self):
        """페이지네이션 기반 최신 100개 기사 수집 (중복 제외) - Playwright 사용"""
        self.console.print("🔌 페이지네이션을 통한 최신 기사 100개 수집 시작...")
        target_count = 100
        page = 1
        page_offset = 0
        max_attempts = 20  # 최대 20페이지 시도

        async with async_playwright() as p:
            # 브라우저 실행 (맥북 에어 M2 최적화)
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--memory-pressure-off',
                    '--max_old_space_size=4096'
                ]
            )
            
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            
            collected_articles = []
            
            while len(collected_articles) < target_count and page <= max_attempts:
                try:
                    # 페이지 URL 구성
                    if page == 1:
                        url = self.politics_url
                    else:
                        url = f"{self.politics_url}/{page}"
                    
                    self.console.print(f"📡 페이지 {page}/{max_attempts} 처리 중 (offset: {page_offset})")
                    
                    # Playwright로 페이지 로드
                    page_obj = await context.new_page()
                    await page_obj.goto(url, wait_until="domcontentloaded", timeout=10000)
                    
                    # HTML 파싱
                    html = await page_obj.content()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # 연합뉴스 기사 링크 추출 - 더 넓은 범위의 선택자 사용
                    article_elements = soup.select('a[href*="/view/"]')
                    
                    # 디버깅: 첫 번째 요소의 HTML 구조 확인
                    if article_elements and page == 1:
                        first_elem = article_elements[0]
                        self.console.print(f"🔍 첫 번째 요소 HTML: {str(first_elem)[:200]}...")
                        self.console.print(f"🔍 첫 번째 요소 텍스트: '{first_elem.get_text(strip=True)}'")
                    
                    self.console.print(f"📊 페이지 {page} 응답: {len(article_elements)}개 요소 수신")
                    
                    page_articles = []
                    collected_urls = set(article['url'] for article in collected_articles)  # 중복 체크용 set
                    
                    for element in article_elements:
                        href = element.get('href')
                        if href and '/view/' in href:
                            if href.startswith('/'):
                                full_url = f"{self.base_url}{href}"
                            else:
                                full_url = href
                            
                            # 중복 체크 (set 사용으로 성능 향상)
                            if full_url not in collected_urls:
                                # 제목 추출 강화 - 연합뉴스는 img alt 속성에서 제목 추출
                                title = element.get_text(strip=True)
                                
                                # 텍스트가 없으면 img alt 속성에서 제목 추출
                                if not title or len(title) < 5:
                                    img_elem = element.find('img')
                                    if img_elem and img_elem.get('alt'):
                                        title = img_elem.get('alt').strip()
                                
                                # 제목 검증 및 정리
                                if title and len(title) > 5:
                                    # 불필요한 문자 제거
                                    title = title.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
                                    title = ' '.join(title.split())  # 연속된 공백 정리
                                    
                                    # 제목이 너무 길면 자르기 (200자 제한)
                                    if len(title) > 200:
                                        title = title[:200] + "..."
                                    
                                    # 제목이 실제 제목인지 확인 (기본 텍스트가 아닌지)
                                    if title not in ['연합뉴스', '뉴스', '기사', '정치', '경제', '사회', '더보기', '목록', '이전', '다음']:
                                        self.console.print(f"📰 제목 추출: {title[:50]}...")
                                    else:
                                        title = f"연합뉴스 기사 {len(collected_articles) + 1}"
                                        self.console.print(f"📰 제목 추출: {title[:50]}...")
                                else:
                                    title = f"연합뉴스 기사 {len(collected_articles) + 1}"
                                    self.console.print(f"📰 제목 추출: {title[:50]}...")
                                
                                page_articles.append({
                                    'url': full_url,
                                    'title': title,
                                    'published_at': datetime.now().isoformat()
                                })
                                collected_urls.add(full_url)  # set에 추가
                    
                    # 페이지 기사들을 전체 목록에 추가
                    collected_articles.extend(page_articles)
                    
                    self.console.print(f"📈 페이지 {page} 파싱 성공: {len(page_articles)}개, 최종 추가: {len(page_articles)}개")
                    self.console.print(f"📊 현재 수집된 기사: {len(collected_articles)}개")
                    
                    await page_obj.close()
                    
                    # 목표 달성 시 중단
                    if len(collected_articles) >= target_count:
                        break
                    
                    page += 1
                    page_offset += len(page_articles)
                    
                    # 페이지 간 대기 (과부하 방지)
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    self.console.print(f"❌ 페이지 {page} 처리 실패: {str(e)}")
                    page += 1
                    continue
            
            await browser.close()
            
            # 목표 개수만큼 자르기
            final_articles = collected_articles[:target_count]
            self.console.print(f"🎯 수집 완료: {len(final_articles)}개 기사 (목표: {target_count}개)")
            
            return final_articles

    async def collect_contents(self):
        """수집된 기사들의 본문 내용 추출 - Playwright 사용"""
        if not hasattr(self, 'articles') or not self.articles:
            self.console.print("❌ 수집된 기사가 없습니다.")
            return
        
        self.console.print(f"📖 본문 수집 시작: {len(self.articles)}개 기사 (배치 처리)")
        
        # 배치 크기 설정 (메모리 최적화)
        batch_size = 5  # Playwright는 더 무거우므로 배치 크기 줄임
        total_batches = (len(self.articles) + batch_size - 1) // batch_size
        
        async with async_playwright() as p:
            # 브라우저 실행 (맥북 에어 M2 최적화)
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--memory-pressure-off',
                    '--max_old_space_size=4096'
                ]
            )
            
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(self.articles))
                batch_articles = self.articles[start_idx:end_idx]
                
                self.console.print(f"📄 배치 {batch_idx + 1}/{total_batches} 처리 중...")
                
                # 배치 내 기사들 순차 처리 (Playwright는 동시 처리 시 메모리 부족 위험)
                for i, article in enumerate(batch_articles):
                    try:
                        result = await self._fetch_article_content_playwright(context, article, start_idx + i + 1)
                        if result:
                            self.console.print(f"✅ [{start_idx + i + 1}/{len(self.articles)}] 본문 수집 성공")
                            self.successful_articles += 1
                        else:
                            self.console.print(f"⚠️ [{start_idx + i + 1}/{len(self.articles)}] 본문 수집 실패 (내용 없음)")
                            self.failed_articles += 1
                    except Exception as e:
                        self.console.print(f"❌ [{start_idx + i + 1}/{len(self.articles)}] 본문 수집 실패: {str(e)}")
                        self.failed_articles += 1
                
                # 배치 간 대기 (메모리 정리)
                if batch_idx < total_batches - 1:
                    self.console.print("⏳ 배치 간 대기 중... (메모리 정리)")
                    await asyncio.sleep(2)
            
            await browser.close()
        
        self.console.print(f"✅ 본문 수집 완료: {self.successful_articles}/{len(self.articles)}개 성공")

    async def _fetch_article_content_playwright(self, context, article: Dict[str, Any], index: int) -> bool:
        """Playwright를 사용한 개별 기사 본문 추출"""
        try:
            page = await context.new_page()
            await page.goto(article['url'], wait_until="domcontentloaded", timeout=10000)
            
            # HTML 파싱
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            # 연합뉴스 본문 추출
            content = None
            content_selectors = [
                'div.story-news.article p',  # 연합뉴스 실제 본문
                '.article-content p',        # 연합뉴스 본문 섹션
                '.news-content p',
                'div.article p',
                '.content p',
                '.body p',
                '.text p',
                'article p',
                '.desc p',
                'div.story-news p',          # 연합뉴스 스토리 뉴스
                '.story p',                  # 스토리 섹션
                'div.article-body p',        # 기사 본문
                '.article-body p',           # 기사 본문
                'div.news-body p',           # 뉴스 본문
                '.news-body p'               # 뉴스 본문
            ]
            
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    # 불필요한 요소 제거
                    for unwanted in content_elem.select('script, style, .ad, .advertisement, .related, .comment, .ui-control, .font-control, .share-control, .sidebar, .side-news, .related-news, .reading-mode, .dark-mode, .font-size, .bookmark, .print, .share, .reaction, .recommend, .like, .subscribe, .donation, .footer, .navigation, .menu, .header, .banner'):
                        unwanted.decompose()
                    
                    content = content_elem.get_text(strip=True, separator='\n')
                    if content and len(content) > 100:
                        # 불필요한 텍스트 패턴 제거
                        unwanted_patterns = [
                            '연합뉴스', '저작권자', '재배포 금지', 'yna.co.kr',
                            '기자 이메일', '기자 연락처', '광고', 'sponsored',
                            '관련기사', '더보기', '목록', '이전글', '다음글',
                            '구독', '기사 후원하기', '카카오톡', '라인', '링크복사',
                            '펼치기/접기', '기사 읽기', '요약', '닫기'
                        ]
                        
                        for pattern in unwanted_patterns:
                            content = content.replace(pattern, '')
                        
                        # 기자 정보 이후의 모든 불필요한 내용 제거
                        lines = content.split('\n')
                        cleaned_lines = []
                        for line in lines:
                            # 기자 정보를 찾으면 그 이후는 모두 제거
                            if '기자' in line and len(line.strip()) < 30:
                                cleaned_lines.append(line.strip())
                                break
                            cleaned_lines.append(line)
                        
                        content = '\n'.join(cleaned_lines)
                        
                        # 정규식 패턴으로 추가 정리
                        import re
                        
                        # 이메일 주소 제거
                        content = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '', content)
                        
                        # 해시태그 제거
                        content = re.sub(r'#\s*\w+', '', content)
                        
                        # 숫자만 있는 라인 제거
                        content = re.sub(r'^\d+$', '', content, flags=re.MULTILINE)
                        
                        # 시간 형식 제거 (12:10 같은)
                        content = re.sub(r'^\d{1,2}:\d{2}$', '', content, flags=re.MULTILINE)
                        
                        # 연속된 빈 줄 정리
                        content = '\n'.join([line.strip() for line in content.split('\n') if line.strip()])
                        
                        if content and len(content) > 100:
                            break
            
            if not content or len(content) < 100:
                await page.close()
                return False
            
            # 제목 추출 (더 정확한 제목) - 연합뉴스 특화
            title = article.get('title', '')
            
            # 링크에서 추출한 제목이 기본값이면 본문에서 재추출
            if not title or len(title) < 10 or title.startswith('연합뉴스 기사'):
                # 연합뉴스 제목 선택자 (우선순위 순)
                title_selectors = [
                    'strong.tit-news a',     # 연합뉴스 메인 제목
                    'h1.title',              # 일반 제목
                    '.title h1',             # 제목 래퍼
                    'h1',                    # 기본 h1
                    'h2',                    # 기본 h2
                    'h3'                     # 기본 h3
                ]
                
                for selector in title_selectors:
                    title_elem = soup.select_one(selector)
                    if title_elem:
                        candidate_title = title_elem.get_text(strip=True)
                        if candidate_title and len(candidate_title) > 10:
                            # 제목 정리
                            candidate_title = candidate_title.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
                            candidate_title = ' '.join(candidate_title.split())  # 연속된 공백 정리
                            title = candidate_title
                            break
                
                # og:title 메타태그에서 제목 추출 (백업)
                if not title or len(title) < 10:
                    meta_title = soup.find('meta', property='og:title')
                    if meta_title and meta_title.get('content'):
                        title = meta_title.get('content').strip()
                
                # title 태그에서 제목 추출 (백업)
                if not title or len(title) < 10:
                    page_title = soup.find('title')
                    if page_title:
                        title = page_title.get_text(strip=True)
                        # 사이트명 제거 (예: " - 연합뉴스")
                        if ' - ' in title:
                            title = title.split(' - ')[0].strip()
                
                # 제목이 여전히 없으면 URL에서 추출
                if not title or len(title) < 10:
                    title = f"연합뉴스 기사 {index}"
            
            # 발행일 추출
            published_at = article.get('published_at', datetime.now().isoformat())
            date_selectors = [
                'meta[name="article:published_time"]',
                'span.time',
                '.date',
                '.published_date',
                '.article_date',
                '.publish_date'
            ]
            
            for selector in date_selectors:
                if selector.startswith('meta'):
                    date_elem = soup.select_one(selector)
                    if date_elem:
                        date_str = date_elem.get('content')
                        if date_str:
                            try:
                                # ISO 8601 형식 파싱
                                published_at = datetime.fromisoformat(date_str.replace('Z', '+00:00')).isoformat()
                                break
                            except:
                                pass
                else:
                    date_elem = soup.select_one(selector)
                    if date_elem:
                        date_text = date_elem.get_text(strip=True)
                        if date_text:
                            # 연합뉴스 날짜 형식: "2025-08-21 21:35"
                            try:
                                date_obj = datetime.strptime(date_text, '%Y-%m-%d %H:%M')
                                published_at = date_obj.isoformat()
                                break
                            except:
                                pass
            
            # 기사 정보 업데이트
            article.update({
                'title': title,
                'content': content,
                'published_at': published_at
            })
            
            await page.close()
            return True
            
        except Exception as e:
            logger.error(f"기사 본문 추출 실패 ({article['url']}): {str(e)}")
            return False

    async def save_to_supabase(self):
        """Supabase에 기사 저장"""
        if not hasattr(self, 'articles') or not self.articles:
            self.console.print("❌ 저장할 기사가 없습니다.")
            return
        
        self.console.print(f"💾 Supabase에 {len(self.articles)}개 기사 저장 중...")
        
        # 기본 이슈 생성 확인
        await self._create_default_issue()
        
        # media_info 가져오기
        media_id, bias = await self._get_media_info()
        
        success_count = 0
        failed_count = 0
        
        for article in self.articles:
            try:
                # 중복 체크
                existing = self.supabase_manager.client.table('articles').select('id').eq('url', article['url']).execute()
                if existing.data:
                    continue  # 이미 존재하는 기사
                
                # 기사 데이터 준비
                article_data = {
                    'title': article['title'],
                    'url': article['url'],
                    'content': article['content'],
                    'published_at': article['published_at'],
                    'media_id': media_id
                }
                
                # Supabase에 저장
                result = self.supabase_manager.insert_article(article_data)
                if result:
                    self.console.print(f"✅ 기사 삽입 성공: {article['title'][:50]}...")
                    success_count += 1
                else:
                    self.console.print(f"❌ 기사 삽입 실패: {article['title'][:50]}...")
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"기사 저장 실패: {str(e)}")
                failed_count += 1
        
        # 결과 표시
        self.console.print(f"\n📊 저장 결과:")
        self.console.print(f"  ✅ 성공: {success_count}개")
        self.console.print(f"  ❌ 실패: {failed_count}개")
        self.console.print(f"  ⚠️ 중복 스킵: {len(self.articles) - success_count - failed_count}개")
        self.console.print(f"  📈 성공률: {(success_count/len(self.articles)*100):.1f}%")

    async def _create_default_issue(self):
        """기본 이슈를 생성합니다."""
        try:
            # 기존 이슈 확인 (UUID 형식으로 수정)
            existing = self.supabase_manager.client.table('issues').select('id').limit(1).execute()
            
            if not existing.data:
                # 기본 이슈 생성 (UUID 자동 생성, date 컬럼 포함)
                issue_data = {
                    'title': '기본 이슈',
                    'subtitle': '크롤러로 수집된 기사들을 위한 기본 이슈',
                    'summary': '다양한 언론사에서 수집된 정치 관련 기사들을 포함하는 기본 이슈입니다.',
                    'date': datetime.now().isoformat()  # date 컬럼 추가
                }
                
                result = self.supabase_manager.client.table('issues').insert(issue_data).execute()
                logger.info("기본 이슈 생성 성공")
            else:
                logger.info("기본 이슈가 이미 존재합니다")
                
        except Exception as e:
            logger.error(f"기본 이슈 생성 실패: {str(e)}")

    async def run(self):
        """실행 (에러 처리 강화)"""
        try:
            self.console.print(f"🚀 연합뉴스 정치 기사 크롤링 시작 (최신 100개)")
            self.console.print("💡 맥북 에어 M2 최적화 모드로 실행됩니다")
            
            # 1단계: 기사 링크 수집
            self.articles = await self._collect_latest_articles()
            
            if not self.articles:
                self.console.print("❌ 수집된 기사가 없습니다.")
                return
            
            # 2단계: 본문 수집
            await self.collect_contents()
            
            # 3단계: Supabase 저장
            await self.save_to_supabase()
            
            self.console.print("🎉 크롤링 완료!")
            
        except Exception as e:
            logger.error(f"크롤링 중 오류 발생: {str(e)}")
            self.console.print(f"❌ 크롤링 중 오류가 발생했습니다: {str(e)}")
        finally:
            # 리소스 정리
            try:
                pass  # httpx 클라이언트는 자동으로 정리됨
            except Exception as e:
                self.console.print(f"⚠️ 리소스 정리 중 오류: {str(e)[:50]}")

async def main():
    """메인 함수"""
    crawler = YonhapPoliticsCrawler(max_articles=100)
    await crawler.run()

if __name__ == "__main__":
    asyncio.run(main())
