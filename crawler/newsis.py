#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
뉴시스 정치 기사 크롤러 (Playwright 기반)
"""
import os
import sys
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
from urllib.parse import urljoin

# 프로젝트 루트 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.supabase_manager import SupabaseManager

class NewsisCrawler:
    """
    뉴시스 정치 섹션의 기사를 수집하는 크롤러
    """

    def __init__(self):
        load_dotenv()
        self.media_name = "뉴시스"
        self.base_url = "https://www.newsis.com"
        self.article_list_url = "https://www.newsis.com/pol/list/?cid=10300&scid=10301"
        self.supabase_manager = SupabaseManager()
        self.media_info = self._get_media_info()

    async def get_news(self):
        """
        뉴시스 정치 섹션의 최신 기사를 수집하여 반환합니다.
        """
        print(f"{self.media_name} 정치 기사 크롤링 시작")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            article_urls = await self._get_article_urls(browser)
            
            articles = []
            # URL 목록을 역순으로 처리하여 최신 기사부터 저장되도록 함
            for url in reversed(article_urls):
                try:
                    article = await self._get_article_content(browser, url)
                    if article:
                        articles.append(article)
                        print(f"  - 기사 수집 완료: {article['title']}")
                except Exception as e:
                    print(f"  - 기사 수집 중 오류 발생: {url}, 오류: {e}")
            
            await browser.close()
        
        print(f"총 {len(articles)}개의 기사 수집 완료")
        
        if articles:
            self.supabase_manager.save_articles(self.media_info['id'], articles)
        
        return articles

    def _get_media_info(self):
        """
        Supabase에서 언론사 정보를 가져오거나 생성합니다.
        """
        media_info = self.supabase_manager.get_media_outlet(self.media_name)
        if not media_info:
            print(f"'{self.media_name}' 언론사 정보가 없어 새로 생성합니다.")
            media_id = self.supabase_manager.create_media_outlet(self.media_name, "center", self.base_url)
            if not media_id:
                raise Exception(f"'{self.media_name}' 언론사 정보를 생성하지 못했습니다.")
            media_info = {'id': media_id, 'name': self.media_name}
        return media_info

    async def _get_article_urls(self, browser, max_pages=10):
        """
        기사 목록 페이지를 순회하며 기사 URL을 수집합니다.
        """
        urls = []
        page = await browser.new_page()
        try:
            for i in range(1, max_pages + 1):
                page_url = f"{self.article_list_url}&page={i}"
                print(f"기사 목록 페이지 순회 중: {i} / {max_pages}")
                
                try:
                    await page.goto(page_url, wait_until='domcontentloaded', timeout=15000)
                    await page.wait_for_selector('ul.articleList2 li', timeout=10000)
                    
                    content = await page.content()
                    soup = BeautifulSoup(content, 'html.parser')
                    
                    links = soup.select('ul.articleList2 .tit a')
                    
                    if not links:
                        print(f"  - {i} 페이지에서 기사 링크를 찾을 수 없습니다. 중단합니다.")
                        break

                    for link in links:
                        href = link.get('href')
                        if href and href.startswith('/view'):
                            full_url = urljoin(self.base_url, href)
                            if full_url not in urls:
                                urls.append(full_url)
                
                except Exception as e:
                    print(f"  - 페이지 로드/파싱 실패: {page_url}, 오류: {e}")
                    break
        finally:
            await page.close()

        print(f"총 {len(urls)}개의 기사 URL 수집 완료")
        return urls

    async def _get_article_content(self, browser, url):
        """
        개별 기사 페이지에서 제목, 본문, 발행일자를 추출합니다.
        """
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=15000)
            content_html = await page.content()
            soup = BeautifulSoup(content_html, 'html.parser')

            title_tag = soup.select_one('h1.title')
            title = title_tag.get_text(strip=True) if title_tag else "제목 없음"

            content_tag = soup.select_one('div#articleBody')
            if content_tag:
                for unwanted in content_tag.select('div.view_text, script, .photo_area, .copyright'):
                    unwanted.decompose()
                content = content_tag.get_text(separator='\n', strip=True)
            else:
                content = "본문 없음"

            date_tag = soup.select_one('div.info span.date')
            if date_tag:
                date_text = date_tag.get_text(strip=True)
                date_str = date_text.replace('기사입력 :', '').strip()
                try:
                    publication_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                except ValueError:
                    publication_date = datetime.now()
            else:
                publication_date = datetime.now()

            return {
                'url': url,
                'title': title,
                'content': content,
                'publisher': self.media_name,
                'publication_date': publication_date.strftime('%Y-%m-%d %H:%M:%S')
            }

        except Exception as e:
            print(f"  - 기사 내용 파싱 실패: {url}, 오류: {e}")
        finally:
            await page.close()
        
        return None

if __name__ == '__main__':
    asyncio.run(NewsisCrawler().get_news())
