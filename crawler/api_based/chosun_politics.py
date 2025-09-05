#!/usr/bin/env python3
"""
조선일보 정치 기사 크롤러 (API 기반)
- story-card-by-id API를 사용하여 개별 기사 정보 수집
- 정치 섹션 기사만 필터링
- 본문은 Playwright로 별도 수집
"""

import asyncio
import json
import sys
import os
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin, quote
import httpx
import pytz
from bs4 import BeautifulSoup
from rich.console import Console
from rich.table import Table
from playwright.async_api import async_playwright

# 프로젝트 루트 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

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

    async def _get_politics_article_ids(self, max_articles: int = 50) -> List[str]:
        """정치 섹션 기사 ID 목록 수집"""
        console.print("🔌 정치 섹션 기사 ID 수집 시작...")
        
        api_base = "https://www.chosun.com/pf/api/v3/content/fetch/story-feed"
        article_ids = []
        offset = 0
        size = 50
        
        async with httpx.AsyncClient(timeout=5.0) as client:  # 타임아웃 단축
            while len(article_ids) < max_articles:
                try:
                    console.print(f"📡 API 호출 (offset: {offset})")
                    
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
                    
                    for element in content_elements:
                        if len(article_ids) >= max_articles:
                            break
                        article_id = element.get("_id")
                        if article_id:
                            article_ids.append(article_id)
                    
                    console.print(f"📈 수집된 기사 ID: {len(article_ids)}개")
                    offset += size
                    # 대기 시간 제거
                    
                except Exception as e:
                    console.print(f"❌ API 호출 오류: {e}")
                    break

        console.print(f"🎯 총 {len(article_ids)}개 기사 ID 수집 완료")
        return article_ids

    async def _get_article_details(self, article_id: str) -> Optional[Dict]:
        """개별 기사 상세 정보 수집"""
        api_url = "https://www.chosun.com/pf/api/v3/content/fetch/story-card-by-id"
        
        # API 필터 설정 (제공해주신 것과 동일)
        filter_data = {
            "_id": "",
            "canonical_url": "",
            "credits": {
                "by": {
                    "_id": "",
                    "additional_properties": {
                        "original": {
                            "affiliations": "",
                            "byline": ""
                        }
                    },
                    "name": "",
                    "org": "",
                    "url": ""
                }
            },
            "description": {
                "basic": ""
            },
            "display_date": "",
            "first_publish_date": "",
            "headlines": {
                "basic": "",
                "mobile": ""
            },
            "label": {
                "membership_icon": {
                    "text": ""
                },
                "shoulder_title": {
                    "text": "",
                    "url": ""
                },
                "video_icon": {
                    "text": ""
                }
            },
            "last_updated_date": "",
            "liveblogging_content": {
                "basic": {
                    "date": "",
                    "headline": "",
                    "id": "",
                    "url": "",
                    "website": ""
                }
            },
            "promo_items": {
                "basic": {
                    "_id": "",
                    "additional_properties": {
                        "focal_point": {
                            "max": "",
                            "min": ""
                        }
                    },
                    "alt_text": "",
                    "caption": "",
                    "content": "",
                    "content_elements": {
                        "_id": "",
                        "alignment": "",
                        "alt_text": "",
                        "caption": "",
                        "content": "",
                        "credits": {
                            "affiliation": {
                                "name": ""
                            },
                            "by": {
                                "_id": "",
                                "byline": "",
                                "name": "",
                                "org": ""
                            }
                        },
                        "height": "",
                        "resizedUrls": {
                            "16x9_lg": "",
                            "16x9_md": "",
                            "16x9_sm": "",
                            "16x9_xxl": ""
                        },
                        "subtype": "",
                        "type": "",
                        "url": "",
                        "width": ""
                    },
                    "credits": {
                        "affiliation": {
                            "byline": "",
                            "name": ""
                        },
                        "by": {
                            "byline": "",
                            "name": ""
                        }
                    },
                    "description": {
                        "basic": ""
                    },
                    "embed_html": "",
                    "focal_point": {
                        "x": "",
                        "y": ""
                    },
                    "headlines": {
                        "basic": ""
                    },
                    "height": "",
                    "promo_items": {
                        "basic": {
                            "_id": "",
                            "height": "",
                            "resizedUrls": {
                                "16x9_lg": "",
                                "16x9_md": "",
                                "16x9_sm": "",
                                "16x9_xxl": ""
                            },
                            "subtype": "",
                            "type": "",
                            "url": "",
                            "width": ""
                        }
                    },
                    "resizedUrls": {
                        "16x9_lg": "",
                        "16x9_md": "",
                        "16x9_sm": "",
                        "16x9_xxl": ""
                    },
                    "streams": {
                        "height": "",
                        "width": ""
                    },
                    "subtype": "",
                    "type": "",
                    "url": "",
                    "websites": "",
                    "width": ""
                },
                "lead_art": {
                    "duration": "",
                    "type": ""
                }
            },
            "related_content": {
                "basic": {
                    "_id": "",
                    "absolute_canonical_url": "",
                    "headlines": {
                        "basic": "",
                        "mobile": ""
                    },
                    "referent": {
                        "id": "",
                        "type": ""
                    },
                    "type": ""
                }
            },
            "subheadlines": {
                "basic": ""
            },
            "subtype": "",
            "taxonomy": {
                "primary_section": {
                    "_id": "",
                    "name": ""
                },
                "tags": {
                    "slug": "",
                    "text": ""
                }
            },
            "type": "",
            "website_url": ""
        }
        
        query_data = {
            "arr": "",
            "expandLiveBlogging": False,
            "expandRelated": False,
            "id": article_id,
            "published": ""
        }
        
        async with httpx.AsyncClient(timeout=5.0) as client:  # 타임아웃 단축
            try:
                params = {
                    "query": json.dumps(query_data),
                    "filter": json.dumps(filter_data),
                    "d": "1925",
                    "mxId": "00000000",
                    "_website": "chosun"
                }
                
                resp = await client.get(api_url, params=params)
                resp.raise_for_status()
                data = resp.json()
                
                return self._parse_article_data(data)
                
            except Exception as e:
                console.print(f"❌ 기사 상세 정보 수집 실패 ({article_id}): {e}")
                return None

    def _parse_article_data(self, data: Dict) -> Optional[Dict]:
        """API 응답 데이터 파싱"""
        try:
            # 필수 필드 확인
            title = data.get("headlines", {}).get("basic")
            if not title:
                return None

            canonical_url = data.get("canonical_url")
            if not canonical_url:
                return None
            url = urljoin(self.base_url, canonical_url) if canonical_url.startswith("/") else canonical_url

            # 날짜 처리 - 여러 필드 중 가장 적절한 것 선택
            published_at = None
            
            # last_updated_date만 사용 (마지막 업데이트 시간)
            date_fields = [
                data.get("last_updated_date")
            ]
            
            for date_field in date_fields:
                if date_field:
                    try:
                        # ISO 형식 정규화
                        if date_field.endswith("Z"):
                            date_field = date_field.replace("Z", "+00:00")
                        
                        # 소수점 자릿수 정규화 (최대 6자리)
                        if "." in date_field and "+" in date_field:
                            # T와 + 사이의 시간 부분 찾기
                            t_index = date_field.find("T")
                            plus_index = date_field.find("+")
                            if t_index != -1 and plus_index != -1:
                                time_part = date_field[t_index+1:plus_index]
                                if "." in time_part:
                                    seconds_part = time_part.split(".")[0]
                                    decimal_part = time_part.split(".")[1]
                                    # 소수점을 6자리로 맞춤
                                    decimal_part = decimal_part.ljust(6, "0")[:6]
                                    normalized_time = seconds_part + "." + decimal_part
                                    date_field = date_field[:t_index+1] + normalized_time + date_field[plus_index:]
                        
                        # API에서 제공하는 시간을 그대로 사용 (변환하지 않음)
                        published_at = date_field
                        break  # 첫 번째로 성공한 날짜 사용
                            
                    except Exception as e:
                        console.print(f"⚠️ 날짜 변환 실패: {date_field} - {e}")
                        continue

            # 기자 정보
            credits = data.get("credits", {}).get("by", [])
            author = ""
            if credits and len(credits) > 0:
                author = credits[0].get("name", "")

            # 섹션 정보
            taxonomy = data.get("taxonomy", {})
            section = taxonomy.get("primary_section", {}).get("name", "")

            # 태그 정보
            tags = taxonomy.get("tags", [])
            tag_list = [tag.get("text", "") for tag in tags if tag.get("text")]

            return {
                "title": title.strip(),
                "url": url,
                "content": "",  # 본문은 나중에 Playwright로 채움
                "published_at": published_at,  # API에서 제공하는 시간 (없을 수도 있음)
                "created_at": datetime.now(KST).isoformat(),  # 수집 시점의 현재 시간 (항상 존재)
                "author": author,
                "section": section,
                "tags": tag_list,
                "description": data.get("description", {}).get("basic", ""),
            }
        except Exception as e:
            console.print(f"❌ 데이터 파싱 실패: {e}")
            return None

    async def _collect_articles(self, max_articles: int = 100):
        """기사 수집 (ID 수집 → 상세 정보 수집) - 병렬 처리"""
        console.print(f"🚀 조선일보 정치 기사 수집 시작 (최대 {max_articles}개)")
        
        # 1단계: 기사 ID 수집
        article_ids = await self._get_politics_article_ids(max_articles)
        
        if not article_ids:
            console.print("❌ 수집할 기사가 없습니다.")
            return

        # 2단계: 병렬로 기사 상세 정보 수집 (20개씩 배치)
        console.print(f"📖 {len(article_ids)}개 기사 상세 정보 수집 중... (병렬 처리)")
        
        batch_size = 20
        success_count = 0
        
        for i in range(0, len(article_ids), batch_size):
            batch_ids = article_ids[i:i + batch_size]
            console.print(f"📄 배치 {i//batch_size + 1}/{(len(article_ids) + batch_size - 1)//batch_size} 처리 중...")
            
            # 배치 내에서 병렬 처리
            tasks = [self._get_article_details(article_id) for article_id in batch_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for j, result in enumerate(results):
                if isinstance(result, Exception):
                    console.print(f"❌ [{i + j + 1}/{len(article_ids)}] 오류: {str(result)[:50]}")
                elif result:
                    self.articles.append(result)
                    success_count += 1
                    console.print(f"✅ [{i + j + 1}/{len(article_ids)}] {result['title'][:30]}...")
                else:
                    console.print(f"⚠️ [{i + j + 1}/{len(article_ids)}] 기사 정보 수집 실패")
            
            # 배치 간 짧은 대기
            if i + batch_size < len(article_ids):
                await asyncio.sleep(0.1)

        console.print(f"📊 수집 완료: {success_count}/{len(article_ids)}개 성공")

    async def _extract_content(self, url: str) -> str:
        """Playwright로 본문 전문 추출"""
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
            await page.set_viewport_size({"width": 1280, "height": 720})
            await page.goto(url, wait_until="domcontentloaded", timeout=10000)

            # 조선일보 본문 추출
            content = ""
            
            try:
                content = await page.evaluate('''() => {
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
                                .slice(0, 20);
                            
                            if (texts.length > 0) {
                                return texts.join('\\n\\n');
                            }
                        }
                    }
                    
                    return "";
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
        """본문 전문 수집 - 병렬 처리"""
        if not self.articles:
            return

        console.print(f"📖 본문 수집 시작: {len(self.articles)}개 기사 (병렬 처리)")
        
        batch_size = 10  # 더 큰 배치 크기
        success_count = 0
        
        for i in range(0, len(self.articles), batch_size):
            batch = self.articles[i:i + batch_size]
            console.print(f"📄 배치 {i//batch_size + 1}/{(len(self.articles) + batch_size - 1)//batch_size} 처리 중...")
            
            # 배치 내에서 병렬 처리
            tasks = [self._extract_content(art["url"]) for art in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for j, (art, result) in enumerate(zip(batch, results)):
                if isinstance(result, Exception):
                    console.print(f"❌ [{i + j + 1}/{len(self.articles)}] 오류: {str(result)[:50]}")
                elif result:
                    self.articles[i + j]["content"] = result
                    success_count += 1
                    console.print(f"✅ [{i + j + 1}/{len(self.articles)}] 본문 수집 성공")
                else:
                    console.print(f"⚠️ [{i + j + 1}/{len(self.articles)}] 본문 수집 실패")
            
            # 배치 간 짧은 대기
            if i + batch_size < len(self.articles):
                await asyncio.sleep(0.5)

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

                published_at_str = None
                created_at_str = None
                
                if isinstance(art["published_at"], datetime):
                    published_at_str = art["published_at"].strftime('%Y-%m-%d %H:%M:%S')
                    created_at_str = art["created_at"].strftime('%Y-%m-%d %H:%M:%S') if art.get("created_at") else published_at_str
                elif art.get("published_at"):
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

    async def run(self, max_articles: int = 50):
        """실행"""
        try:
            console.print(f"🚀 조선일보 정치 기사 크롤링 시작 (최대 {max_articles}개)")
            
            await self._collect_articles(max_articles)
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
    await collector.run(max_articles=100)

if __name__ == "__main__":
    asyncio.run(main())