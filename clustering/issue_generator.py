#!/usr/bin/env python3
"""
이슈 생성기 클래스 - KISS 원칙 적용
LLM을 통한 이슈 생성과 DB 저장만 담당하는 단일 책임
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from datetime import datetime
from rich.console import Console
from openai import OpenAI
from functools import lru_cache

from utils.supabase_manager import get_supabase_client
from clustering.config import get_config

console = Console()

class IssueGenerator:
    """이슈 생성기 클래스 - 단일 책임: 이슈 생성 및 저장"""
    
    def __init__(self, clusters_info, articles_data, media_outlets):
        """초기화"""
        self.config = get_config()
        self.supabase = get_supabase_client()
        self.openai_client = OpenAI(api_key=self.config["openai_api_key"])
        self.clusters_info = clusters_info
        self.articles_data = articles_data
        self.media_outlets = media_outlets
        
        # 캐싱을 위한 딕셔너리
        self._bias_classification_cache = {}
        self._content_cache = {}
        
        # 메모리 최적화 설정
        self._max_cache_size = 1000  # 최대 캐시 크기
        self._cache_hit_count = 0
        self._cache_miss_count = 0
    
    async def generate_issue_content(self, cluster_info: dict) -> dict:
        """LLM으로 이슈 내용 생성 - 성향별 기사 분류 기반"""
        try:
            console.print(f"🤖 클러스터 {cluster_info['cluster_id']} 이슈 생성 중...")
            
            # title, subtitle, summary용 (merged_content 사용)
            articles_by_bias = self._classify_articles_by_bias(cluster_info['articles'])
            
            # view용 (articles.content 사용)
            articles_by_bias_with_content = self._classify_articles_by_bias_with_content(cluster_info['articles'])
            
            # 1. title, subtitle 생성 (merged_content 기반)
            title, subtitle = await self._generate_title_and_subtitle(articles_by_bias['all'])
            
            # 2. summary 생성 (모든 기사 본문 - merged_content)
            summary = await self._generate_summary(articles_by_bias['all'])
            
            # 3. 성향별 관점 생성 (articles.content 사용) - 지지/중립/비판 관점 명확화
            left_view = await self._generate_bias_view(articles_by_bias_with_content['left'], '진보적')  # 지지 관점
            center_view = await self._generate_bias_view(articles_by_bias_with_content['center'], '중도적')  # 중립 관점
            right_view = await self._generate_bias_view(articles_by_bias_with_content['right'], '보수적')  # 비판 관점
            
            return {
                'title': title,
                'subtitle': subtitle,
                'summary': summary,
                'left_view': left_view,
                'center_view': center_view,
                'right_view': right_view
            }
            
        except Exception as e:
            console.print(f"❌ 이슈 생성 실패: {e}")
            return {
                'title': f"클러스터 {cluster_info['cluster_id']}",
                'subtitle': f"{cluster_info['size']}개 기사",
                'summary': "내용 분석 중 오류가 발생했습니다.",
                'left_view': "",
                'center_view': "",
                'right_view': ""
            }
    
    def _classify_articles_by_bias(self, articles: list) -> dict:
        """기사를 성향별로 분류 (통합 함수 사용)"""
        unified = self._classify_articles_by_bias_unified(articles)
        return {
            'all': unified['all_merged'],
            'left': unified['left_merged'],
            'center': unified['center_merged'],
            'right': unified['right_merged']
        }
    
    def _select_articles_by_bias_ratio(self, articles: list, max_articles: int) -> list:
        """성향별 비율에 맞춰 기사 선택 (시간순)"""
        # 1단계: 실제 성향 비율 계산
        total = len(articles)
        left_articles = []
        center_articles = []
        right_articles = []
        
        bias_mapping = self.config["media_bias_mapping"]
        
        for article in articles:
            media_id = article.get('media_id', '')
            media_name = self._get_media_name(media_id)
            
            if media_name in bias_mapping['left']:
                left_articles.append(article)
            elif media_name in bias_mapping['center']:
                center_articles.append(article)
            elif media_name in bias_mapping['right']:
                right_articles.append(article)
        
        # 2단계: 비율 계산
        left_ratio = len(left_articles) / total if total > 0 else 0
        center_ratio = len(center_articles) / total if total > 0 else 0
        right_ratio = len(right_articles) / total if total > 0 else 0
        
        # 3단계: 비율에 맞춰 샘플 수 계산
        left_samples = int(max_articles * left_ratio)
        center_samples = int(max_articles * center_ratio)
        right_samples = int(max_articles * right_ratio)
        
        # 4단계: 각 성향별로 최신순으로 선택
        selected = []
        
        # 최신순 정렬 (published_at 기준 내림차순)
        left_sorted = sorted(left_articles, key=lambda x: x.get('published_at', ''), reverse=True)
        center_sorted = sorted(center_articles, key=lambda x: x.get('published_at', ''), reverse=True)
        right_sorted = sorted(right_articles, key=lambda x: x.get('published_at', ''), reverse=True)
        
        # 각 성향별로 최신 기사들 선택
        selected.extend(left_sorted[:left_samples])
        selected.extend(center_sorted[:center_samples])
        selected.extend(right_sorted[:right_samples])
        
        console.print(f"📊 크기 제한 적용: {total}개 → {len(selected)}개 (좌:{left_samples}, 중:{center_samples}, 우:{right_samples})")
        
        return selected
    
    def _classify_articles_by_bias_unified(self, articles: list) -> dict:
        """통합 성향 분류 (캐싱 적용)"""
        # 캐시 키 생성 (기사 ID 목록으로)
        article_ids = tuple(sorted([article.get('article_id', '') for article in articles]))
        cache_key = article_ids
        
        # 캐시 확인
        if cache_key in self._bias_classification_cache:
            self._cache_hit_count += 1
            console.print(f"📊 캐시된 성향 분류 결과 사용")
            return self._bias_classification_cache[cache_key]
        
        self._cache_miss_count += 1
        
        # 크기 제한 적용
        max_articles = self.config["max_articles_per_cluster"]
        if len(articles) > max_articles:
            articles = self._select_articles_by_bias_ratio(articles, max_articles)
        
        # 일괄 content 로딩 (캐싱 적용)
        article_contents = self._load_article_contents_batch_cached(articles)
        
        # 통합 분류 결과
        result = {
            'all_merged': [],      # title, subtitle, summary용
            'all_content': [],     # view용
            'left_merged': [],     # 좌파 merged_content
            'left_content': [],    # 좌파 content
            'center_merged': [],   # 중파 merged_content
            'center_content': [],  # 중파 content
            'right_merged': [],    # 우파 merged_content
            'right_content': []    # 우파 content
        }
        
        bias_mapping = self.config["media_bias_mapping"]
        
        # 한 번만 성향 분류
        for article in articles:
            media_id = article.get('media_id', '')
            media_name = self._get_media_name(media_id)
            
            merged_content = article.get('merged_content', '')
            article_id = article.get('article_id', '')
            content = article_contents.get(article_id, '')
            
            # merged_content 분류
            if merged_content and merged_content.strip():
                result['all_merged'].append(merged_content.strip())
                if media_name in bias_mapping['left']:
                    result['left_merged'].append(merged_content.strip())
                elif media_name in bias_mapping['center']:
                    result['center_merged'].append(merged_content.strip())
                elif media_name in bias_mapping['right']:
                    result['right_merged'].append(merged_content.strip())
            
            # content 분류
            if content and content.strip():
                result['all_content'].append(content.strip())
                if media_name in bias_mapping['left']:
                    result['left_content'].append(content.strip())
                elif media_name in bias_mapping['center']:
                    result['center_content'].append(content.strip())
                elif media_name in bias_mapping['right']:
                    result['right_content'].append(content.strip())
        
        # 결과 캐싱
        self._bias_classification_cache[cache_key] = result
        
        # 캐시 크기 관리
        self._manage_cache_size()
        
        console.print(f"📊 통합 성향 분류: {len(articles)}개 기사 → 좌:{len(result['left_merged'])}, 중:{len(result['center_merged'])}, 우:{len(result['right_merged'])}")
        
        return result
    
    def _load_article_contents_batch(self, articles: list) -> dict:
        """기사들의 content를 일괄 로딩 (중복 DB 조회 제거)"""
        try:
            # article_id 목록 추출
            article_ids = [article.get('article_id', '') for article in articles if article.get('article_id')]
            
            if not article_ids:
                return {}
            
            # 일괄 조회
            result = self.supabase.client.table('articles').select('id,content').in_('id', article_ids).execute()
            
            # 딕셔너리로 변환
            content_dict = {}
            for item in result.data:
                content_dict[item['id']] = item.get('content', '')
            
            console.print(f"📊 일괄 content 로딩: {len(article_ids)}개 기사 → {len(content_dict)}개 성공")
            return content_dict
            
        except Exception as e:
            console.print(f"❌ 일괄 content 로딩 실패: {e}")
            return {}
    
    def _manage_cache_size(self):
        """캐시 크기 관리 (메모리 최적화)"""
        # content 캐시 크기 관리
        if len(self._content_cache) > self._max_cache_size:
            # 가장 오래된 항목들 제거 (간단한 FIFO)
            items_to_remove = len(self._content_cache) - self._max_cache_size
            keys_to_remove = list(self._content_cache.keys())[:items_to_remove]
            for key in keys_to_remove:
                del self._content_cache[key]
            console.print(f"🗑️ content 캐시 정리: {items_to_remove}개 항목 제거")
        
        # bias 분류 캐시 크기 관리
        if len(self._bias_classification_cache) > 100:  # 더 작은 크기로 제한
            items_to_remove = len(self._bias_classification_cache) - 100
            keys_to_remove = list(self._bias_classification_cache.keys())[:items_to_remove]
            for key in keys_to_remove:
                del self._bias_classification_cache[key]
            console.print(f"🗑️ bias 분류 캐시 정리: {items_to_remove}개 항목 제거")
    
    def _get_cache_stats(self) -> dict:
        """캐시 통계 반환"""
        total_requests = self._cache_hit_count + self._cache_miss_count
        hit_rate = (self._cache_hit_count / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'content_cache_size': len(self._content_cache),
            'bias_cache_size': len(self._bias_classification_cache),
            'cache_hits': self._cache_hit_count,
            'cache_misses': self._cache_miss_count,
            'hit_rate': hit_rate
        }
    
    def _load_article_contents_batch_cached(self, articles: list) -> dict:
        """기사들의 content를 일괄 로딩 (캐싱 적용)"""
        try:
            # article_id 목록 추출
            article_ids = [article.get('article_id', '') for article in articles if article.get('article_id')]
            
            if not article_ids:
                return {}
            
            # 캐시에서 먼저 확인
            cached_contents = {}
            uncached_ids = []
            
            for article_id in article_ids:
                if article_id in self._content_cache:
                    cached_contents[article_id] = self._content_cache[article_id]
                else:
                    uncached_ids.append(article_id)
            
            # 캐시에 없는 것들만 DB에서 조회
            if uncached_ids:
                result = self.supabase.client.table('articles').select('id,content').in_('id', uncached_ids).execute()
                
                # 결과를 캐시에 저장
                for item in result.data:
                    content = item.get('content', '')
                    self._content_cache[item['id']] = content
                    cached_contents[item['id']] = content
            
            console.print(f"📊 캐싱된 content 로딩: {len(article_ids)}개 중 {len(uncached_ids)}개 새로 로딩")
            
            # 캐시 크기 관리
            self._manage_cache_size()
            
            return cached_contents
            
        except Exception as e:
            console.print(f"❌ 캐싱된 content 로딩 실패: {e}")
            return {}
    
    def _get_article_content(self, article_id: str) -> str:
        """articles 테이블에서 원본 content 조회"""
        try:
            result = self.supabase.client.table('articles').select('content').eq('id', article_id).execute()
            if result.data:
                return result.data[0].get('content', '')
            return ''
        except Exception as e:
            console.print(f"❌ 기사 내용 조회 실패: {e}")
            return ''
    
    def _classify_articles_by_bias_with_content(self, articles: list) -> dict:
        """기사를 성향별로 분류 (통합 함수 사용)"""
        unified = self._classify_articles_by_bias_unified(articles)
        return {
            'all': unified['all_merged'],
            'left': unified['left_content'],
            'center': unified['center_content'],
            'right': unified['right_content']
        }
    
    async def _generate_title_and_subtitle(self, articles: list) -> tuple:
        """제목과 부제목 생성 (merged_content 기반, 제한 없음)"""
        if not articles:
            return "정치 이슈", "기사 없음"
        
        # 모든 기사의 제목 부분 추출 (제한 없음)
        titles = []
        for article_content in articles:  # 제한 제거
            lines = article_content.split('\n')
            for line in lines:
                if line.startswith('제목:'):
                    title = line.replace('제목:', '').strip()
                    if title:
                        titles.append(title)
                    break
        
        if not titles:
            return "정치 이슈", f"{len(articles)}개 기사"
        
        titles_text = "\n".join(titles)
        prompt = f"""
다음 정치 뉴스 제목들을 분석하여 이슈 제목과 부제목을 만들어주세요:

{titles_text}

다음 형식으로 응답해주세요:
제목: [간결하고 명확한 이슈 제목]
부제목: [이슈에 대한 간단한 설명]
"""
        
        try:
            response = self.openai_client.chat.completions.create(
                model=self.config["openai_model"],
                messages=[{"role": "user", "content": prompt}],
                max_tokens=500,  # 토큰 수 증가
                temperature=0.7
            )
            
            content = response.choices[0].message.content.strip()
            lines = content.split('\n')
            
            title = "정치 이슈"
            subtitle = f"{len(articles)}개 기사"
            
            for line in lines:
                if line.startswith('제목:'):
                    title = line.replace('제목:', '').strip()
                elif line.startswith('부제목:'):
                    subtitle = line.replace('부제목:', '').strip()
            
            return title, subtitle
            
        except Exception as e:
            console.print(f"❌ 제목+부제목 생성 실패: {e}")
            return "정치 이슈", f"{len(articles)}개 기사"
    
    async def _generate_summary(self, articles: list) -> str:
        """간결한 요약 생성 (배치 처리)"""
        if not articles:
            return "내용 분석 중 오류가 발생했습니다."
        
        # 배치 크기 설정 (토큰 제한 고려)
        batch_size = self.config["summary_batch_size"]
        batches = [articles[i:i + batch_size] for i in range(0, len(articles), batch_size)]
        
        console.print(f"📝 간결한 요약 생성 중... ({len(articles)}개 기사를 {len(batches)}개 배치로 처리)")
        
        batch_summaries = []
        
        for i, batch in enumerate(batches):
            try:
                console.print(f"  배치 {i+1}/{len(batches)} 처리 중... ({len(batch)}개 기사)")
                
                content_text = "\n\n".join(batch)
                prompt = f"""
다음 정치 뉴스들을 분석하여 이슈의 핵심 내용을 기승전결 구조로 자연스럽게 요약해주세요:

{content_text}

요약: [기승전결 구조에 맞춰 4-6문장으로 자연스럽게 작성]
- 서론: 핵심 사건의 배경과 상황
- 전개: 주요 전개 상황과 진행 과정  
- 전환: 핵심 쟁점과 갈등 요소
- 결론: 현재 상황과 향후 전망

단, "기/승/전/결" 같은 레이블은 사용하지 말고 논리적 흐름만 드러나도록 자연스럽게 작성해주세요.
"""
                
                response = self.openai_client.chat.completions.create(
                    model=self.config["openai_model"],
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=300,  # 150 → 300으로 증가 (기승전결 구조를 위해)
                    temperature=0.7
                )
                
                content = response.choices[0].message.content.strip()
                if content.startswith('요약:'):
                    summary = content.replace('요약:', '').strip()
                else:
                    summary = content
                
                batch_summaries.append(summary)
                
                # API 제한 방지를 위한 대기
                await asyncio.sleep(2)  # 대기 시간 증가
                
            except Exception as e:
                console.print(f"❌ 배치 {i+1} 요약 생성 실패: {e}")
                batch_summaries.append(f"배치 {i+1} 처리 실패")
        
        # 모든 배치 요약을 종합
        if len(batch_summaries) == 1:
            return batch_summaries[0]
        
        try:
            combined_summaries = "\n\n".join(batch_summaries)
            final_prompt = f"""
다음 요약들을 종합하여 하나의 기승전결 구조의 이슈 요약을 만들어주세요:

{combined_summaries}

통합 요약: [기승전결 구조에 맞춰 4-6문장으로 자연스럽게 통합]
- 서론: 핵심 사건의 배경과 상황
- 전개: 주요 전개 상황과 진행 과정
- 전환: 핵심 쟁점과 갈등 요소  
- 결론: 현재 상황과 향후 전망

단, "기/승/전/결" 같은 레이블은 사용하지 말고 논리적 흐름만 드러나도록 자연스럽게 작성해주세요.
"""
            
            response = self.openai_client.chat.completions.create(
                model=self.config["openai_model"],
                messages=[{"role": "user", "content": final_prompt}],
                max_tokens=400,  # 200 → 400으로 증가 (기승전결 구조를 위해)
                temperature=0.7
            )
            
            content = response.choices[0].message.content.strip()
            if content.startswith('통합 요약:'):
                return content.replace('통합 요약:', '').strip()
            return content
            
        except Exception as e:
            console.print(f"❌ 통합 요약 생성 실패: {e}")
            return "\n".join(batch_summaries)
    
    async def _generate_bias_view(self, articles: list, bias_type: str) -> str:
        """성향별 관점 생성 (지지/중립/비판 관점 명확화)"""
        if not articles:
            return ""
        
        # 배치 크기 설정 (토큰 제한 고려)
        batch_size = self.config["view_batch_size"]
        batches = [articles[i:i + batch_size] for i in range(0, len(articles), batch_size)]
        
        # 관점 타입 매핑
        view_type_mapping = {
            '진보적': '지지',
            '중도적': '중립', 
            '보수적': '비판'
        }
        view_type = view_type_mapping.get(bias_type, bias_type)
        
        console.print(f"📝 {bias_type} 성향 → {view_type} 관점 생성 중... ({len(articles)}개 기사를 {len(batches)}개 배치로 처리)")
        
        batch_views = []
        
        for i, batch in enumerate(batches):
            try:
                console.print(f"  {view_type} 관점 배치 {i+1}/{len(batches)} 처리 중... ({len(batch)}개 기사)")
                
                content_text = "\n\n".join(batch)
                prompt = f"""
다음 {bias_type} 성향의 언론사 기사들을 분석하여 {view_type} 관점에서의 입장을 기승전결 구조로 자연스럽게 작성해주세요:

{content_text}

{view_type} 관점: [기승전결 구조에 맞춰 4-5문장으로 자연스럽게 작성]
- 서론: {view_type} 관점에서 바라본 이슈의 핵심 인식과 기본 입장
- 전개: {view_type} 관점의 핵심 논리와 근거, 구체적 분석
- 전환: {view_type} 관점에서 제기하는 주요 쟁점과 비판/지지 사항
- 결론: {view_type} 관점의 명확한 입장과 향후 방향성

단, "기/승/전/결" 같은 레이블은 사용하지 말고 논리적 흐름만 드러나도록 자연스럽게 작성해주세요.
{view_type} 관점의 뚜렷한 목소리와 주장이 명확히 드러나도록 해주세요.
"""
                
                response = self.openai_client.chat.completions.create(
                    model=self.config["openai_model"],
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=400,  # 250 → 400으로 증가 (기승전결 구조를 위해)
                    temperature=0.7
                )
                
                content = response.choices[0].message.content.strip()
                if content.startswith(f'{view_type} 관점:'):
                    view = content.replace(f'{view_type} 관점:', '').strip()
                else:
                    view = content
                
                batch_views.append(view)
                
                # API 제한 방지를 위한 대기
                await asyncio.sleep(2)  # 대기 시간 증가
                
            except Exception as e:
                console.print(f"❌ {view_type} 관점 배치 {i+1} 생성 실패: {e}")
                batch_views.append(f"{view_type} 관점 배치 {i+1} 처리 실패")
        
        # 모든 배치 관점을 종합
        if len(batch_views) == 1:
            return batch_views[0]
        
        try:
            combined_views = "\n\n".join(batch_views)
            final_prompt = f"""
다음 {view_type} 관점들을 종합하여 기승전결 구조의 일관된 {view_type} 입장을 정리해주세요:

{combined_views}

통합 {view_type} 관점: [기승전결 구조에 맞춰 4-5문장으로 자연스럽게 통합]
- 서론: {view_type} 관점에서 바라본 이슈의 핵심 인식과 기본 입장
- 전개: {view_type} 관점의 핵심 논리와 근거, 구체적 분석
- 전환: {view_type} 관점에서 제기하는 주요 쟁점과 비판/지지 사항
- 결론: {view_type} 관점의 명확한 입장과 향후 방향성

단, "기/승/전/결" 같은 레이블은 사용하지 말고 논리적 흐름만 드러나도록 자연스럽게 작성해주세요.
{view_type} 관점의 뚜렷한 목소리와 주장이 명확히 드러나도록 해주세요.
"""
            
            response = self.openai_client.chat.completions.create(
                model=self.config["openai_model"],
                messages=[{"role": "user", "content": final_prompt}],
                max_tokens=500,  # 350 → 500으로 증가 (기승전결 구조를 위해)
                temperature=0.7
            )
            
            content = response.choices[0].message.content.strip()
            if content.startswith(f'통합 {view_type} 관점:'):
                return content.replace(f'통합 {view_type} 관점:', '').strip()
            return content
            
        except Exception as e:
            console.print(f"❌ 통합 {view_type} 관점 생성 실패: {e}")
            return "\n".join(batch_views)
    
    async def generate_title_and_subtitle(self, cluster_info: dict) -> dict:
        """제목과 부제목을 LLM으로 생성하고 나머지는 기본값 설정 - 기사 본문 기반"""
        try:
            console.print(f"🤖 클러스터 {cluster_info['cluster_id']} 제목+부제목 생성 중...")
            
            # 클러스터의 기사 본문에서 제목 부분만 추출 (최대 설정값만큼)
            article_titles = []
            max_titles = 50  # 하드코딩으로 임시 수정
            
            for article in cluster_info['articles'][:max_titles]:
                merged_content = article.get('merged_content', '')
                if merged_content and merged_content.strip():
                    # merged_content에서 제목 부분 추출 ("제목: ..." 형식)
                    lines = merged_content.split('\n')
                    for line in lines:
                        if line.startswith('제목:'):
                            title = line.replace('제목:', '').strip()
                            if title:
                                article_titles.append(title)
                            break
            
            if not article_titles:
                return {
                    'title': f"정치 이슈 {cluster_info['cluster_id']}",
                    'subtitle': f"{cluster_info['size']}개 기사",
                    'summary': f"클러스터 {cluster_info['cluster_id']}에 속한 {cluster_info['size']}개의 기사들",
                    'left_view': "",
                    'center_view': "",
                    'right_view': ""
                }
            
            # 제목과 부제목 생성 프롬프트
            titles_text = "\n".join(article_titles)
            prompt = f"""
다음 정치 뉴스 제목들을 분석하여 이슈 제목과 부제목을 만들어주세요:

{titles_text}

다음 형식으로 응답해주세요:
제목: [10-20자 이내의 간결한 이슈 제목]
부제목: [이슈에 대한 간단한 설명, 30자 이내]

요구사항:
- 제목: 핵심 키워드 포함, 명확하고 이해하기 쉬운 표현
- 부제목: 이슈의 핵심 내용을 간단히 설명
"""
            
            # OpenAI API 호출
            response = self.openai_client.chat.completions.create(
                model=self.config["openai_model"],
                messages=[{"role": "user", "content": prompt}],
                max_tokens=100,
                temperature=0.7
            )
            
            content = response.choices[0].message.content.strip()
            
            # 응답 파싱
            lines = content.split('\n')
            title = f"정치 이슈 {cluster_info['cluster_id']}"
            subtitle = f"{cluster_info['size']}개 기사"
            
            for line in lines:
                if line.startswith('제목:'):
                    title = line.replace('제목:', '').strip()
                elif line.startswith('부제목:'):
                    subtitle = line.replace('부제목:', '').strip()
            
            # 기본값으로 나머지 필드 설정
            return {
                'title': title,
                'subtitle': subtitle,
                'summary': f"클러스터 {cluster_info['cluster_id']}에 속한 {cluster_info['size']}개의 기사들",
                'left_view': "",
                'center_view': "",
                'right_view': ""
            }
            
        except Exception as e:
            console.print(f"❌ 제목+부제목 생성 실패: {e}")
            return {
                'title': f"정치 이슈 {cluster_info['cluster_id']}",
                'subtitle': f"{cluster_info['size']}개 기사",
                'summary': f"클러스터 {cluster_info['cluster_id']}에 속한 {cluster_info['size']}개의 기사들",
                'left_view': "",
                'center_view': "",
                'right_view': ""
            }
    
    def analyze_political_bias(self, cluster_info: dict) -> dict:
        """정치 성향 분석 - config 기반"""
        try:
            # 언론사별 기사 수
            media_counts = cluster_info.get('media_counts', {})
            
            # config에서 언론사 성향 매핑 가져오기
            bias_mapping = self.config["media_bias_mapping"]
            left_media = bias_mapping['left']
            center_media = bias_mapping['center']
            right_media = bias_mapping['right']
            
            left_count = sum(media_counts.get(media_id, 0) for media_id in media_counts 
                           if self._get_media_name(media_id) in left_media)
            center_count = sum(media_counts.get(media_id, 0) for media_id in media_counts 
                             if self._get_media_name(media_id) in center_media)
            right_count = sum(media_counts.get(media_id, 0) for media_id in media_counts 
                            if self._get_media_name(media_id) in right_media)
            
            return {
                'left': left_count,
                'center': center_count,
                'right': right_count
            }
            
        except Exception as e:
            console.print(f"❌ 성향 분석 실패: {e}")
            return {'left': 0, 'center': 0, 'right': 0}
    
    @lru_cache(maxsize=1000)
    def _get_media_name(self, media_id: int) -> str:
        """언론사 ID로 이름 조회 (캐싱 적용)"""
        try:
            media = self.media_outlets[self.media_outlets['id'] == media_id]
            return media.iloc[0]['name'] if not media.empty else 'unknown'
        except:
            return 'unknown'
    
    async def save_issues_to_database(self) -> bool:
        """이슈들을 데이터베이스에 저장 - source 기준 랭킹 적용"""
        try:
            console.print("💾 이슈 저장 중...")
            
            # 클러스터를 크기순으로 정렬 (source 기준)
            sorted_clusters = sorted(self.clusters_info, key=lambda x: x['size'], reverse=True)
            
            # source 기준 5위까지만 처리
            max_issues = self.config["max_issues"]
            top_issues_full = self.config["top_issues_full_content"]
            
            clusters_to_process = sorted_clusters[:max_issues]
            console.print(f"📊 처리 대상: {len(clusters_to_process)}개 클러스터 (source 기준 {max_issues}위까지)")
            
            saved_count = 0
            
            for i, cluster_info in enumerate(clusters_to_process):
                is_top_ranked = (i < top_issues_full)  # 5위까지는 전체 내용 생성
                
                if is_top_ranked:
                    # 5위까지: 전체 내용 + 관점 LLM 생성
                    console.print(f"🤖 클러스터 {cluster_info['cluster_id']} (순위 {i+1}) - 전체 내용 + 관점 생성")
                    issue_content = await self.generate_issue_content(cluster_info)
                else:
                    # 6위 이후: 제목, 부제목, 요약만 생성
                    console.print(f"🤖 클러스터 {cluster_info['cluster_id']} (순위 {i+1}) - 제목+부제목만 생성")
                    issue_content = await self.generate_title_and_subtitle(cluster_info)
                
                # 정치 성향 분석
                bias_analysis = self.analyze_political_bias(cluster_info)
                
                # 이슈 데이터 구성
                issue_data = {
                    'title': issue_content['title'],
                    'subtitle': issue_content['subtitle'],
                    'summary': issue_content['summary'],
                    'left_source': str(bias_analysis['left']),
                    'center_source': str(bias_analysis['center']),
                    'right_source': str(bias_analysis['right']),
                    'source': str(cluster_info['size']),
                    'date': datetime.now().date().isoformat()
                }
                
                # 5위까지만 관점 정보 추가
                if is_top_ranked:
                    issue_data.update({
                        'left_view': issue_content.get('left_view', ''),
                        'center_view': issue_content.get('center_view', ''),
                        'right_view': issue_content.get('right_view', '')
                    })
                else:
                    # 6위 이후는 빈 문자열로 설정
                    issue_data.update({
                        'left_view': '',
                        'center_view': '',
                        'right_view': ''
                    })
                
                # 이슈 저장
                issue_result = self.supabase.client.table('issues').insert(issue_data).execute()
                
                if issue_result.data:
                    issue_id = issue_result.data[0]['id']
                    
                    # issue_articles 매핑 저장 (원본 articles의 id 사용)
                    for article in cluster_info['articles']:
                        mapping_data = {
                            'issue_id': issue_id,
                            'article_id': article['article_id'],  # 원본 articles의 id 사용
                            'stance': 'center'
                        }
                        self.supabase.client.table('issue_articles').insert(mapping_data).execute()
                    
                    saved_count += 1
            
            console.print(f"✅ 이슈 저장 완료: {saved_count}개")
            console.print(f"   - 1-{top_issues_full}위: 전체 내용 + 관점 생성")
            console.print(f"   - 모든 이슈: 지지/중립/비판 관점 명확화")
            return True
            
        except Exception as e:
            console.print(f"❌ 이슈 저장 실패: {e}")
            return False