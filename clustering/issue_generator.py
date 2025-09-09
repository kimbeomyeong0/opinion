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
            
            # 3. 성향별 관점 생성 (articles.content 사용)
            left_view = await self._generate_bias_view(articles_by_bias_with_content['left'], '진보적')
            center_view = await self._generate_bias_view(articles_by_bias_with_content['center'], '중도적')
            right_view = await self._generate_bias_view(articles_by_bias_with_content['right'], '보수적')
            
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
        """기사를 성향별로 분류"""
        articles_by_bias = {
            'all': [],
            'left': [],
            'center': [],
            'right': []
        }
        
        bias_mapping = self.config["media_bias_mapping"]
        
        for article in articles:
            merged_content = article.get('merged_content', '')
            if merged_content and merged_content.strip():
                articles_by_bias['all'].append(merged_content.strip())
                
                # 언론사 성향 확인
                media_id = article.get('media_id', '')
                media_name = self._get_media_name(media_id)
                
                if media_name in bias_mapping['left']:
                    articles_by_bias['left'].append(merged_content.strip())
                elif media_name in bias_mapping['center']:
                    articles_by_bias['center'].append(merged_content.strip())
                elif media_name in bias_mapping['right']:
                    articles_by_bias['right'].append(merged_content.strip())
        
        return articles_by_bias
    
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
        """기사를 성향별로 분류 (view용 - articles.content 사용, 제한 없음)"""
        articles_by_bias = {
            'all': [],
            'left': [],
            'center': [],
            'right': []
        }
        
        bias_mapping = self.config["media_bias_mapping"]
        
        for article in articles:
            # title, subtitle, summary용 (merged_content 사용)
            merged_content = article.get('merged_content', '')
            if merged_content and merged_content.strip():
                articles_by_bias['all'].append(merged_content.strip())
            
            # view용 (articles.content 사용) - 모든 기사 포함
            article_id = article.get('article_id', '')
            content = self._get_article_content(article_id)
            
            if content and content.strip():
                # 언론사 성향 확인
                media_id = article.get('media_id', '')
                media_name = self._get_media_name(media_id)
                
                if media_name in bias_mapping['left']:
                    articles_by_bias['left'].append(content.strip())
                elif media_name in bias_mapping['center']:
                    articles_by_bias['center'].append(content.strip())
                elif media_name in bias_mapping['right']:
                    articles_by_bias['right'].append(content.strip())
        
        return articles_by_bias
    
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
        """요약 생성 (배치 처리)"""
        if not articles:
            return "내용 분석 중 오류가 발생했습니다."
        
        # 배치 크기 설정 (토큰 제한 고려)
        batch_size = 5  # 한 번에 처리할 기사 수 (더 작게)
        batches = [articles[i:i + batch_size] for i in range(0, len(articles), batch_size)]
        
        console.print(f"📝 요약 생성 중... ({len(articles)}개 기사를 {len(batches)}개 배치로 처리)")
        
        batch_summaries = []
        
        for i, batch in enumerate(batches):
            try:
                console.print(f"  배치 {i+1}/{len(batches)} 처리 중... ({len(batch)}개 기사)")
                
                content_text = "\n\n".join(batch)
                prompt = f"""
다음 정치 뉴스들을 분석하여 이슈의 핵심 내용과 배경을 요약해주세요:

{content_text}

요약: [이슈의 핵심 내용과 배경을 요약]
"""
                
                response = self.openai_client.chat.completions.create(
                    model=self.config["openai_model"],
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=500,
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
다음 요약들을 종합하여 하나의 통합된 이슈 요약을 만들어주세요:

{combined_summaries}

통합 요약: [모든 요약을 종합한 최종 이슈 요약]
"""
            
            response = self.openai_client.chat.completions.create(
                model=self.config["openai_model"],
                messages=[{"role": "user", "content": final_prompt}],
                max_tokens=800,
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
        """성향별 관점 생성 (배치 처리)"""
        if not articles:
            return ""
        
        # 배치 크기 설정 (토큰 제한 고려)
        batch_size = 3  # 한 번에 처리할 기사 수 (관점 생성은 더 작게)
        batches = [articles[i:i + batch_size] for i in range(0, len(articles), batch_size)]
        
        console.print(f"📝 {bias_type} 관점 생성 중... ({len(articles)}개 기사를 {len(batches)}개 배치로 처리)")
        
        batch_views = []
        
        for i, batch in enumerate(batches):
            try:
                console.print(f"  {bias_type} 배치 {i+1}/{len(batches)} 처리 중... ({len(batch)}개 기사)")
                
                content_text = "\n\n".join(batch)
                prompt = f"""
다음 {bias_type} 성향의 언론사 기사들을 분석하여 {bias_type} 입장에서의 관점과 의견을 작성해주세요:

{content_text}

{bias_type}관점: [{bias_type} 입장에서의 관점과 의견을 작성]
"""
                
                response = self.openai_client.chat.completions.create(
                    model=self.config["openai_model"],
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=400,
                    temperature=0.7
                )
                
                content = response.choices[0].message.content.strip()
                if content.startswith(f'{bias_type}관점:'):
                    view = content.replace(f'{bias_type}관점:', '').strip()
                else:
                    view = content
                
                batch_views.append(view)
                
                # API 제한 방지를 위한 대기
                await asyncio.sleep(2)  # 대기 시간 증가
                
            except Exception as e:
                console.print(f"❌ {bias_type} 배치 {i+1} 관점 생성 실패: {e}")
                batch_views.append(f"{bias_type} 배치 {i+1} 처리 실패")
        
        # 모든 배치 관점을 종합
        if len(batch_views) == 1:
            return batch_views[0]
        
        try:
            combined_views = "\n\n".join(batch_views)
            final_prompt = f"""
다음 {bias_type} 관점들을 종합하여 하나의 통합된 {bias_type} 관점을 만들어주세요:

{combined_views}

통합 {bias_type}관점: [모든 관점을 종합한 최종 {bias_type} 관점]
"""
            
            response = self.openai_client.chat.completions.create(
                model=self.config["openai_model"],
                messages=[{"role": "user", "content": final_prompt}],
                max_tokens=600,
                temperature=0.7
            )
            
            content = response.choices[0].message.content.strip()
            if content.startswith(f'통합 {bias_type}관점:'):
                return content.replace(f'통합 {bias_type}관점:', '').strip()
            return content
            
        except Exception as e:
            console.print(f"❌ 통합 {bias_type} 관점 생성 실패: {e}")
            return "\n".join(batch_views)
    
    async def generate_title_and_subtitle(self, cluster_info: dict) -> dict:
        """제목과 부제목을 LLM으로 생성하고 나머지는 기본값 설정 - 기사 본문 기반"""
        try:
            console.print(f"🤖 클러스터 {cluster_info['cluster_id']} 제목+부제목 생성 중...")
            
            # 클러스터의 기사 본문에서 제목 부분만 추출 (최대 설정값만큼)
            article_titles = []
            max_titles = self.config["max_titles_for_llm"]
            
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
    
    def _get_media_name(self, media_id: int) -> str:
        """언론사 ID로 이름 조회"""
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
            
            # source 기준 10위까지만 처리
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
            console.print(f"   - 1-{top_issues_full}위: 전체 내용 생성")
            console.print(f"   - {top_issues_full+1}-{saved_count}위: 제목+부제목만 생성")
            return True
            
        except Exception as e:
            console.print(f"❌ 이슈 저장 실패: {e}")
            return False