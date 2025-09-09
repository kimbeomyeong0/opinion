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

console = Console()

class IssueGenerator:
    """이슈 생성기 클래스 - 단일 책임: 이슈 생성 및 저장"""
    
    def __init__(self, clusters_info, articles_data, media_outlets):
        """초기화"""
        self.supabase = get_supabase_client()
        self.openai_client = OpenAI()
        self.clusters_info = clusters_info
        self.articles_data = articles_data
        self.media_outlets = media_outlets
    
    async def generate_issue_content(self, cluster_info: dict) -> dict:
        """LLM으로 이슈 내용 생성"""
        try:
            console.print(f"🤖 클러스터 {cluster_info['cluster_id']} 이슈 생성 중...")
            
            # 클러스터의 기사 내용 수집 (최대 5개)
            article_contents = []
            for article in cluster_info['articles'][:5]:
                title = article.get('title_cleaned', '')
                lead = article.get('lead_paragraph', '')
                if title and lead:
                    article_contents.append(f"제목: {title}\n내용: {lead}")
            
            if not article_contents:
                return {
                    'title': f"클러스터 {cluster_info['cluster_id']}",
                    'subtitle': f"{cluster_info['size']}개 기사",
                    'summary': "내용 분석 중 오류가 발생했습니다.",
                    'left_view': "",
                    'center_view': "",
                    'right_view': ""
                }
            
            # LLM 프롬프트
            content_text = "\n\n".join(article_contents)
            prompt = f"""
다음 정치 뉴스들을 분석하여 하나의 이슈로 정리해주세요:

{content_text}

다음 형식으로 응답해주세요:
제목: [간결하고 명확한 이슈 제목]
부제목: [이슈에 대한 간단한 설명]
요약: [이슈의 핵심 내용과 배경을 2-3문장으로 요약]
진보적관점: [진보적 입장에서의 관점과 의견]
중도적관점: [중도적 입장에서의 관점과 의견]
보수적관점: [보수적 입장에서의 관점과 의견]
"""
            
            # OpenAI API 호출
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=800,
                temperature=0.7
            )
            
            content = response.choices[0].message.content.strip()
            
            # 응답 파싱
            lines = content.split('\n')
            title = "정치 이슈"
            subtitle = f"{cluster_info['size']}개 기사"
            summary = content
            left_view = ""
            center_view = ""
            right_view = ""
            
            for line in lines:
                if line.startswith('제목:'):
                    title = line.replace('제목:', '').strip()
                elif line.startswith('부제목:'):
                    subtitle = line.replace('부제목:', '').strip()
                elif line.startswith('요약:'):
                    summary = line.replace('요약:', '').strip()
                elif line.startswith('진보적관점:'):
                    left_view = line.replace('진보적관점:', '').strip()
                elif line.startswith('중도적관점:'):
                    center_view = line.replace('중도적관점:', '').strip()
                elif line.startswith('보수적관점:'):
                    right_view = line.replace('보수적관점:', '').strip()
            
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
    
    async def generate_title_and_subtitle(self, cluster_info: dict) -> dict:
        """제목과 부제목을 LLM으로 생성하고 나머지는 기본값 설정"""
        try:
            console.print(f"🤖 클러스터 {cluster_info['cluster_id']} 제목+부제목 생성 중...")
            
            # 클러스터의 기사 제목들만 수집 (최대 3개)
            article_titles = []
            for article in cluster_info['articles'][:3]:
                title = article.get('title_cleaned', '')
                if title:
                    article_titles.append(title)
            
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
                model="gpt-4o-mini",
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
        """정치 성향 분석"""
        try:
            # 언론사별 기사 수
            media_counts = cluster_info.get('media_counts', {})
            
            # 언론사 성향 매핑 (간단한 분류)
            left_media = ['한겨레', '오마이뉴스']
            center_media = ['연합뉴스', '뉴시스']
            right_media = ['조선일보', '동아일보', '중앙일보', '경향신문', '뉴스원']
            
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
        """이슈들을 데이터베이스에 저장"""
        try:
            console.print("💾 이슈 저장 중...")
            
            # 클러스터를 크기순으로 정렬하여 TOP1 선정
            sorted_clusters = sorted(self.clusters_info, key=lambda x: x['size'], reverse=True)
            
            saved_count = 0
            
            for i, cluster_info in enumerate(sorted_clusters):
                is_top1 = (i == 0)  # 첫 번째가 TOP1
                
                # 모든 클러스터: 전체 내용 + 관점 LLM 생성
                console.print(f"🤖 클러스터 {cluster_info['cluster_id']} - 전체 내용 + 관점 생성")
                issue_content = await self.generate_issue_content(cluster_info)
                
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
                    'left_view': issue_content.get('left_view', ''),
                    'center_view': issue_content.get('center_view', ''),
                    'right_view': issue_content.get('right_view', ''),
                    'source': str(cluster_info['size']),
                    'date': datetime.now().date().isoformat()
                }
                
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
            
            console.print(f"✅ 이슈 저장 완료: {saved_count}개 (TOP1: 1개, 나머지: {saved_count-1}개)")
            return True
            
        except Exception as e:
            console.print(f"❌ 이슈 저장 실패: {e}")
            return False