#!/usr/bin/env python3
"""
정치 뉴스 클러스터링 스크립트
- UMAP + HDBSCAN을 사용한 클러스터링
- LLM을 통한 이슈 제목/요약 생성
- 정치 성향 분석
- 데이터베이스 저장
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import umap
import hdbscan
from datetime import datetime
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn
from rich.panel import Panel
from openai import OpenAI
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor

from utils.supabase_manager import get_supabase_client

console = Console()

class PoliticalNewsClusterer:
    """정치 뉴스 클러스터링 클래스"""
    
    def __init__(self):
        """초기화"""
        self.supabase = get_supabase_client()
        self.openai_client = OpenAI()
        
        # 데이터 저장소
        self.embeddings_data = None
        self.articles_data = None
        self.media_outlets = None
        self.embeddings = None
        self.umap_embedding = None
        self.cluster_labels = None
        self.clusters_info = None
        
        # 성능 최적화를 위한 딕셔너리 매핑
        self.id_to_article = None
        self.id_to_media = None
        
        console.print("✅ Supabase 클라이언트 초기화 완료")
        console.print("✅ OpenAI 클라이언트 초기화 완료 (gpt-4o-mini)")
        
    def load_embeddings_with_pagination(self) -> bool:
        """임베딩 데이터를 페이지네이션으로 로드"""
        try:
            console.print("📊 임베딩 데이터 로드 중...")
            
            all_embeddings = []
            offset = 0
            batch_size = 100
            
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                console=console
            ) as progress:
                
                task = progress.add_task("임베딩 데이터 로드 중...", total=None)
                
                while True:
                    result = self.supabase.client.table('articles_embeddings').select(
                        'cleaned_article_id, embedding_vector, model_name'
                    ).eq('embedding_type', 'combined').range(offset, offset + batch_size - 1).execute()
                    
                    if not result.data:
                        break
                    
                    all_embeddings.extend(result.data)
                    progress.update(task, description=f"임베딩 데이터 로드 중... ({len(all_embeddings)}개)")
                    
                    if len(result.data) < batch_size:
                        break
                    
                    offset += batch_size
            
            if not all_embeddings:
                console.print("❌ 임베딩 데이터가 없습니다.")
                return False
            
            # DataFrame으로 변환
            self.embeddings_data = pd.DataFrame(all_embeddings)
            
            # 임베딩 벡터 추출
            self.embeddings = np.array([eval(emb) for emb in self.embeddings_data['embedding_vector']])
            
            console.print(f"✅ 임베딩 데이터 로드 완료: {len(all_embeddings)}개")
            console.print(f"   - 임베딩 차원: {self.embeddings.shape[1]}")
            
            return True
            
        except Exception as e:
            console.print(f"❌ 임베딩 데이터 로드 실패: {e}")
            return False
    
    def load_articles_data(self) -> bool:
        """기사 메타데이터 로드"""
        try:
            console.print("📰 기사 메타데이터 로드 중...")
            
            # 필요한 컬럼만 조회
            embedding_ids = self.embeddings_data['cleaned_article_id'].tolist()
            
            # 배치 크기를 작게 설정하여 URL 길이 제한 회피
            batch_size = 100
            all_articles = []
            
            for i in range(0, len(embedding_ids), batch_size):
                batch_ids = embedding_ids[i:i + batch_size]
                
                result = self.supabase.client.table('articles_cleaned').select(
                    'id, article_id, merged_content'
                ).in_('id', batch_ids).execute()
                
                if result.data:
                    all_articles.extend(result.data)
            
            if not all_articles:
                console.print("❌ 기사 메타데이터가 없습니다.")
                return False
            
            self.articles_data = pd.DataFrame(all_articles)
            console.print(f"✅ 기사 메타데이터 로드 완료: {len(all_articles)}개")
            
            return True
            
        except Exception as e:
            console.print(f"❌ 기사 메타데이터 로드 실패: {e}")
            return False
    
    def load_media_outlets(self) -> bool:
        """언론사 정보 로드"""
        try:
            console.print("📺 언론사 정보 로드 중...")
            
            result = self.supabase.client.table('media_outlets').select('id, name, bias').execute()
            
            if not result.data:
                console.print("❌ 언론사 정보가 없습니다.")
                return False
            
            self.media_outlets = pd.DataFrame(result.data)
            console.print(f"✅ 언론사 정보 로드 완료: {len(result.data)}개")
            
            return True
            
        except Exception as e:
            console.print(f"❌ 언론사 정보 로드 실패: {e}")
            return False
    
    def create_performance_mappings(self) -> bool:
        """성능 최적화를 위한 딕셔너리 매핑 생성"""
        try:
            console.print("⚡ 성능 최적화 매핑 생성 중...")
            
            # 1. embedding_id -> article_id 매핑
            self.id_to_article = self.articles_data.set_index('id')['article_id'].to_dict()
            
            # 2. article_id -> media_id 매핑 (사전 로드) - 배치 처리로 URL 길이 제한 회피
            article_ids = list(self.id_to_article.values())
            batch_size = 100  # URL 길이 제한 회피
            media_results_data = []
            
            for i in range(0, len(article_ids), batch_size):
                batch_ids = article_ids[i:i + batch_size]
                media_results = self.supabase.client.table('articles').select(
                    'id, media_id'
                ).in_('id', batch_ids).execute()
                
                if media_results.data:
                    media_results_data.extend(media_results.data)
            
            media_results = type('obj', (object,), {'data': media_results_data})()
            
            if media_results.data:
                self.id_to_media = {row['id']: row['media_id'] for row in media_results.data}
            else:
                self.id_to_media = {}
            
            console.print(f"✅ 성능 최적화 매핑 완료:")
            console.print(f"   - embedding_id -> article_id: {len(self.id_to_article)}개")
            console.print(f"   - article_id -> media_id: {len(self.id_to_media)}개")
            
            return True
            
        except Exception as e:
            console.print(f"❌ 성능 최적화 매핑 생성 실패: {e}")
            return False
    
    def optimize_clustering_parameters(self) -> tuple:
        """클러스터링 파라미터 최적화"""
        n_samples = len(self.embeddings)
        
        # UMAP 파라미터 조정
        if n_samples < 100:
            n_neighbors = min(5, n_samples - 1)
            min_dist = 0.1
        elif n_samples < 500:
            n_neighbors = min(10, n_samples // 10)
            min_dist = 0.2
        elif n_samples < 1000:
            n_neighbors = 25
            min_dist = 0.1
        else:
            n_neighbors = 30
            min_dist = 0.1
        
        # HDBSCAN 파라미터 조정
        min_cluster_size = max(3, n_samples // 200)  # 전체의 0.5%
        min_samples = max(2, min_cluster_size // 2)
        
        return (n_neighbors, min_dist, min_cluster_size, min_samples)
    
    def run_umap_reduction(self) -> bool:
        """UMAP 차원 축소"""
        try:
            console.print("🔄 UMAP 차원 축소 실행 중...")
            
            n_neighbors, min_dist, _, _ = self.optimize_clustering_parameters()
            
            reducer = umap.UMAP(
                n_components=2,
                n_neighbors=n_neighbors,
                min_dist=min_dist,
                random_state=42,
                verbose=True,
                n_jobs=-1
            )
            
            self.umap_embedding = reducer.fit_transform(self.embeddings)
            
            console.print(f"✅ UMAP 완료: {self.embeddings.shape[1]}D → 2D")
            console.print(f"   - 파라미터: n_neighbors={n_neighbors}, min_dist={min_dist}")
            
            return True
            
        except Exception as e:
            console.print(f"❌ UMAP 실행 실패: {e}")
            return False
    
    def run_hdbscan_clustering(self) -> bool:
        """HDBSCAN 클러스터링"""
        try:
            console.print("🔄 HDBSCAN 클러스터링 실행 중...")
            
            _, _, min_cluster_size, min_samples = self.optimize_clustering_parameters()
            
            clusterer = hdbscan.HDBSCAN(
                min_cluster_size=min_cluster_size,
                min_samples=min_samples,
                metric='euclidean',
                cluster_selection_epsilon=0.1
            )
            
            self.cluster_labels = clusterer.fit_predict(self.umap_embedding)
            
            # 클러스터 통계
            unique_labels = np.unique(self.cluster_labels)
            n_clusters = len(unique_labels) - (1 if -1 in unique_labels else 0)
            n_noise = np.sum(self.cluster_labels == -1)
            
            console.print(f"✅ HDBSCAN 완료:")
            console.print(f"   - 클러스터 수: {n_clusters}개")
            console.print(f"   - 노이즈 기사: {n_noise}개")
            console.print(f"   - 파라미터: min_cluster_size={min_cluster_size}, min_samples={min_samples}")
            
            return True
            
        except Exception as e:
            console.print(f"❌ HDBSCAN 실행 실패: {e}")
            return False
    
    def analyze_clusters(self) -> bool:
        """클러스터 분석"""
        try:
            console.print("🔍 클러스터 분석 중...")
            
            # 클러스터별 기사 그룹화
            clusters_info = []
            
            for cluster_id in np.unique(self.cluster_labels):
                if cluster_id == -1:  # 노이즈 제외
                    continue
                
                cluster_mask = self.cluster_labels == cluster_id
                cluster_embedding_ids = self.embeddings_data[cluster_mask]['cleaned_article_id'].tolist()
                
                clusters_info.append({
                    'cluster_id': cluster_id,
                    'size': len(cluster_embedding_ids),
                    'embedding_ids': cluster_embedding_ids
                })
            
            # 크기순 정렬
            clusters_info.sort(key=lambda x: x['size'], reverse=True)
            self.clusters_info = clusters_info
            
            # 결과 표시
            from rich.table import Table
            table = Table(title="클러스터 분석 결과")
            table.add_column("클러스터 ID", style="cyan")
            table.add_column("크기", style="magenta")
            table.add_column("비율", style="green")
            
            total_articles = sum(cluster['size'] for cluster in clusters_info)
            
            for cluster in clusters_info[:20]:  # 상위 20개만 표시
                percentage = (cluster['size'] / total_articles) * 100
                table.add_row(
                    str(cluster['cluster_id']),
                    str(cluster['size']),
                    f"{percentage:.1f}%"
                )
            
            console.print(table)
            console.print(f"✅ 분석 완료: {len(clusters_info)}개 클러스터")
            
            return True
            
        except Exception as e:
            console.print(f"❌ 클러스터 분석 실패: {e}")
            return False
    
    def generate_issue_content_with_llm(self, cluster_info: dict) -> dict:
        """LLM으로 이슈 내용 생성"""
        try:
            console.print(f"🤖 클러스터 {cluster_info['cluster_id']} LLM 내용 생성 중...")
            
            # 클러스터의 기사 내용 수집 (원본 articles 테이블에서) - 최적화된 배치 쿼리
            article_contents = []
            
            # 1단계: embedding_id에서 article_id로 매핑 (최적화된 딕셔너리 조회)
            embedding_ids = cluster_info['embedding_ids'][:5]  # 최대 5개 기사
            article_ids = []
            
            for embedding_id in embedding_ids:
                article_id = self.id_to_article.get(embedding_id)
                if article_id:
                    article_ids.append(article_id)
            
            # 2단계: 원본 articles 테이블에서 배치로 전체 내용 가져오기 (단일 쿼리!)
            if article_ids:
                original_articles = self.supabase.client.table('articles').select(
                    'id, title, content'
                ).in_('id', article_ids).execute()
                
                if original_articles.data:
                    # 3단계: 결과 처리
                    for article_info in original_articles.data:
                        full_content = f"제목: {article_info['title']}\n내용: {article_info['content']}"
                        if len(full_content.strip()) > 50:
                            article_contents.append(full_content[:1000])  # 1000자로 확장
            
            if not article_contents:
                return {
                    'title': f"클러스터 {cluster_info['cluster_id']}",
                    'subtitle': f"{cluster_info['size']}개 기사",
                    'summary': "내용 분석 중 오류가 발생했습니다."
                }
            
            # 전문적이고 세련된 기사 스타일 프롬프트
            prompt = f"""
당신은 대한민국 주요 언론사의 베테랑 기자입니다. 아래 가이드라인에 따라 전문적이고 세련된 기사 스타일로 제목, 부제목, 요약을 생성해주세요.

## 제목 개선 가이드라인
❌ 피해야 할 표현:
- 과도한 감탄부호 (!, ?)
- "급기야", "결국", "무려" 등 선정적 수식어 남발
- "폭증", "폭발" 등 과장된 표현

✅ 지향해야 할 표현:
- 간결하고 정확한 팩트 중심
- 구체적 수치나 기관명 활용
- 중립적이면서도 임팩트 있는 표현
- 12-15자 내외 권장

## 부제목 가이드라인
- 제목에서 다루지 못한 핵심 정보 보완
- 구체적 배경이나 추가 쟁점 제시
- 20자 내외로 간결하게

## 요약문 개선 가이드라인
❌ 개선 필요한 부분:
- "무려", "기승을 부리며" 등 과도한 수사법
- 단순한 사실 나열
- 모호한 전망 ("계속될 전망", "시급한 상황")

✅ 개선 방향:
- 객관적이고 구체적인 사실 기반 서술
- 핵심 이슈의 배경과 현재 상황을 논리적으로 연결
- 구체적 수치, 날짜, 기관명 활용
- 각 당사자의 입장을 균형있게 제시
- 명확한 후속 일정이나 절차 언급
- 60-80자 내외 권장

## 문체 개선 포인트
1. **중립성 유지**: 특정 정파에 치우치지 않는 균형잡힌 시각
2. **정확성 우선**: 추측이나 감정적 표현보다 확인된 사실 중심
3. **간결성**: 불필요한 수식어 제거, 핵심 내용만 간추림
4. **전문성**: 해당 분야 전문용어를 적절히 활용

## 참고할 기사 스타일
- 연합뉴스: 정확하고 간결한 팩트 중심
- 한국경제: 경제 이슈의 파급효과 명확히 제시
- 조선일보: 임팩트 있으면서도 품격있는 표현

기사 내용들:
{chr(10).join(article_contents)}

반드시 다음 형식으로만 응답하세요:
제목: [전문적이고 세련된 제목]
부제목: [핵심 정보 보완]
요약: [객관적이고 구체적인 요약]

주의사항:
- 제목, 부제목, 요약 앞에 다른 텍스트나 기호를 붙이지 마세요
- 각 항목은 반드시 "제목:", "부제목:", "요약:"으로 시작하세요
- 정치적 중립성을 유지하고 전문적인 기사 스타일로 작성하세요
"""
            
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "당신은 대한민국 주요 언론사의 베테랑 기자입니다. 전문적이고 세련된 기사 스타일로 작성하며, 객관적이고 정확한 팩트 중심의 뉴스를 만들어주세요. 선정적 표현을 피하고 중립적이면서도 임팩트 있는 표현을 사용하세요."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=500,
                temperature=0.7
            )
            
            content = response.choices[0].message.content.strip()
            
            # 응답 파싱 (개선된 파싱)
            lines = content.split('\n')
            title = f"클러스터 {cluster_info['cluster_id']} 이슈"
            subtitle = f"{cluster_info['size']}개 기사 분석"
            summary = "정치 이슈 분석 결과입니다."
            
            for line in lines:
                line = line.strip()
                if line.startswith('제목:'):
                    title = line.replace('제목:', '').strip()
                    # 불필요한 기호 제거
                    title = title.replace('**', '').replace('*', '').strip()
                elif line.startswith('부제목:'):
                    subtitle = line.replace('부제목:', '').strip()
                    # 불필요한 기호 제거
                    subtitle = subtitle.replace('**', '').replace('*', '').strip()
                elif line.startswith('요약:'):
                    summary = line.replace('요약:', '').strip()
                    # 불필요한 기호 제거
                    summary = summary.replace('**', '').replace('*', '').strip()
            
            # 기본값 방지 및 품질 검증
            if title == f"클러스터 {cluster_info['cluster_id']} 이슈" and len(content) > 50:
                # 첫 번째 줄을 제목으로 사용 (기호 제거)
                first_line = content.split('\n')[0].strip()
                if len(first_line) > 10 and len(first_line) < 100:
                    title = first_line.replace('**', '').replace('*', '').strip()
            
            # 품질 검증 및 개선
            if len(title) < 5:
                title = f"정치 이슈 {cluster_info['cluster_id']}"
            if len(subtitle) < 5:
                subtitle = f"{cluster_info['size']}개 기사 관련 이슈"
            if len(summary) < 20:
                summary = f"클러스터 {cluster_info['cluster_id']}에 포함된 {cluster_info['size']}개 기사를 분석한 결과, 중요한 정치 이슈가 발견되었습니다."
            
            return {
                'title': title,
                'subtitle': subtitle,
                'summary': summary
            }
            
        except Exception as e:
            console.print(f"❌ LLM 내용 생성 실패: {e}")
            return {
                'title': f"클러스터 {cluster_info['cluster_id']}",
                'subtitle': f"{cluster_info['size']}개 기사",
                'summary': "내용 분석 중 오류가 발생했습니다."
            }
    
    async def generate_issue_content_with_llm_async(self, cluster_info: dict) -> dict:
        """비동기 LLM으로 이슈 내용 생성"""
        try:
            console.print(f"🤖 클러스터 {cluster_info['cluster_id']} LLM 내용 생성 중...")
            
            # 클러스터의 기사 내용 수집 (원본 articles 테이블에서) - 최적화된 배치 쿼리
            article_contents = []
            
            # 1단계: embedding_id에서 article_id로 매핑 (최적화된 딕셔너리 조회)
            embedding_ids = cluster_info['embedding_ids'][:5]  # 최대 5개 기사
            article_ids = []
            
            for embedding_id in embedding_ids:
                article_id = self.id_to_article.get(embedding_id)
                if article_id:
                    article_ids.append(article_id)
            
            # 2단계: 원본 articles 테이블에서 배치로 전체 내용 가져오기 (단일 쿼리!)
            if article_ids:
                original_articles = self.supabase.client.table('articles').select(
                    'id, title, content'
                ).in_('id', article_ids).execute()
                
                if original_articles.data:
                    # 3단계: 결과 처리
                    for article_info in original_articles.data:
                        full_content = f"제목: {article_info['title']}\n내용: {article_info['content']}"
                        if len(full_content.strip()) > 50:
                            article_contents.append(full_content[:1000])  # 1000자로 확장
            
            if not article_contents:
                return {
                    'title': f"클러스터 {cluster_info['cluster_id']}",
                    'subtitle': f"{cluster_info['size']}개 기사",
                    'summary': "내용 분석 중 오류가 발생했습니다."
                }
            
            # 전문적이고 세련된 기사 스타일 프롬프트
            prompt = f"""
당신은 대한민국 주요 언론사의 베테랑 기자입니다. 아래 가이드라인에 따라 전문적이고 세련된 기사 스타일로 제목, 부제목, 요약을 생성해주세요.

## 제목 개선 가이드라인
❌ 피해야 할 표현:
- 과도한 감탄부호 (!, ?)
- "급기야", "결국", "무려" 등 선정적 수식어 남발
- "폭증", "폭발" 등 과장된 표현

✅ 지향해야 할 표현:
- 간결하고 정확한 팩트 중심
- 구체적 수치나 기관명 활용
- 중립적이면서도 임팩트 있는 표현
- 12-15자 내외 권장

## 부제목 가이드라인
- 제목에서 다루지 못한 핵심 정보 보완
- 구체적 배경이나 추가 쟁점 제시
- 20자 내외로 간결하게

## 요약문 개선 가이드라인
❌ 개선 필요한 부분:
- "무려", "기승을 부리며" 등 과도한 수사법
- 단순한 사실 나열
- 모호한 전망 ("계속될 전망", "시급한 상황")

✅ 개선 방향:
- 객관적이고 구체적인 사실 기반 서술
- 핵심 이슈의 배경과 현재 상황을 논리적으로 연결
- 구체적 수치, 날짜, 기관명 활용
- 각 당사자의 입장을 균형있게 제시
- 명확한 후속 일정이나 절차 언급
- 60-80자 내외 권장

## 문체 개선 포인트
1. **중립성 유지**: 특정 정파에 치우치지 않는 균형잡힌 시각
2. **정확성 우선**: 추측이나 감정적 표현보다 확인된 사실 중심
3. **간결성**: 불필요한 수식어 제거, 핵심 내용만 간추림
4. **전문성**: 해당 분야 전문용어를 적절히 활용

## 참고할 기사 스타일
- 연합뉴스: 정확하고 간결한 팩트 중심
- 한국경제: 경제 이슈의 파급효과 명확히 제시
- 조선일보: 임팩트 있으면서도 품격있는 표현

기사 내용들:
{chr(10).join(article_contents)}

반드시 다음 형식으로만 응답하세요:
제목: [전문적이고 세련된 제목]
부제목: [핵심 정보 보완]
요약: [객관적이고 구체적인 요약]

주의사항:
- 제목, 부제목, 요약 앞에 다른 텍스트나 기호를 붙이지 마세요
- 각 항목은 반드시 "제목:", "부제목:", "요약:"으로 시작하세요
- 정치적 중립성을 유지하고 전문적인 기사 스타일로 작성하세요
"""
            
            # 비동기 LLM 호출
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "당신은 대한민국 주요 언론사의 베테랑 기자입니다. 전문적이고 세련된 기사 스타일로 작성하며, 객관적이고 정확한 팩트 중심의 뉴스를 만들어주세요. 선정적 표현을 피하고 중립적이면서도 임팩트 있는 표현을 사용하세요."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=500,
                    temperature=0.7
                )
            )
            
            content = response.choices[0].message.content.strip()
            
            # 응답 파싱 (개선된 파싱)
            lines = content.split('\n')
            title = f"클러스터 {cluster_info['cluster_id']} 이슈"
            subtitle = f"{cluster_info['size']}개 기사 분석"
            summary = "정치 이슈 분석 결과입니다."
            
            for line in lines:
                line = line.strip()
                if line.startswith('제목:'):
                    title = line.replace('제목:', '').strip()
                    # 불필요한 기호 제거
                    title = title.replace('**', '').replace('*', '').strip()
                elif line.startswith('부제목:'):
                    subtitle = line.replace('부제목:', '').strip()
                    # 불필요한 기호 제거
                    subtitle = subtitle.replace('**', '').replace('*', '').strip()
                elif line.startswith('요약:'):
                    summary = line.replace('요약:', '').strip()
                    # 불필요한 기호 제거
                    summary = summary.replace('**', '').replace('*', '').strip()
            
            # 기본값 방지 및 품질 검증
            if title == f"클러스터 {cluster_info['cluster_id']} 이슈" and len(content) > 50:
                # 첫 번째 줄을 제목으로 사용 (기호 제거)
                first_line = content.split('\n')[0].strip()
                if len(first_line) > 10 and len(first_line) < 100:
                    title = first_line.replace('**', '').replace('*', '').strip()
            
            # 품질 검증 및 개선
            if len(title) < 5:
                title = f"정치 이슈 {cluster_info['cluster_id']}"
            if len(subtitle) < 5:
                subtitle = f"{cluster_info['size']}개 기사 관련 이슈"
            if len(summary) < 20:
                summary = f"클러스터 {cluster_info['cluster_id']}에 포함된 {cluster_info['size']}개 기사를 분석한 결과, 중요한 정치 이슈가 발견되었습니다."
            
            return {
                'title': title,
                'subtitle': subtitle,
                'summary': summary
            }
            
        except Exception as e:
            console.print(f"❌ LLM 내용 생성 실패: {e}")
            return {
                'title': f"클러스터 {cluster_info['cluster_id']}",
                'subtitle': f"{cluster_info['size']}개 기사",
                'summary': "내용 분석 중 오류가 발생했습니다."
            }
    
    def analyze_political_bias(self, cluster_info: dict) -> dict:
        """정치 성향 분석 - 최적화된 배치 처리"""
        try:
            bias_counts = {'left': 0, 'center': 0, 'right': 0}
            
            # 최적화된 딕셔너리 조회 방식
            for embedding_id in cluster_info['embedding_ids']:
                # 1단계: embedding_id -> article_id (O(1) 조회)
                article_id = self.id_to_article.get(embedding_id)
                if not article_id:
                    continue
                
                # 2단계: article_id -> media_id (O(1) 조회)
                media_id = self.id_to_media.get(article_id)
                if not media_id:
                    continue
                
                # 3단계: 언론사 성향 분석 (O(1) 조회)
                outlet_data = self.media_outlets[self.media_outlets['id'] == media_id]
                if not outlet_data.empty:
                    bias = outlet_data.iloc[0]['bias']
                    if bias in bias_counts:
                        bias_counts[bias] += 1
            
            return bias_counts
            
        except Exception as e:
            console.print(f"❌ 정치 성향 분석 실패: {e}")
            return {'left': 0, 'center': 0, 'right': 0}
    
    def save_clusters_to_database(self) -> bool:
        """클러스터 결과를 데이터베이스에 저장 (LLM 비용 절약을 위해 크기 순 TOP3만 선택)"""
        try:
            console.print("💾 데이터베이스 저장 중...")
            
            # LLM 비용 절약을 위해 클러스터 중 크기 순으로 TOP3만 선택
            if len(self.clusters_info) > 3:
                selected_clusters = self.clusters_info[:3]  # 이미 크기순으로 정렬되어 있음
                console.print(f"🏆 LLM 비용 절약: {len(self.clusters_info)}개 중 TOP3 선택")
            else:
                selected_clusters = self.clusters_info
                console.print(f"📝 모든 클러스터 ({len(self.clusters_info)}개) 처리")
            
            saved_count = 0
            
            for cluster_info in selected_clusters:
                # LLM으로 이슈 내용 생성
                llm_content = self.generate_issue_content_with_llm(cluster_info)
                
                # 정치 성향 분석
                bias_analysis = self.analyze_political_bias(cluster_info)
                
                # 이슈 데이터 구성
                issue_data = {
                    'title': llm_content['title'],
                    'subtitle': llm_content['subtitle'],
                    'summary': llm_content['summary'],
                    'left_view': str(bias_analysis['left']),
                    'center_view': str(bias_analysis['center']),
                    'right_view': str(bias_analysis['right']),
                    'source': str(cluster_info['size']),
                    'date': datetime.now().date().isoformat()
                }
                
                # 이슈 저장
                issue_result = self.supabase.client.table('issues').insert(issue_data).execute()
                
                if issue_result.data:
                    issue_id = issue_result.data[0]['id']
                    
                    # issue_articles 매핑 저장 (최적화된 딕셔너리 조회)
                    for embedding_id in cluster_info['embedding_ids']:
                        # embedding_id -> article_id (O(1) 조회)
                        article_id = self.id_to_article.get(embedding_id)
                        if article_id:
                            # issue_articles 테이블에 매핑 저장
                            mapping_data = {
                                'issue_id': issue_id,
                                'article_id': article_id,
                                'stance': 'center'  # 기본값 (neutral 대신 center 사용)
                            }
                            
                            self.supabase.client.table('issue_articles').insert(mapping_data).execute()
                    
                    saved_count += 1
                    console.print(f"✅ 클러스터 {cluster_info['cluster_id']} 저장 완료")
            
            console.print(f"✅ 총 {saved_count}개 이슈 저장 완료")
            return True
            
        except Exception as e:
            console.print(f"❌ 데이터베이스 저장 실패: {e}")
            return False
    
    async def save_clusters_to_database_async(self) -> bool:
        """비동기 클러스터 결과를 데이터베이스에 저장 (LLM 비용 절약을 위해 크기 순 TOP3만 선택)"""
        try:
            console.print("💾 데이터베이스 저장 중...")
            
            # LLM 비용 절약을 위해 클러스터 중 크기 순으로 TOP3만 선택
            if len(self.clusters_info) > 3:
                selected_clusters = self.clusters_info[:3]  # 이미 크기순으로 정렬되어 있음
                console.print(f"🏆 LLM 비용 절약: {len(self.clusters_info)}개 중 TOP3 선택")
            else:
                selected_clusters = self.clusters_info
                console.print(f"📝 모든 클러스터 ({len(self.clusters_info)}개) 처리")
            
            # 비동기 LLM 호출들을 병렬로 실행
            llm_tasks = []
            for cluster_info in selected_clusters:
                task = self.generate_issue_content_with_llm_async(cluster_info)
                llm_tasks.append(task)
            
            # 모든 LLM 호출을 병렬로 실행
            llm_results = await asyncio.gather(*llm_tasks)
            
            saved_count = 0
            
            for i, cluster_info in enumerate(selected_clusters):
                # LLM 결과 사용
                llm_content = llm_results[i]
                
                # 정치 성향 분석
                bias_analysis = self.analyze_political_bias(cluster_info)
                
                # 이슈 데이터 구성
                issue_data = {
                    'title': llm_content['title'],
                    'subtitle': llm_content['subtitle'],
                    'summary': llm_content['summary'],
                    'left_view': str(bias_analysis['left']),
                    'center_view': str(bias_analysis['center']),
                    'right_view': str(bias_analysis['right']),
                    'source': str(cluster_info['size']),
                    'date': datetime.now().date().isoformat()
                }
                
                # 이슈 저장
                issue_result = self.supabase.client.table('issues').insert(issue_data).execute()
                
                if issue_result.data:
                    issue_id = issue_result.data[0]['id']
                    
                    # issue_articles 매핑 저장 (최적화된 딕셔너리 조회)
                    for embedding_id in cluster_info['embedding_ids']:
                        # embedding_id -> article_id (O(1) 조회)
                        article_id = self.id_to_article.get(embedding_id)
                        if article_id:
                            # issue_articles 테이블에 매핑 저장
                            mapping_data = {
                                'issue_id': issue_id,
                                'article_id': article_id,
                                'stance': 'center'  # 기본값 (neutral 대신 center 사용)
                            }
                            
                            self.supabase.client.table('issue_articles').insert(mapping_data).execute()
                    
                    saved_count += 1
                    console.print(f"✅ 클러스터 {cluster_info['cluster_id']} 저장 완료")
            
            console.print(f"✅ 총 {saved_count}개 이슈 저장 완료")
            return True
            
        except Exception as e:
            console.print(f"❌ 데이터베이스 저장 실패: {e}")
            return False
    
    async def run_full_clustering(self) -> bool:
        """전체 클러스터링 파이프라인 실행"""
        try:
            console.print(Panel(
                "[bold blue]🚀 정치 뉴스 클러스터링 시작[/bold blue]\n"
                "[yellow]UMAP + HDBSCAN + LLM[/yellow]",
                title="클러스터링 파이프라인"
            ))
            
            # 1. 데이터 로드
            if not self.load_embeddings_with_pagination():
                return False
            
            if not self.load_articles_data():
                return False
            
            if not self.load_media_outlets():
                return False
            
            # 1.5. 성능 최적화 매핑 생성
            if not self.create_performance_mappings():
                return False
            
            # 2. UMAP 차원 축소
            if not self.run_umap_reduction():
                return False
            
            # 3. HDBSCAN 클러스터링
            if not self.run_hdbscan_clustering():
                    return False
            
            # 4. 클러스터 분석
            if not self.analyze_clusters():
                return False
            
            # 5. 데이터베이스 저장 (비동기)
            if not await self.save_clusters_to_database_async():
                return False
            
            console.print(Panel(
                f"[bold green]✅ 클러스터링 완료! 총 {len(self.clusters_info)}개 이슈 생성[/bold green]",
                title="완료"
            ))
            
            return True
            
        except Exception as e:
            console.print(f"❌ 클러스터링 파이프라인 실패: {e}")
            return False

async def main():
    """메인 함수"""
    try:
        clusterer = PoliticalNewsClusterer()
        await clusterer.run_full_clustering()
    except Exception as e:
        console.print(f"❌ 실행 실패: {e}")

def run_main():
    """비동기 메인 함수 실행"""
    asyncio.run(main())

if __name__ == "__main__":
    run_main()
