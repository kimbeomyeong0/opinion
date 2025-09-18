#!/usr/bin/env python3
"""
전체 정치 카테고리 기사 클러스터링 스크립트
- 8개 정치 카테고리별 독립 클러스터링 수행
- LLM 기반 동적 사건 패턴 생성
- UMAP 차원축소 + HDBSCAN 클러스터링
- 카테고리별 상위 3개 클러스터를 issues 테이블에 저장 (20개 기사 이상만)
- 하이브리드 처리: 대용량 순차, 소량 병렬
"""

import sys
import os
import json
import numpy as np
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils.supabase_manager import SupabaseManager
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

console = Console()

# 필요한 라이브러리 import
try:
    import umap
    import hdbscan
    from openai import OpenAI
except ImportError as e:
    console.print(f"❌ 필요한 라이브러리가 설치되지 않았습니다: {e}")
    console.print("다음 명령어로 설치해주세요:")
    console.print("pip install umap-learn hdbscan scikit-learn openai")
    sys.exit(1)


class MultiCategoryClusterer:
    """전체 정치 카테고리 클러스터링 클래스"""
    
    def __init__(self):
        """초기화"""
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
        
        # OpenAI 클라이언트 초기화
        try:
            self.openai_client = OpenAI()
            console.print("✅ OpenAI 클라이언트 초기화 완료")
        except Exception as e:
            console.print(f"❌ OpenAI 클라이언트 초기화 실패: {str(e)}")
            raise Exception("OpenAI 연결 실패")
        
        # 정치 카테고리 정의
        self.categories = {
            # 대용량 카테고리 (순차 처리)
            "large": ["행정부", "사법/검찰", "기타", "국회/정당", "외교/안보"],
            # 소량 카테고리 (병렬 처리)  
            "small": ["정책/경제사회", "선거", "지역정치"]
        }
        
        console.print("✅ MultiCategoryClusterer 초기화 완료")
        console.print(f"📊 대용량 카테고리: {len(self.categories['large'])}개")
        console.print(f"📊 소량 카테고리: {len(self.categories['small'])}개")
    
    def fetch_articles_by_category(self, category: str) -> List[Dict[str, Any]]:
        """특정 카테고리 기사들과 임베딩 조회"""
        try:
            console.print(f"🔍 {category} 카테고리 기사 조회 중...")
            
            all_articles = []
            page_size = 1000
            offset = 0
            
            while True:
                result = self.supabase_manager.client.table('articles').select(
                    'id, title, media_id, political_category, embedding, published_at'
                ).eq('political_category', category).not_.is_('embedding', 'null').range(
                    offset, offset + page_size - 1
                ).execute()
                
                if not result.data:
                    break
                    
                all_articles.extend(result.data)
                
                if len(result.data) < page_size:
                    break
                    
                offset += page_size
                console.print(f"📄 페이지 조회 중... {len(all_articles)}개 수집됨")
            
            console.print(f"✅ {category} 카테고리 기사 {len(all_articles)}개 조회 완료")
            return all_articles
            
        except Exception as e:
            console.print(f"❌ {category} 기사 조회 실패: {str(e)}")
            return []
    
    def extract_embeddings(self, articles: List[Dict[str, Any]]) -> Tuple[np.ndarray, List[Dict[str, Any]]]:
        """임베딩 벡터 추출 및 유효한 기사 필터링"""
        try:
            console.print("🔄 임베딩 벡터 추출 중...")
            
            valid_articles = []
            embeddings = []
            
            for article in articles:
                try:
                    embedding_json = article.get('embedding')
                    if embedding_json:
                        embedding_vector = json.loads(embedding_json)
                        if isinstance(embedding_vector, list) and len(embedding_vector) > 0:
                            embeddings.append(embedding_vector)
                            valid_articles.append(article)
                except Exception as e:
                    console.print(f"⚠️ 임베딩 파싱 실패: {article.get('id', 'Unknown')} - {str(e)}")
                    continue
            
            embeddings_array = np.array(embeddings)
            console.print(f"✅ 유효한 임베딩 {len(valid_articles)}개 추출 완료")
            console.print(f"📊 임베딩 차원: {embeddings_array.shape}")
            
            return embeddings_array, valid_articles
            
        except Exception as e:
            console.print(f"❌ 임베딩 추출 실패: {str(e)}")
            return np.array([]), []
    
    def perform_umap_reduction(self, embeddings: np.ndarray) -> np.ndarray:
        """UMAP 차원축소 수행"""
        try:
            console.print("🔄 UMAP 차원축소 수행 중...")
            console.print(f"⚙️ 설정: n_neighbors=30, n_components=15, min_dist=0.1, metric=cosine")
            
            # UMAP 파라미터 설정
            umap_reducer = umap.UMAP(
                n_neighbors=30,
                n_components=15,
                min_dist=0.1,
                metric='cosine',
                random_state=42,
                n_jobs=1  # 안정성을 위해 단일 스레드 사용
            )
            
            # 차원축소 수행
            reduced_embeddings = umap_reducer.fit_transform(embeddings)
            
            console.print(f"✅ UMAP 차원축소 완료: {embeddings.shape} → {reduced_embeddings.shape}")
            return reduced_embeddings
            
        except Exception as e:
            console.print(f"❌ UMAP 차원축소 실패: {str(e)}")
            return np.array([])
    
    def perform_hdbscan_clustering(self, reduced_embeddings: np.ndarray) -> np.ndarray:
        """HDBSCAN 클러스터링 수행"""
        try:
            console.print("🔄 HDBSCAN 클러스터링 수행 중...")
            console.print(f"⚙️ 설정: min_cluster_size=15, min_samples=6, cluster_selection_method=eom")
            
            # HDBSCAN 파라미터 설정
            clusterer = hdbscan.HDBSCAN(
                min_cluster_size=15,
                min_samples=6,
                cluster_selection_method='eom',
                metric='euclidean'
            )
            
            # 클러스터링 수행
            cluster_labels = clusterer.fit_predict(reduced_embeddings)
            
            # 클러스터 통계
            unique_labels = np.unique(cluster_labels)
            n_clusters = len(unique_labels) - (1 if -1 in unique_labels else 0)  # -1은 노이즈
            n_noise = np.sum(cluster_labels == -1)
            
            console.print(f"✅ HDBSCAN 클러스터링 완료")
            console.print(f"📊 클러스터 수: {n_clusters}개")
            console.print(f"📊 노이즈 포인트: {n_noise}개")
            
            # 클러스터별 크기 출력
            for label in unique_labels:
                if label != -1:  # 노이즈 제외
                    cluster_size = np.sum(cluster_labels == label)
                    console.print(f"   클러스터 {label}: {cluster_size}개 기사")
            
            return cluster_labels
            
        except Exception as e:
            console.print(f"❌ HDBSCAN 클러스터링 실패: {str(e)}")
            return np.array([])
    
    def get_media_bias_mapping(self) -> Dict[str, str]:
        """언론사 ID별 bias 매핑 조회"""
        try:
            result = self.supabase_manager.client.table('media_outlets').select('id, bias').execute()
            
            bias_mapping = {}
            for outlet in result.data:
                bias_mapping[outlet['id']] = outlet['bias']
            
            console.print(f"✅ 언론사 bias 매핑 {len(bias_mapping)}개 조회 완료")
            return bias_mapping
            
        except Exception as e:
            console.print(f"❌ 언론사 bias 매핑 조회 실패: {str(e)}")
            return {}
    
    def generate_dynamic_patterns_with_llm(self, cluster_articles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """LLM을 활용한 동적 사건 패턴 생성"""
        try:
            console.print(f"🤖 LLM으로 {len(cluster_articles)}개 기사의 사건 패턴 분석 중...")
            
            # 기사 제목들 수집 (최대 50개로 제한하여 토큰 절약)
            titles = []
            for article in cluster_articles[:50]:
                title = article.get('title', '')
                if title:
                    titles.append(title)
            
            if not titles:
                return {}
            
            # LLM 프롬프트 구성
            titles_text = '\n'.join([f"{i+1}. {title}" for i, title in enumerate(titles)])
            
            prompt = f"""다음 {len(titles)}개의 한국 정치 기사 제목들을 분석하여 주요 정치 사건들을 식별하고 분류해주세요:

{titles_text}

요구사항:
1. 3-6개의 주요 정치 사건을 식별하세요
2. 각 사건별로 핵심 키워드 3-4개를 추출하세요  
3. 각 사건의 정확한 명칭을 20자 내외로 작성하세요
4. 명백히 다른 주제의 기사는 '기타'로 분류하세요

JSON 형태로 응답해주세요:
{{
  "events": [
    {{
      "event_id": "사건_식별자",
      "title": "20자 내외 사건명",
      "keywords": ["키워드1", "키워드2", "키워드3"],
      "description": "사건 간단 설명"
    }}
  ]
}}"""

            # OpenAI API 호출
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "당신은 한국 정치 뉴스 분석 전문가입니다. 정확하고 객관적인 분석을 제공해주세요."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,  # 일관성을 위해 낮은 온도
                max_tokens=1000
            )
            
            # JSON 파싱
            import json
            try:
                llm_response = response.choices[0].message.content
                # JSON 부분만 추출 (```json 태그 제거)
                if "```json" in llm_response:
                    json_start = llm_response.find("```json") + 7
                    json_end = llm_response.find("```", json_start)
                    llm_response = llm_response[json_start:json_end]
                elif "```" in llm_response:
                    json_start = llm_response.find("```") + 3
                    json_end = llm_response.rfind("```")
                    llm_response = llm_response[json_start:json_end]
                
                patterns = json.loads(llm_response.strip())
                console.print(f"✅ LLM 패턴 분석 완료: {len(patterns.get('events', []))}개 사건 식별")
                
                return patterns
                
            except json.JSONDecodeError as e:
                console.print(f"❌ LLM 응답 JSON 파싱 실패: {str(e)}")
                console.print(f"원본 응답: {llm_response[:200]}...")
                return {}
                
        except Exception as e:
            console.print(f"❌ LLM 패턴 생성 실패: {str(e)}")
            return {}

    def create_subgroups_within_cluster(self, cluster_articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """LLM 기반 동적 소그룹 생성 (혁신적 2.1단계)"""
        try:
            console.print(f"📝 {len(cluster_articles)}개 기사의 LLM 기반 소그룹 생성 중...")
            
            # LLM으로 동적 패턴 생성
            llm_patterns = self.generate_dynamic_patterns_with_llm(cluster_articles)
            
            if not llm_patterns or 'events' not in llm_patterns:
                console.print("⚠️ LLM 패턴 생성 실패, 기본 그룹화로 대체")
                return self._fallback_grouping(cluster_articles)
            
            # LLM이 생성한 패턴으로 분류
            event_patterns = {}
            for event in llm_patterns['events']:
                event_id = event.get('event_id', f"event_{len(event_patterns)}")
                event_patterns[event_id] = {
                    'keywords': event.get('keywords', []),
                    'title': event.get('title', '정치 이슈')
                }
            
            console.print(f"🎯 LLM 생성 패턴: {list(event_patterns.keys())}")
            
            # 사건별 그룹 분류
            event_groups = {event: [] for event in event_patterns.keys()}
            noise_articles = []
            
            for article in cluster_articles:
                title = article.get('title', '').lower()
                matched_events = []
                
                # 각 LLM 생성 패턴과 매칭 점수 계산
                for event_name, pattern_info in event_patterns.items():
                    score = 0
                    for keyword in pattern_info["keywords"]:
                        if keyword.lower() in title:
                            score += 1
                    
                    if score >= 2:  # 최소 2개 키워드 매칭
                        matched_events.append((event_name, score))
                
                # 가장 높은 점수의 사건에 배정
                if matched_events:
                    best_event = max(matched_events, key=lambda x: x[1])[0]
                    event_groups[best_event].append(article)
                else:
                    noise_articles.append(article)
            
            # 소그룹 생성
            subgroups = []
            
            # LLM 패턴 기반 그룹 추가 (최소 3개 이상인 것만)
            for event_name, articles in event_groups.items():
                if len(articles) >= 3:
                    subgroups.append({
                        'subgroup_id': event_name,
                        'articles': articles,
                        'article_count': len(articles),
                        'event_type': event_name,
                        'predefined_title': event_patterns[event_name]["title"]
                    })
                    console.print(f"   ✅ {event_name}: {len(articles)}개 기사 - '{event_patterns[event_name]['title']}'")
            
            # 노이즈 기사들은 각각 개별 소그룹으로
            for i, article in enumerate(noise_articles):
                subgroups.append({
                    'subgroup_id': f"noise_{i}",
                    'articles': [article],
                    'article_count': 1,
                    'event_type': "noise",
                    'predefined_title': None
                })
            
            # 사건별 그룹에서 3개 미만인 것들도 개별 소그룹으로
            for event_name, articles in event_groups.items():
                if 0 < len(articles) < 3:
                    for i, article in enumerate(articles):
                        subgroups.append({
                            'subgroup_id': f"small_{event_name}_{i}",
                            'articles': [article],
                            'article_count': 1,
                            'event_type': "small_group",
                            'predefined_title': None
                        })
            
            # 통계 출력
            major_groups = [sg for sg in subgroups if sg['article_count'] >= 3]
            individual_groups = [sg for sg in subgroups if sg['article_count'] == 1]
            
            console.print(f"✅ LLM 기반 소그룹 생성 완료:")
            console.print(f"   - 주요 사건 그룹: {len(major_groups)}개")
            console.print(f"   - 개별 기사 그룹: {len(individual_groups)}개")
            
            return subgroups
            
        except Exception as e:
            console.print(f"❌ LLM 소그룹 생성 실패: {str(e)}")
            return self._fallback_grouping(cluster_articles)
    
    def _fallback_grouping(self, cluster_articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """LLM 실패 시 백업 그룹화"""
        console.print("🔄 백업 그룹화 시스템 사용")
        subgroups = []
        for i, article in enumerate(cluster_articles):
            subgroups.append({
                'subgroup_id': f"fallback_{i}",
                'articles': [article],
                'article_count': 1,
                'event_type': "fallback",
                'predefined_title': None
            })
        return subgroups
    
    def generate_subgroup_headlines(self, subgroups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """각 소그룹별 정확한 헤드라인 생성 (개선된 2.2단계)"""
        try:
            console.print(f"📝 {len(subgroups)}개 소그룹 헤드라인 생성 중...")
            
            for subgroup in subgroups:
                # 사전 정의된 제목이 있으면 사용
                if subgroup.get('predefined_title'):
                    subgroup['title'] = subgroup['predefined_title']
                else:
                    # 개별 기사나 소규모 그룹은 제목 간소화
                    articles = subgroup['articles']
                    if len(articles) == 1:
                        original_title = articles[0].get('title', '')
                        simplified_title = self._simplify_single_title(original_title)
                        subgroup['title'] = simplified_title
                    else:
                        # 여러 기사지만 사전 패턴에 없는 경우
                        titles = [article.get('title', '') for article in articles]
                        subgroup['title'] = self._create_custom_headline(titles)
                
            console.print("✅ 소그룹 헤드라인 생성 완료")
            return subgroups
            
        except Exception as e:
            console.print(f"❌ 소그룹 헤드라인 생성 실패: {str(e)}")
            return subgroups
    
    def _simplify_single_title(self, title: str) -> str:
        """단일 기사 제목 간소화"""
        import re
        
        if not title:
            return "정치 이슈"
        
        # 불필요한 부분 제거
        simplified = title
        
        # 따옴표 내용 제거
        simplified = re.sub(r'["""].*?["""]', '', simplified)
        
        # 괄호 내용 제거  
        simplified = re.sub(r'\([^)]*\)', '', simplified)
        
        # 연속 공백 정리
        simplified = re.sub(r'\s+', ' ', simplified).strip()
        
        # 핵심 키워드 추출하여 간소화
        if '대통령실' in simplified and '논의' in simplified:
            return "대통령실 입장 발표"
        elif '조희대' in simplified and '사퇴' in simplified:
            return "조희대 사퇴 논란"
        elif '세종' in simplified and '집무실' in simplified:
            return "세종 집무실 이전"
        elif '규제' in simplified and ('배임죄' in simplified or '합리화' in simplified):
            return "규제합리화 추진"
        elif '한미' in simplified and '관세' in simplified:
            return "한미 관세협상"
        elif '내각' in simplified and '구성' in simplified:
            return "내각 구성"
        
        # 20자 내외로 조정
        if len(simplified) > 20:
            simplified = simplified[:18] + ".."
        
        return simplified if simplified else "정치 이슈"
    
    def _create_custom_headline(self, titles: List[str]) -> str:
        """사전 패턴에 없는 그룹의 헤드라인 생성"""
        import re
        from collections import Counter
        
        all_text = ' '.join(titles)
        
        # 공통 키워드 추출
        words = re.findall(r'[가-힣]{2,}', all_text)
        word_counts = Counter(words)
        
        # 상위 2개 키워드로 헤드라인 생성
        top_words = [word for word, count in word_counts.most_common(2) if count >= 2]
        
        if len(top_words) >= 2:
            headline = f"{top_words[0]} {top_words[1]}"
        elif len(top_words) == 1:
            headline = f"{top_words[0]} 이슈"
        else:
            headline = "정치 현안"
        
        # 20자 내외로 조정
        if len(headline) > 20:
            headline = headline[:18] + ".."
        
        return headline
    
    def merge_similar_subgroups(self, all_subgroups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """사건 기반 엄격한 재병합 (개선된 2.3단계)"""
        try:
            console.print(f"🔄 {len(all_subgroups)}개 소그룹 재병합 중...")
            
            # 주요 사건 그룹과 개별 그룹 분리
            major_groups = [sg for sg in all_subgroups if sg['article_count'] >= 3]
            individual_groups = [sg for sg in all_subgroups if sg['article_count'] < 3]
            
            console.print(f"   - 주요 그룹: {len(major_groups)}개")
            console.print(f"   - 개별 그룹: {len(individual_groups)}개")
            
            # 주요 그룹들은 그대로 유지 (이미 명확한 사건별로 분류됨)
            final_groups = []
            
            # 주요 사건 그룹들 추가
            for group in major_groups:
                final_groups.append({
                    'group_id': group['subgroup_id'],
                    'articles': group['articles'],
                    'article_count': group['article_count'],
                    'title': group.get('title', '정치 이슈'),
                    'event_type': group.get('event_type', 'unknown')
                })
            
            # 개별 그룹들 중에서 유사한 것들만 선별적 병합
            merged_individuals = self._selective_merge_individuals(individual_groups)
            final_groups.extend(merged_individuals)
            
            console.print(f"✅ {len(final_groups)}개 그룹으로 재병합 완료")
            
            # 그룹별 상세 정보 출력
            for group in final_groups:
                if group['article_count'] >= 5:
                    console.print(f"   - {group['title']}: {group['article_count']}개 기사")
            
            return final_groups
            
        except Exception as e:
            console.print(f"❌ 재병합 실패: {str(e)}")
            return all_subgroups
    
    def _selective_merge_individuals(self, individual_groups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """개별 그룹들의 선별적 병합"""
        import re
        
        # 동일 사건 패턴 정의 (더 엄격)
        exact_patterns = {
            "대통령실_입장": ["대통령실", "입장", "발표"],
            "장동혁_발언": ["장동혁", "대통령"],
            "송언석_발언": ["송언석", "비판"],
            "국힘_반발": ["국힘", "반발", "비판"]
        }
        
        pattern_groups = {pattern: [] for pattern in exact_patterns.keys()}
        remaining_individuals = []
        
        # 패턴별 분류
        for group in individual_groups:
            title = group.get('title', '').lower()
            matched = False
            
            for pattern_name, keywords in exact_patterns.items():
                if sum(1 for keyword in keywords if keyword in title) >= 2:
                    pattern_groups[pattern_name].append(group)
                    matched = True
                    break
            
            if not matched:
                remaining_individuals.append(group)
        
        # 병합된 그룹 생성
        merged_groups = []
        
        for pattern_name, groups in pattern_groups.items():
            if len(groups) >= 3:  # 최소 3개 이상만 병합
                merged_articles = []
                for group in groups:
                    merged_articles.extend(group['articles'])
                
                merged_groups.append({
                    'group_id': f"merged_{pattern_name}",
                    'articles': merged_articles,
                    'article_count': len(merged_articles),
                    'title': self._get_pattern_title(pattern_name),
                    'event_type': 'merged'
                })
            else:
                # 3개 미만은 개별 유지
                remaining_individuals.extend(groups)
        
        # 개별 그룹들 추가 (5개 이상만 - 노이즈 필터링)
        for group in remaining_individuals:
            if group['article_count'] >= 1:  # 일단 모든 개별 그룹 포함
                merged_groups.append({
                    'group_id': group['subgroup_id'],
                    'articles': group['articles'],
                    'article_count': group['article_count'],
                    'title': group.get('title', '정치 이슈'),
                    'event_type': group.get('event_type', 'individual')
                })
        
        return merged_groups
    
    def _get_pattern_title(self, pattern_name: str) -> str:
        """패턴별 제목 반환"""
        pattern_titles = {
            "대통령실_입장": "대통령실 공식 입장",
            "장동혁_발언": "장동혁 대통령 비판",
            "송언석_발언": "송언석 정부 비판", 
            "국힘_반발": "국민의힘 반발"
        }
        return pattern_titles.get(pattern_name, "정치 이슈")

    def analyze_clusters(self, articles: List[Dict[str, Any]], cluster_labels: np.ndarray, 
                        bias_mapping: Dict[str, str]) -> List[Dict[str, Any]]:
        """새로운 4단계 후처리 시스템으로 클러스터 분석"""
        try:
            console.print("🔄 4단계 후처리 시스템 시작...")
            
            # 클러스터별 기사 그룹화
            cluster_groups = {}
            for idx, label in enumerate(cluster_labels):
                if label != -1:  # 노이즈 제외
                    if label not in cluster_groups:
                        cluster_groups[label] = []
                    cluster_groups[label].append(articles[idx])
            
            console.print(f"📊 초기 클러스터: {len(cluster_groups)}개")
            
            # 모든 소그룹 수집
            all_subgroups = []
            
            # 2.1단계: 각 클러스터 내에서 소그룹 생성
            console.print("\n🔄 2.1단계: 클러스터 내 소그룹 생성")
            for label, cluster_articles in cluster_groups.items():
                console.print(f"📝 클러스터 {label} ({len(cluster_articles)}개 기사) 처리 중...")
                subgroups = self.create_subgroups_within_cluster(cluster_articles)
                all_subgroups.extend(subgroups)
            
            console.print(f"✅ 2.1단계 완료: 총 {len(all_subgroups)}개 소그룹 생성")
            
            # 2.2단계: 각 소그룹별 헤드라인 생성
            console.print(f"\n🔄 2.2단계: 소그룹 헤드라인 생성")
            all_subgroups = self.generate_subgroup_headlines(all_subgroups)
            
            # 2.3단계: 소그룹들을 제목 유사도 기반으로 재병합
            console.print(f"\n🔄 2.3단계: 소그룹 재병합")
            final_groups = self.merge_similar_subgroups(all_subgroups)
            
            # 언론사별 bias 통계 계산
            for group in final_groups:
                bias_counts = {'left': 0, 'center': 0, 'right': 0}
                for article in group['articles']:
                    media_id = article.get('media_id')
                    if media_id and media_id in bias_mapping:
                        bias = bias_mapping[media_id]
                        if bias in bias_counts:
                            bias_counts[bias] += 1
                
                group['left_source'] = bias_counts['left']
                group['center_source'] = bias_counts['center']
                group['right_source'] = bias_counts['right']
            
            # 2.4단계: 품질 기반 상위 3개 선별 (개선됨)
            console.print(f"\n🔄 2.4단계: 품질 기반 상위 3개 그룹 선별")
            
            # 품질 점수 계산 (기사수 + 사건 명확도)
            for group in final_groups:
                quality_score = group['article_count']
                
                # 사건 명확도 보너스
                if group.get('event_type') in ['조희대_사퇴', '세종_집무실', '규제_합리화', '한미_관세']:
                    quality_score += 10  # 명확한 사건에 보너스
                elif group.get('event_type') == 'merged':
                    quality_score += 5   # 병합된 그룹에 중간 보너스
                
                # 최소 임계값 적용 (20개로 상향)
                if group['article_count'] < 20:
                    quality_score = 0  # 20개 미만 그룹은 제외
                
                group['quality_score'] = quality_score
            
            # 품질 점수 기준 정렬
            qualified_groups = [g for g in final_groups if g.get('quality_score', 0) > 0]
            qualified_groups = sorted(qualified_groups, key=lambda x: x['quality_score'], reverse=True)
            top_groups = qualified_groups[:3]
            
            console.print(f"✅ 후처리 완료!")
            console.print(f"📊 전체 그룹: {len(final_groups)}개")
            console.print(f"📊 자격 요건 충족: {len(qualified_groups)}개")
            console.print(f"📊 최종 선별: {len(top_groups)}개")
            
            for i, group in enumerate(top_groups, 1):
                event_type = group.get('event_type', 'unknown')
                console.print(f"   {i}위: {group['article_count']}개 기사 - '{group['title']}' ({event_type})")
            
            # 기존 형식에 맞춰 변환
            result = []
            for i, group in enumerate(top_groups):
                result.append({
                    'cluster_id': group.get('group_id', f'final_{i}'),
                    'articles': group['articles'],
                    'total_articles': group['article_count'],
                    'title': group['title'],
                    'left_source': group['left_source'],
                    'center_source': group['center_source'],
                    'right_source': group['right_source']
                })
            
            return result
            
        except Exception as e:
            console.print(f"❌ 후처리 시스템 실패: {str(e)}")
            return []
    
    def save_issues_to_db(self, top_clusters: List[Dict[str, Any]], category: str) -> List[str]:
        """상위 클러스터를 issues 테이블에 저장 (카테고리 정보 포함)"""
        try:
            console.print(f"💾 {category} 카테고리 issues 테이블에 저장 중...")
            
            saved_issue_ids = []
            
            for i, cluster in enumerate(top_clusters, 1):
                try:
                    issue_data = {
                        'title': cluster['title'],
                        'category': category,  # 카테고리 정보 추가
                        'source': cluster['total_articles'],
                        'left_source': cluster['left_source'],
                        'center_source': cluster['center_source'],
                        'right_source': cluster['right_source'],
                        'created_at': datetime.now(timezone.utc).isoformat()
                    }
                    
                    result = self.supabase_manager.client.table('issues').insert(issue_data).execute()
                    
                    if result.data:
                        issue_id = result.data[0]['id']
                        saved_issue_ids.append(issue_id)
                        console.print(f"✅ {category} 이슈 {i} 저장 완료: {cluster['title']} ({cluster['total_articles']}개 기사)")
                    else:
                        console.print(f"❌ {category} 이슈 {i} 저장 실패")
                        
                except Exception as e:
                    console.print(f"❌ {category} 이슈 {i} 저장 오류: {str(e)}")
            
            console.print(f"✅ {category} 카테고리 {len(saved_issue_ids)}개 이슈 저장 완료")
            return saved_issue_ids
            
        except Exception as e:
            console.print(f"❌ {category} issues 테이블 저장 실패: {str(e)}")
            return []
    
    def update_articles_with_issue_ids(self, top_clusters: List[Dict[str, Any]], 
                                     issue_ids: List[str]) -> int:
        """클러스터 소속 기사들에 issue_id 업데이트"""
        try:
            console.print("🔄 기사들에 issue_id 업데이트 중...")
            
            total_updated = 0
            
            for cluster, issue_id in zip(top_clusters, issue_ids):
                cluster_articles = cluster['articles']
                updated_count = 0
                
                for article in cluster_articles:
                    try:
                        result = self.supabase_manager.client.table('articles').update({
                            'issue_id': issue_id
                        }).eq('id', article['id']).execute()
                        
                        if result.data:
                            updated_count += 1
                        
                    except Exception as e:
                        console.print(f"❌ 기사 업데이트 실패: {article.get('id', 'Unknown')} - {str(e)}")
                
                total_updated += updated_count
                console.print(f"✅ 클러스터 {cluster['cluster_id']}: {updated_count}개 기사 업데이트")
            
            console.print(f"✅ 총 {total_updated}개 기사 issue_id 업데이트 완료")
            return total_updated
            
        except Exception as e:
            console.print(f"❌ 기사 issue_id 업데이트 실패: {str(e)}")
            return 0
    
    def process_single_category(self, category: str) -> Dict[str, Any]:
        """단일 카테고리 클러스터링 처리"""
        try:
            console.print(f"\n{'='*20} {category} 카테고리 처리 시작 {'='*20}")
            
            # 1. 카테고리별 기사 조회
            articles = self.fetch_articles_by_category(category)
            if not articles:
                console.print(f"❌ {category} 카테고리 기사가 없습니다.")
                return {'category': category, 'success': False, 'issues': 0, 'articles': 0}
            
            # 2. 임베딩 추출
            embeddings, valid_articles = self.extract_embeddings(articles)
            if len(embeddings) == 0:
                console.print(f"❌ {category} 유효한 임베딩이 없습니다.")
                return {'category': category, 'success': False, 'issues': 0, 'articles': 0}
            
            if len(valid_articles) < 30:  # 최소 기사 수 확인 (카테고리별 조정)
                console.print(f"⚠️ {category} 기사 수가 너무 적습니다: {len(valid_articles)}개")
                console.print("클러스터링을 위해서는 최소 30개 이상의 기사가 필요합니다.")
                return {'category': category, 'success': False, 'issues': 0, 'articles': 0}
            
            # 3. UMAP 차원축소
            reduced_embeddings = self.perform_umap_reduction(embeddings)
            if len(reduced_embeddings) == 0:
                console.print(f"❌ {category} UMAP 차원축소 실패")
                return {'category': category, 'success': False, 'issues': 0, 'articles': 0}
            
            # 4. HDBSCAN 클러스터링
            cluster_labels = self.perform_hdbscan_clustering(reduced_embeddings)
            if len(cluster_labels) == 0:
                console.print(f"❌ {category} HDBSCAN 클러스터링 실패")
                return {'category': category, 'success': False, 'issues': 0, 'articles': 0}
            
            # 5. 언론사 bias 매핑 조회
            bias_mapping = self.get_media_bias_mapping()
            if not bias_mapping:
                console.print(f"❌ {category} 언론사 bias 매핑 조회 실패")
                return {'category': category, 'success': False, 'issues': 0, 'articles': 0}
            
            # 6. 클러스터 분석 및 상위 3개 선별
            top_clusters = self.analyze_clusters(valid_articles, cluster_labels, bias_mapping)
            if not top_clusters:
                console.print(f"❌ {category} 유효한 클러스터가 없습니다.")
                return {'category': category, 'success': False, 'issues': 0, 'articles': 0}
            
            # 7. issues 테이블에 저장 (카테고리 정보 포함)
            issue_ids = self.save_issues_to_db(top_clusters, category)
            if not issue_ids:
                console.print(f"❌ {category} issues 테이블 저장 실패")
                return {'category': category, 'success': False, 'issues': 0, 'articles': 0}
            
            # 8. 기사들에 issue_id 업데이트
            updated_count = self.update_articles_with_issue_ids(top_clusters, issue_ids)
            
            console.print(f"✅ {category} 카테고리 완료: {len(issue_ids)}개 이슈, {updated_count}개 기사")
            return {
                'category': category, 
                'success': True, 
                'issues': len(issue_ids), 
                'articles': updated_count
            }
            
        except Exception as e:
            console.print(f"❌ {category} 카테고리 처리 실패: {str(e)}")
            return {'category': category, 'success': False, 'issues': 0, 'articles': 0}

    def run_clustering(self) -> bool:
        """하이브리드 방식으로 전체 카테고리 클러스터링 실행"""
        try:
            console.print("=" * 80)
            console.print("🚀 전체 정치 카테고리 클러스터링 시작 (하이브리드 방식)")
            console.print("=" * 80)
            
            total_results = []
            
            # 1단계: 대용량 카테고리 순차 처리
            console.print("\n🔄 1단계: 대용량 카테고리 순차 처리")
            for category in self.categories["large"]:
                result = self.process_single_category(category)
                total_results.append(result)
            
            # 2단계: 소량 카테고리 병렬 처리 (추후 구현)
            console.print("\n🔄 2단계: 소량 카테고리 처리")
            for category in self.categories["small"]:
                result = self.process_single_category(category)
                total_results.append(result)
            
            # 최종 결과 집계
            total_issues = sum(r['issues'] for r in total_results)
            total_articles = sum(r['articles'] for r in total_results)
            successful_categories = [r['category'] for r in total_results if r['success']]
            
            console.print("\n" + "=" * 80)
            console.print("🎉 전체 카테고리 클러스터링 완료!")
            console.print(f"✅ 처리된 카테고리: {len(successful_categories)}개")
            console.print(f"✅ 생성된 총 이슈: {total_issues}개")
            console.print(f"✅ 업데이트된 총 기사: {total_articles}개")
            console.print("\n📊 카테고리별 결과:")
            
            for result in total_results:
                status = "✅" if result['success'] else "❌"
                console.print(f"   {status} {result['category']}: {result['issues']}개 이슈, {result['articles']}개 기사")
            
            console.print("=" * 80)
            
            return len(successful_categories) > 0
            
        except Exception as e:
            console.print(f"❌ 전체 클러스터링 프로세스 실패: {str(e)}")
            return False


def main():
    """메인 함수"""
    try:
        clusterer = MultiCategoryClusterer()
        success = clusterer.run_clustering()
        
        if success:
            console.print("\n✅ 전체 카테고리 클러스터링 성공!")
        else:
            console.print("\n❌ 전체 카테고리 클러스터링 실패!")
            
    except KeyboardInterrupt:
        console.print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        console.print(f"\n❌ 오류 발생: {str(e)}")


if __name__ == "__main__":
    main()
