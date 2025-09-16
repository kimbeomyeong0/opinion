#!/usr/bin/env python3
"""
개선된 이슈 클러스터링 스크립트
- '및' 문제 해결: 더 세밀한 이슈 분리
- 명확한 단일 주제 클러스터링
"""

import sys
import os
import json
import pytz
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utils.supabase_manager import SupabaseManager

# OpenAI 설치 확인 및 import
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    print("❌ OpenAI 라이브러리가 설치되지 않았습니다.")
    print("pip install openai 명령으로 설치해주세요.")
    OPENAI_AVAILABLE = False

@dataclass
class Article:
    """기사 데이터 클래스"""
    id: str
    title: str
    lead_content: str
    media_id: str
    published_at: str

@dataclass
class Cluster:
    """클러스터 데이터 클래스"""
    id: str
    name: str
    keywords: List[str]
    articles: List[Article]
    issue_number: Optional[int] = None

class ImprovedIssueClustering:
    """개선된 이슈 클러스터링 클래스"""
    
    def __init__(self):
        """초기화"""
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
        
        if OPENAI_AVAILABLE:
            self.openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        else:
            self.openai_client = None
        
        # 언론사 성향 매핑 테이블
        self.media_bias_mapping = self.get_media_bias_mapping()
    
    def get_media_bias_mapping(self) -> Dict[str, str]:
        """언론사 ID → 성향 매핑 테이블 생성"""
        try:
            result = self.supabase_manager.client.table('media_outlets').select('id, bias').execute()
            return {item['id']: item['bias'] for item in result.data}
        except Exception as e:
            print(f"❌ 언론사 성향 매핑 실패: {str(e)}")
            return {}
    
    def parse_date_range(self, date_input: str) -> Tuple[str, str]:
        """사용자 입력을 KST 날짜 범위로 파싱"""
        try:
            if '~' in date_input:
                start_str, end_str = date_input.split('~')
            else:
                start_str = end_str = date_input.strip()
            
            current_year = datetime.now().year
            
            # 형식 변환: 0910 -> 2024-09-10
            start_date = f"{current_year}-{start_str[:2]}-{start_str[2:]}"
            end_date = f"{current_year}-{end_str[:2]}-{end_str[2:]}"
            
            # KST → UTC 변환
            kst = pytz.timezone('Asia/Seoul')
            utc = pytz.UTC
            
            # 시작: 해당 날 00:00 KST
            start_kst = kst.localize(datetime.strptime(start_date, '%Y-%m-%d'))
            
            # 종료: 다음날 00:00 KST (해당 날 23:59:59까지 포함)
            end_kst = kst.localize(datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1))
            
            start_utc = start_kst.astimezone(utc)
            end_utc = end_kst.astimezone(utc)
            
            return start_utc.isoformat(), end_utc.isoformat()
            
        except Exception as e:
            raise ValueError(f"날짜 형식 오류: {e}")
    
    def fetch_articles_by_date_range(self, start_date: str, end_date: str) -> List[Article]:
        """날짜 범위에 해당하는 기사만 조회"""
        try:
            print(f"📅 날짜 범위: {start_date} ~ {end_date}")
            
            result = self.supabase_manager.client.table('articles').select(
                'id, title, content, media_id, published_at'
            ).eq('is_preprocessed', True).gte('published_at', start_date).lt('published_at', end_date).execute()
            
            if not result.data:
                print("❌ 해당 날짜 범위에 기사가 없습니다.")
                return []
            
            articles = []
            for item in result.data:
                # 첫 두 문장 추출 (맥락과 효율성의 균형)
                first_two_sentences = ""
                if item['content']:
                    first_two_sentences = self.extract_first_two_sentences(item['content'])
                
                article = Article(
                    id=item['id'],
                    title=item['title'],
                    lead_content=first_two_sentences,
                    media_id=item['media_id'],
                    published_at=item['published_at']
                )
                articles.append(article)
            
            print(f"✅ {len(articles)}개 기사 조회 완료")
            return articles
            
        except Exception as e:
            print(f"❌ 기사 조회 실패: {str(e)}")
            return []
    
    def extract_first_two_sentences(self, content: str) -> str:
        """기사 본문에서 첫 두 문장 추출 (맥락과 효율성의 균형)"""
        if not content:
            return ""
        
        # 문장들을 마침표로 분리
        sentences = content.split('.')
        
        # 첫 두 문장 추출
        if len(sentences) >= 2:
            first_two = sentences[0].strip() + '. ' + sentences[1].strip() + '.'
        elif len(sentences) == 1:
            first_two = sentences[0].strip() + '.'
        else:
            first_two = content.split('\n')[0].strip()
        
        # 너무 길면 150자로 제한
        if len(first_two) > 150:
            first_two = first_two[:150] + "..."
        
        return first_two
    
    def split_into_2_batches(self, articles: List[Article]) -> List[List[Article]]:
        """2개 배치로 균등 분할 (토큰 제한 해결)"""
        batch_size = len(articles) // 2
        batches = []
        
        for i in range(2):
            start_idx = i * batch_size
            if i == 1:  # 마지막 배치
                end_idx = len(articles)
            else:
                end_idx = (i + 1) * batch_size
            
            batch = articles[start_idx:end_idx]
            batches.append(batch)
        
        return batches
    
    def create_precise_clustering_prompt(self, articles: List[Article]) -> str:
        """개선된 클러스터링 프롬프트 - 단일 주제 강조"""
        articles_text = ""
        for i, article in enumerate(articles, 1):
            articles_text += f"{i}. 제목: {article.title}\n"
            articles_text += f"   내용: {article.lead_content}\n\n"
        
        prompt = f"""
다음은 정치 뉴스의 제목과 첫 두 문장입니다. **하나의 명확한 정치 이슈**를 다루는 기사들만 하나의 그룹으로 분류해주세요.

기사 목록:
{articles_text}

🚨 **중요한 분류 원칙** 🚨

1. **단일 주제 원칙**: 각 그룹은 반드시 **하나의 구체적인 정치 이슈**만 다뤄야 합니다
   - ❌ "사법개혁 및 검찰 수사" → 2개 이슈가 섞임
   - ✅ "사법개혁" 또는 "검찰 수사" → 각각 별도 그룹

2. **구체적 사건/정책 중심**: 추상적이지 않고 구체적인 사건이나 정책으로 분류
   - ✅ "이재명 대표 사법 리스크"
   - ✅ "윤석열 정부 의료진 집단행동 대응"
   - ✅ "한동훈 당대표 선출"
   - ❌ "정치 갈등" (너무 포괄적)

3. **인물 중심 이슈**: 특정 정치인과 관련된 구체적 사건
   - ✅ "이재명 헬기 이송 의혹"
   - ✅ "한동훈 전 장관 검찰 출석"
   - ❌ "야당 대표들 동향" (여러 인물 섞임)

4. **정책별 분리**: 서로 다른 정책 영역은 반드시 분리
   - ✅ "의료진 집단행동" (의료정책)
   - ✅ "부동산 정책" (부동산정책)
   - ❌ "민생정책" (여러 정책 섞임)

5. **시기별 구분**: 같은 이슈라도 시기가 다르면 분리 고려
   - ✅ "9월 의대 증원 발표"
   - ✅ "10월 의대 증원 후속조치"

🎯 **목표**: '및', '그리고', '등' 없이 **명확한 단일 주제명**

응답 형식 (JSON):
{{
    "clusters": [
        {{
            "name": "구체적인 단일 정치 이슈명",
            "keywords": ["핵심키워드1", "핵심키워드2", "핵심키워드3"],
            "article_indices": [1, 3, 5],
            "confidence": "high|medium|low",
            "reasoning": "이 기사들이 같은 그룹인 이유"
        }}
    ]
}}
"""
        return prompt
    
    def create_merge_prompt(self, all_clusters: List[List[Cluster]]) -> str:
        """배치 결과 통합을 위한 프롬프트 생성"""
        clusters_text = ""
        cluster_id = 1
        
        for batch_idx, batch_clusters in enumerate(all_clusters, 1):
            clusters_text += f"\n=== 배치 {batch_idx} 결과 ===\n"
            for cluster in batch_clusters:
                clusters_text += f"{cluster_id}. {cluster.name}\n"
                clusters_text += f"   키워드: {', '.join(cluster.keywords)}\n"
                clusters_text += f"   기사 수: {len(cluster.articles)}개\n"
                # 첫 세 문장 샘플 추가
                sample_articles = cluster.articles[:2]
                clusters_text += f"   내용 샘플:\n"
                for i, article in enumerate(sample_articles, 1):
                    clusters_text += f"     {i}. {article.lead_content[:100]}...\n"
                clusters_text += "\n"
                cluster_id += 1
        
        prompt = f"""
다음은 2개 배치로 나누어 클러스터링한 정치 뉴스 결과입니다. 의미적으로 유사한 정치 이슈들을 정확하게 합쳐서 최종 이슈 목록을 만들어주세요.

클러스터링 결과:
{clusters_text}

🚨 **중요한 통합 원칙** 🚨

1. **단일 주제 유지**: 통합 시에도 반드시 하나의 구체적인 정치 이슈만 다뤄야 합니다
   - ❌ "사법개혁 및 검찰 수사" → 2개 이슈가 섞임
   - ✅ "사법개혁" 또는 "검찰 수사" → 각각 별도 이슈

2. **정치적 맥락 고려**: 같은 정치 이슈의 다른 표현들을 인식하세요
   - 예: "조희대 대법원장 사퇴"와 "대법원장 사퇴 요구"는 같은 이슈
   - 예: "한미 관세 협상"과 "미국 관세 협상"은 같은 이슈

3. **의미적 유사성 판단**: 단순 키워드가 아닌 전체적 맥락으로 판단하세요
   - 첫 세 문장의 핵심 주제가 같은지 확인
   - 정치적 맥락과 배경이 유사한지 확인

4. **'및' 금지**: 절대로 여러 이슈를 하나로 합치지 마세요
   - 각 이슈는 명확한 단일 주제여야 함

요구사항:
1. 의미적으로 유사한 정치 이슈들을 하나로 합쳐주세요
2. 각 최종 이슈에 적절한 이름을 제공해주세요 (단일 주제)
3. 각 이슈의 핵심 키워드 3-5개를 추출해주세요
4. 합쳐진 이슈에 포함된 원본 클러스터 번호들을 기록해주세요

응답 형식 (JSON):
{{
    "final_clusters": [
        {{
            "name": "최종 정치 이슈명 (단일 주제)",
            "keywords": ["키워드1", "키워드2", "키워드3"],
            "merged_from": [1, 5, 8],
            "confidence": "high|medium|low"
        }}
    ]
}}
"""
        return prompt
    
    def merge_batch_clusters(self, all_clusters: List[List[Cluster]]) -> Optional[List[Cluster]]:
        """배치별 클러스터 결과들을 통합"""
        if not self.openai_client:
            print("❌ OpenAI 클라이언트가 없습니다.")
            return None
        
        try:
            print("🔄 배치 결과 통합 중...")
            
            # 프롬프트 생성
            prompt = self.create_merge_prompt(all_clusters)
            
            # LLM 호출
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "당신은 정치 뉴스 분석 전문가입니다. 여러 배치의 클러스터링 결과를 분석하여 의미적으로 유사한 정치 이슈들을 정확하게 합쳐주세요. 절대로 여러 이슈를 하나로 합치지 마세요."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=6000
            )
            
            # 응답 파싱
            response_text = response.choices[0].message.content
            print("📝 통합 결과 받음")
            
            # JSON 파싱 시도
            try:
                json_start = response_text.find('{')
                json_end = response_text.rfind('}') + 1
                json_text = response_text[json_start:json_end]
                
                result = json.loads(json_text)
                final_clusters_data = result.get('final_clusters', [])
                
                # 모든 원본 클러스터를 하나의 리스트로 만들기
                all_original_clusters = []
                for batch_clusters in all_clusters:
                    all_original_clusters.extend(batch_clusters)
                
                # 최종 클러스터 생성
                final_clusters = []
                for i, cluster_data in enumerate(final_clusters_data):
                    merged_from_indices = cluster_data.get('merged_from', [])
                    merged_articles = []
                    confidence = cluster_data.get('confidence', 'medium')
                    
                    # 통합된 클러스터들의 기사들을 수집
                    for idx in merged_from_indices:
                        if 1 <= idx <= len(all_original_clusters):
                            original_cluster = all_original_clusters[idx - 1]
                            merged_articles.extend(original_cluster.articles)
                    
                    # 중복 기사 제거
                    unique_articles = []
                    seen_ids = set()
                    for article in merged_articles:
                        if article.id not in seen_ids:
                            unique_articles.append(article)
                            seen_ids.add(article.id)
                    
                    final_cluster = Cluster(
                        id=f"final_cluster_{i+1}",
                        name=cluster_data.get('name', f'통합 이슈 {i+1}'),
                        keywords=cluster_data.get('keywords', []),
                        articles=unique_articles
                    )
                    final_clusters.append(final_cluster)
                    
                    print(f"  📊 통합 이슈 {i+1}: {final_cluster.name} ({len(unique_articles)}개 기사, 신뢰도: {confidence})")
                
                print(f"✅ {len(final_clusters)}개 최종 클러스터 생성 완료")
                return final_clusters
                
            except json.JSONDecodeError as e:
                print(f"❌ JSON 파싱 실패: {str(e)}")
                print(f"응답 내용: {response_text[:500]}...")
                return None
                
        except Exception as e:
            print(f"❌ 배치 통합 실패: {str(e)}")
            return None
    
    def cluster_with_precise_llm(self, articles: List[Article]) -> Optional[List[Cluster]]:
        """개선된 LLM 클러스터링"""
        if not self.openai_client:
            print("❌ OpenAI 클라이언트가 없습니다.")
            return None
        
        try:
            print("🎯 개선된 LLM 클러스터링 시작...")
            
            # 프롬프트 생성
            prompt = self.create_precise_clustering_prompt(articles)
            
            # LLM 호출
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "당신은 정치 뉴스 분석 전문가입니다. 기사들을 명확한 단일 주제 이슈로만 분류해주세요. 절대로 여러 이슈를 하나로 합치지 마세요."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,  # 더 일관된 결과를 위해 낮춤
                max_tokens=6000
            )
            
            # 응답 파싱
            response_text = response.choices[0].message.content
            print("📝 LLM 응답 받음")
            
            # JSON 파싱 시도
            try:
                # JSON 부분만 추출
                json_start = response_text.find('{')
                json_end = response_text.rfind('}') + 1
                json_text = response_text[json_start:json_end]
                
                result = json.loads(json_text)
                clusters_data = result.get('clusters', [])
                
                # Cluster 객체로 변환
                clusters = []
                for i, cluster_data in enumerate(clusters_data):
                    article_indices = cluster_data.get('article_indices', [])
                    cluster_articles = [articles[idx-1] for idx in article_indices if 1 <= idx <= len(articles)]
                    
                    # '및' 문제 검증
                    cluster_name = cluster_data.get('name', f'그룹 {i+1}')
                    confidence = cluster_data.get('confidence', 'medium')
                    reasoning = cluster_data.get('reasoning', '')
                    
                    # 경고 체크
                    warning_words = ['및', '그리고', '또한', '아울러', '동시에', '함께']
                    has_warning = any(word in cluster_name for word in warning_words)
                    
                    if has_warning:
                        print(f"⚠️ 의심 클러스터: '{cluster_name}' - 복수 이슈 가능성")
                    
                    cluster = Cluster(
                        id=f"cluster_{i+1}",
                        name=cluster_name,
                        keywords=cluster_data.get('keywords', []),
                        articles=cluster_articles
                    )
                    clusters.append(cluster)
                
                print(f"✅ {len(clusters)}개 정밀 클러스터 생성 완료")
                return clusters
                
            except json.JSONDecodeError as e:
                print(f"❌ JSON 파싱 실패: {str(e)}")
                print(f"응답 내용: {response_text[:500]}...")
                return None
                
        except Exception as e:
            print(f"❌ LLM 클러스터링 실패: {str(e)}")
            return None
    
    def validate_clusters(self, clusters: List[Cluster]) -> List[Cluster]:
        """클러스터 품질 검증 및 필터링"""
        validated_clusters = []
        
        print("🔍 클러스터 품질 검증 중...")
        
        for cluster in clusters:
            issues = []
            
            # 1. '및' 등 복수 이슈 키워드 체크
            warning_words = ['및', '그리고', '또한', '아울러', '동시에', '함께', '등']
            has_multiple_issues = any(word in cluster.name for word in warning_words)
            if has_multiple_issues:
                issues.append(f"복수 이슈 의심: '{cluster.name}'")
            
            # 2. 너무 포괄적인 이름 체크
            generic_words = ['정치', '정부', '여당', '야당', '국정', '정책', '동향', '상황']
            is_too_generic = any(word == cluster.name or cluster.name.startswith(word) for word in generic_words)
            if is_too_generic:
                issues.append(f"너무 포괄적: '{cluster.name}'")
            
            # 3. 최소 기사 수 체크 (10개 이상)
            if len(cluster.articles) < 10:
                issues.append(f"기사 수 부족: {len(cluster.articles)}개")
                continue  # 10개 미만은 제외
            
            # 4. 경고사항 출력
            if issues:
                print(f"⚠️  {cluster.name}: {', '.join(issues)}")
            else:
                print(f"✅ {cluster.name}: {len(cluster.articles)}개 기사")
            
            validated_clusters.append(cluster)
        
        print(f"🔍 검증 완료: {len(clusters)} → {len(validated_clusters)}개 클러스터")
        return validated_clusters
    
    def assign_issue_numbers(self, clusters: List[Cluster]) -> List[Cluster]:
        """기사 수 순으로 이슈 번호 할당"""
        # 기사 수 기준 내림차순 정렬
        sorted_clusters = sorted(clusters, key=lambda x: len(x.articles), reverse=True)
        
        for i, cluster in enumerate(sorted_clusters, 1):
            cluster.issue_number = i
            # 이슈 번호는 괄호로 별도 표시
            cluster.name = f"{cluster.name}"
        
        return sorted_clusters
    
    def count_media_bias(self, cluster: Cluster) -> Dict[str, int]:
        """언론사 성향별 기사 수 계산"""
        bias_counts = {'left': 0, 'center': 0, 'right': 0}
        
        for article in cluster.articles:
            bias = self.media_bias_mapping.get(article.media_id, 'center')
            if bias in bias_counts:
                bias_counts[bias] += 1
        
        return bias_counts
    
    def save_to_issues_table(self, clusters: List[Cluster]) -> bool:
        """클러스터를 issues 테이블에 저장"""
        try:
            print(f"💾 {len(clusters)}개 이슈를 issues 테이블에 저장 중...")
            
            issues_data = []
            for cluster in clusters:
                if not cluster.articles:  # 기사가 없는 클러스터는 건너뛰기
                    continue
                
                # 언론사 성향별 기사 수 계산
                bias_counts = self.count_media_bias(cluster)
                
                issue_data = {
                    "title": cluster.name,
                    "source": len(cluster.articles),
                    "left_source": bias_counts['left'],
                    "center_source": bias_counts['center'],
                    "right_source": bias_counts['right'],
                    "created_at": datetime.now().isoformat()
                }
                issues_data.append(issue_data)
            
            if not issues_data:
                print("⚠️ 저장할 이슈가 없습니다.")
                return True
            
            # issues 테이블에 저장
            result = self.supabase_manager.client.table('issues').insert(issues_data).execute()
            
            if result.data:
                print(f"✅ {len(result.data)}개 이슈 저장 완료")
                
                # issue_articles 테이블에 연결 정보 저장
                self.save_issue_articles_connections(clusters, result.data)
                return True
            else:
                print("❌ 이슈 저장 실패")
                return False
                
        except Exception as e:
            print(f"❌ 이슈 저장 실패: {str(e)}")
            return False
    
    def save_issue_articles_connections(self, clusters: List[Cluster], saved_issues: List[Dict]) -> bool:
        """issue_articles 테이블에 연결 정보 저장"""
        try:
            print("🔗 이슈-기사 연결 정보 저장 중...")
            
            connections = []
            for i, cluster in enumerate(clusters):
                if i >= len(saved_issues) or not cluster.articles:
                    continue
                
                issue_id = saved_issues[i]['id']
                for article in cluster.articles:
                    connection = {
                        "issue_id": issue_id,
                        "article_id": article.id
                    }
                    connections.append(connection)
            
            if connections:
                result = self.supabase_manager.client.table('issue_articles').insert(connections).execute()
                if result.data:
                    print(f"✅ {len(result.data)}개 연결 정보 저장 완료")
                    return True
            
            print("⚠️ 연결 정보가 없습니다.")
            return True
            
        except Exception as e:
            print(f"❌ 연결 정보 저장 실패: {str(e)}")
            return False
    
    def run_improved_clustering(self, start_date: str, end_date: str) -> bool:
        """개선된 클러스터링 실행"""
        try:
            print("🎯 개선된 이슈 클러스터링 시작")
            print("="*60)
            
            # 1. 기사 조회
            articles = self.fetch_articles_by_date_range(start_date, end_date)
            if not articles:
                return False
            
            # 2. 직접 클러스터링 (제목 기반, 배치 불필요)
            print(f"🔄 {len(articles)}개 기사 제목 기반 클러스터링 중...")
            clusters = self.cluster_with_precise_llm(articles)
            if not clusters:
                print("❌ 클러스터링 실패")
                return False
            
            # 3. 클러스터 품질 검증
            validated_clusters = self.validate_clusters(clusters)
            if not validated_clusters:
                print("❌ 검증된 클러스터가 없습니다.")
                return False
            
            # 4. 이슈 번호 할당
            numbered_clusters = self.assign_issue_numbers(validated_clusters)
            
            # 5. 결과 분석
            print(f"\n📊 최종 결과:")
            print("="*60)
            for i, cluster in enumerate(numbered_clusters, 1):
                bias_counts = self.count_media_bias(cluster)
                print(f"이슈 {i:2d}: {cluster.name}")
                print(f"        기사 수: {len(cluster.articles)}개 (좌:{bias_counts['left']}, 중:{bias_counts['center']}, 우:{bias_counts['right']})")
                print(f"        키워드: {', '.join(cluster.keywords)}")
                print()
            
            # 6. 데이터베이스 저장
            save_success = self.save_to_issues_table(numbered_clusters)
            if not save_success:
                print("❌ 데이터베이스 저장 실패")
                return False
            
            print(f"✅ 개선된 클러스터링 완료!")
            print(f"📈 {len(articles)}개 기사 → {len(numbered_clusters)}개 명확한 이슈")
            return True
            
        except Exception as e:
            print(f"❌ 클러스터링 실패: {str(e)}")
            return False

def main():
    """메인 함수"""
    print("🎯 개선된 LLM 기반 이슈 클러스터링")
    print("🚨 '및' 문제 해결 버전")
    print("="*60)
    
    try:
        # 날짜 범위 입력
        print("\n📅 클러스터링할 날짜 범위를 선택하세요 (KST 기준)")
        print("예: 0910~0920, 0901~0930, 0915~0915")
        
        while True:
            date_input = input("\n날짜 범위를 입력하세요: ").strip()
            if not date_input:
                print("❌ 날짜 범위를 입력해주세요.")
                continue
            
            try:
                clustering = ImprovedIssueClustering()
                start_date, end_date = clustering.parse_date_range(date_input)
                break
            except ValueError as e:
                print(f"❌ {e}")
                continue
        
        # 개선된 클러스터링 실행
        success = clustering.run_improved_clustering(start_date, end_date)
        
        if success:
            print("\n🎉 개선된 클러스터링 성공!")
            print("✅ 각 이슈는 명확한 단일 주제를 가집니다")
        else:
            print("\n❌ 클러스터링 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()
