#!/usr/bin/env python3
"""
LLM 기반 클러스터링 실험 스크립트
merged_content를 기반으로 LLM을 사용하여 이슈를 군집화하는 실험
"""

import sys
import os
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
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
    merged_content: str
    media_id: str
    published_at: str

@dataclass
class Cluster:
    """클러스터 데이터 클래스"""
    id: str
    name: str
    description: str
    articles: List[Article]
    keywords: List[str]

class LLMClusteringExperiment:
    """LLM 기반 클러스터링 실험 클래스"""
    
    def __init__(self):
        """초기화"""
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
        
        if OPENAI_AVAILABLE:
            self.openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        else:
            self.openai_client = None
    
    def fetch_all_articles(self) -> List[Article]:
        """모든 기사 조회"""
        try:
            print("📡 모든 기사 조회 중...")
            
            result = self.supabase_manager.client.table('articles_cleaned').select(
                'article_id, merged_content, media_id, published_at'
            ).order('published_at', desc=True).execute()
            
            if not result.data:
                print("❌ 기사 데이터가 없습니다.")
                return []
            
            articles = []
            for item in result.data:
                article = Article(
                    id=item['article_id'],
                    title="",  # 제목 불필요
                    merged_content=item['merged_content'],
                    media_id=item['media_id'],
                    published_at=item['published_at']
                )
                articles.append(article)
            
            print(f"✅ {len(articles)}개 기사 조회 완료")
            return articles
            
        except Exception as e:
            print(f"❌ 기사 조회 실패: {str(e)}")
            return []
    
    def split_articles_into_batches(self, articles: List[Article], batch_size: int = 200) -> List[List[Article]]:
        """기사들을 배치로 분할"""
        batches = []
        for i in range(0, len(articles), batch_size):
            batch = articles[i:i + batch_size]
            batches.append(batch)
        return batches
    
    def create_clustering_prompt(self, articles: List[Article]) -> str:
        """클러스터링 프롬프트 생성"""
        articles_text = ""
        for i, article in enumerate(articles, 1):
            articles_text += f"{i}. {article.merged_content}\n\n"
        
        prompt = f"""
다음 정치 기사 내용들을 의미적으로 유사한 이슈 그룹으로 분류해주세요.

기사 내용:
{articles_text}

요구사항:
1. 같은 정치 이슈를 다루는 기사들을 하나의 그룹으로 묶어주세요
2. 각 그룹에 적절한 이름과 설명을 제공해주세요
3. 각 그룹의 핵심 키워드 3-5개를 추출해주세요
4. 그룹이 너무 많거나 적지 않도록 적절한 수준으로 분류해주세요

응답 형식 (JSON):
{{
    "clusters": [
        {{
            "name": "이슈 그룹 이름",
            "description": "이슈 그룹 설명",
            "keywords": ["키워드1", "키워드2", "키워드3"],
            "article_indices": [1, 3, 5]
        }}
    ]
}}
"""
        return prompt
    
    def cluster_with_llm(self, articles: List[Article]) -> Optional[List[Cluster]]:
        """LLM을 사용한 클러스터링"""
        if not self.openai_client:
            print("❌ OpenAI 클라이언트가 없습니다.")
            return None
        
        try:
            print("🤖 LLM 클러스터링 시작...")
            
            # 프롬프트 생성
            prompt = self.create_clustering_prompt(articles)
            
            # LLM 호출
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "당신은 정치 뉴스 분석 전문가입니다. 기사들을 의미적으로 유사한 이슈 그룹으로 정확하게 분류해주세요."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=4000
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
                    
                    cluster = Cluster(
                        id=f"cluster_{i+1}",
                        name=cluster_data.get('name', f'그룹 {i+1}'),
                        description=cluster_data.get('description', ''),
                        articles=cluster_articles,
                        keywords=cluster_data.get('keywords', [])
                    )
                    clusters.append(cluster)
                
                print(f"✅ {len(clusters)}개 클러스터 생성 완료")
                return clusters
                
            except json.JSONDecodeError as e:
                print(f"❌ JSON 파싱 실패: {str(e)}")
                print(f"응답 내용: {response_text[:500]}...")
                return None
                
        except Exception as e:
            print(f"❌ LLM 클러스터링 실패: {str(e)}")
            return None
    
    def analyze_clusters(self, clusters: List[Cluster]) -> None:
        """클러스터 분석 결과 출력"""
        print("\n" + "="*60)
        print("📊 LLM 클러스터링 결과 분석")
        print("="*60)
        
        for i, cluster in enumerate(clusters, 1):
            print(f"\n🎯 클러스터 {i}: {cluster.name}")
            print(f"📝 설명: {cluster.description}")
            print(f"🏷️  키워드: {', '.join(cluster.keywords)}")
            print(f"📰 기사 수: {len(cluster.articles)}개")
            
            print("📋 포함된 기사들:")
            for j, article in enumerate(cluster.articles, 1):
                print(f"  {j}. {article.merged_content[:50]}...")
            
            print("-" * 40)
    
    def generate_background(self, cluster: Cluster) -> str:
        """클러스터의 배경 정보 생성"""
        if not cluster.articles:
            return ""
        
        # 기사 내용의 앞부분을 배경 정보로 사용
        contents = [article.merged_content[:100] for article in cluster.articles[:3]]  # 최대 3개, 100자씩
        background = f"관련 기사 내용:\n"
        for i, content in enumerate(contents, 1):
            background += f"• {content}...\n"
        
        return background.strip()
    
    def save_to_issues_table(self, clusters: List[Cluster]) -> bool:
        """클러스터를 issues 테이블에 저장"""
        try:
            print(f"💾 {len(clusters)}개 이슈를 issues 테이블에 저장 중...")
            
            issues_data = []
            for cluster in clusters:
                if not cluster.articles:  # 기사가 없는 클러스터는 건너뛰기
                    continue
                
                issue_data = {
                    "date": datetime.now().date().isoformat(),
                    "title": cluster.name,
                    "summary": f"{len(cluster.articles)}개 기사로 구성된 이슈",
                    "subtitle": cluster.description,
                    "background": self.generate_background(cluster),
                    "source": len(cluster.articles),
                    "left_source": 0,
                    "center_source": 0,
                    "right_source": 0,
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
                    # articles_cleaned 테이블에서 cleaned_article_id 조회
                    cleaned_result = self.supabase_manager.client.table('articles_cleaned').select('id').eq('article_id', article.id).execute()
                    
                    if cleaned_result.data:
                        cleaned_article_id = cleaned_result.data[0]['id']
                    else:
                        print(f"⚠️ article_id {article.id}에 해당하는 cleaned_article_id를 찾을 수 없습니다.")
                        cleaned_article_id = None
                    
                    connection = {
                        "issue_id": issue_id,
                        "article_id": article.id,
                        "cleaned_article_id": cleaned_article_id
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
    
    def save_results(self, clusters: List[Cluster], filename: str = None) -> str:
        """결과를 JSON 파일로 저장 (백업용)"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"llm_clustering_results_{timestamp}.json"
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "total_clusters": len(clusters),
            "clusters": []
        }
        
        for cluster in clusters:
            cluster_data = {
                "id": cluster.id,
                "name": cluster.name,
                "description": cluster.description,
                "keywords": cluster.keywords,
                "article_count": len(cluster.articles),
                "articles": [
                    {
                        "id": article.id,
                        "title": article.title,
                        "merged_content": article.merged_content,
                        "media_id": article.media_id,
                        "published_at": article.published_at
                    }
                    for article in cluster.articles
                ]
            }
            results["clusters"].append(cluster_data)
        
        filepath = os.path.join(project_root, "experiments", filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"💾 백업 파일 저장 완료: {filepath}")
        return filepath
    
    def create_merge_prompt(self, all_clusters: List[List[Cluster]]) -> str:
        """배치 결과 통합을 위한 프롬프트 생성 (개선된 버전)"""
        clusters_text = ""
        cluster_id = 1
        
        for batch_idx, batch_clusters in enumerate(all_clusters, 1):
            clusters_text += f"\n=== 배치 {batch_idx} 결과 ===\n"
            for cluster in batch_clusters:
                clusters_text += f"{cluster_id}. {cluster.name}\n"
                clusters_text += f"   설명: {cluster.description}\n"
                clusters_text += f"   키워드: {', '.join(cluster.keywords)}\n"
                clusters_text += f"   기사 수: {len(cluster.articles)}개\n"
                # 기사 샘플 추가 (맥락 이해를 위해)
                sample_articles = cluster.articles[:3]  # 최대 3개 기사 샘플
                clusters_text += f"   기사 샘플:\n"
                for i, article in enumerate(sample_articles, 1):
                    clusters_text += f"     {i}. {article.merged_content[:100]}...\n"
                clusters_text += "\n"
                cluster_id += 1
        
        prompt = f"""
다음은 여러 배치로 나누어 클러스터링한 정치 뉴스 결과입니다. 의미적으로 유사한 이슈들을 정확하게 합쳐서 최종 이슈 목록을 만들어주세요.

클러스터링 결과:
{clusters_text}

분석 가이드라인:
1. **정치적 맥락 고려**: 같은 정치 이슈의 다른 표현들을 인식하세요
   - 예: "사법개혁"과 "사법부 독립"은 같은 이슈
   - 예: "정치자금"과 "부정수급"은 같은 이슈
   - 예: "한미관계"와 "미국 비자문제"는 같은 이슈

2. **시간적 연속성 고려**: 같은 이슈의 지속적 보도들을 인식하세요
   - 예: "특검법 개정" 관련 기사들이 여러 배치에 걸쳐 있을 수 있음

3. **의미적 유사성 판단**: 단순 키워드가 아닌 전체적 맥락으로 판단하세요
   - 기사 내용의 핵심 주제가 같은지 확인
   - 정치적 맥락과 배경이 유사한지 확인

4. **노이즈 식별**: 어떤 이슈에도 속하지 않는 독립적인 기사들도 고려하세요
   - 단발성 사건이나 특수한 상황
   - 다른 이슈들과 명확히 구분되는 내용

요구사항:
1. 의미적으로 유사한 이슈들을 하나로 합쳐주세요
2. 각 최종 이슈에 적절한 이름과 설명을 제공해주세요
3. 각 이슈의 핵심 키워드 3-5개를 추출해주세요
4. 합쳐진 이슈에 포함된 원본 클러스터 번호들을 기록해주세요
5. 독립적인 이슈는 그대로 유지하세요

응답 형식 (JSON):
{{
    "final_clusters": [
        {{
            "name": "최종 이슈 이름",
            "description": "최종 이슈 설명",
            "keywords": ["키워드1", "키워드2", "키워드3"],
            "merged_from": [1, 5, 8],
            "confidence": "high|medium|low"
        }}
    ]
}}
"""
        return prompt
    
    def merge_batch_clusters(self, all_clusters: List[List[Cluster]]) -> Optional[List[Cluster]]:
        """배치별 클러스터 결과들을 통합 (개선된 버전)"""
        if not self.openai_client:
            print("❌ OpenAI 클라이언트가 없습니다.")
            return None
        
        try:
            print("🔄 배치 결과 통합 중...")
            
            # 프롬프트 생성
            prompt = self.create_merge_prompt(all_clusters)
            
            # LLM 호출 (더 정교한 설정)
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "당신은 정치 뉴스 분석 전문가입니다. 여러 배치의 클러스터링 결과를 분석하여 의미적으로 유사한 이슈들을 정확하게 합쳐주세요. 정치적 맥락과 시간적 연속성을 고려하여 판단하세요."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,  # 더 일관된 결과를 위해 낮춤
                max_tokens=6000   # 더 많은 토큰 할당
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
                    
                    # 중복 기사 제거 (같은 기사가 여러 배치에 있을 수 있음)
                    unique_articles = []
                    seen_ids = set()
                    for article in merged_articles:
                        if article.id not in seen_ids:
                            unique_articles.append(article)
                            seen_ids.add(article.id)
                    
                    final_cluster = Cluster(
                        id=f"final_cluster_{i+1}",
                        name=cluster_data.get('name', f'통합 이슈 {i+1}'),
                        description=cluster_data.get('description', ''),
                        articles=unique_articles,
                        keywords=cluster_data.get('keywords', [])
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

    def run_batch_experiment(self, batch_size: int = 200) -> bool:
        """배치 처리 실험 실행"""
        try:
            print("🚀 LLM 기반 배치 클러스터링 실험 시작")
            print("="*60)
            
            # 1. 모든 기사 조회
            articles = self.fetch_all_articles()
            if not articles:
                return False
            
            # 2. 배치로 분할
            batches = self.split_articles_into_batches(articles, batch_size)
            print(f"📦 {len(articles)}개 기사를 {len(batches)}개 배치로 분할")
            
            # 3. 각 배치별 클러스터링
            all_clusters = []
            for i, batch in enumerate(batches, 1):
                print(f"\n🔄 배치 {i}/{len(batches)} 처리 중... ({len(batch)}개 기사)")
                batch_clusters = self.cluster_with_llm(batch)
                if batch_clusters:
                    all_clusters.append(batch_clusters)
                    print(f"✅ 배치 {i} 완료: {len(batch_clusters)}개 클러스터")
                else:
                    print(f"❌ 배치 {i} 실패")
            
            if not all_clusters:
                print("❌ 모든 배치 클러스터링 실패")
                return False
            
            # 4. 배치 결과 통합
            print(f"\n🔄 {len(all_clusters)}개 배치 결과 통합 중...")
            final_clusters = self.merge_batch_clusters(all_clusters)
            if not final_clusters:
                print("❌ 배치 통합 실패")
                return False
            
            # 5. 결과 분석
            self.analyze_clusters(final_clusters)
            
            # 6. issues 테이블에 저장
            save_success = self.save_to_issues_table(final_clusters)
            if not save_success:
                print("❌ issues 테이블 저장 실패")
                return False
            
            # 7. 백업 파일 저장
            self.save_results(final_clusters)
            
            print(f"\n✅ 배치 실험 완료! {len(articles)}개 기사 → {len(final_clusters)}개 최종 클러스터")
            print("📊 결과가 issues 테이블에 저장되었습니다.")
            return True
            
        except Exception as e:
            print(f"❌ 배치 실험 실패: {str(e)}")
            return False

    def run_experiment(self, article_limit: int = 30) -> bool:
        """기존 실험 실행 (하위 호환성)"""
        try:
            print("🚀 LLM 기반 클러스터링 실험 시작")
            print("="*60)
            
            # 1. 샘플 기사 조회
            articles = self.fetch_sample_articles(article_limit)
            if not articles:
                return False
            
            # 2. LLM 클러스터링
            clusters = self.cluster_with_llm(articles)
            if not clusters:
                return False
            
            # 3. 결과 분석
            self.analyze_clusters(clusters)
            
            # 4. issues 테이블에 저장
            save_success = self.save_to_issues_table(clusters)
            if not save_success:
                print("❌ issues 테이블 저장 실패")
                return False
            
            # 5. 백업 파일 저장
            self.save_results(clusters)
            
            print(f"\n✅ 실험 완료! {len(articles)}개 기사 → {len(clusters)}개 클러스터")
            print("📊 결과가 issues 테이블에 저장되었습니다.")
            return True
            
        except Exception as e:
            print(f"❌ 실험 실패: {str(e)}")
            return False

def main():
    """메인 함수"""
    print("🧪 LLM 기반 클러스터링 실험")
    print("="*60)
    
    try:
        # 실험 모드 선택
        print("\n실험 모드를 선택하세요:")
        print("1. 소규모 실험 (30-50개 기사)")
        print("2. 전체 기사 배치 처리 (522개 기사)")
        
        while True:
            mode_input = input("\n모드를 선택하세요 (1 또는 2): ").strip()
            if mode_input == "1":
                # 소규모 실험
                while True:
                    try:
                        limit_input = input("처리할 기사 수를 입력하세요 (기본값: 30): ").strip()
                        if not limit_input:
                            article_limit = 30
                            break
                        article_limit = int(limit_input)
                        if article_limit <= 0:
                            print("❌ 1 이상의 숫자를 입력해주세요.")
                            continue
                        break
                    except ValueError:
                        print("❌ 올바른 숫자를 입력해주세요.")
                        continue
                
                experiment = LLMClusteringExperiment()
                success = experiment.run_experiment(article_limit)
                break
                
            elif mode_input == "2":
                # 전체 기사 배치 처리
                batch_size_input = input("배치 크기를 입력하세요 (기본값: 200): ").strip()
                if not batch_size_input:
                    batch_size = 200
                else:
                    try:
                        batch_size = int(batch_size_input)
                        if batch_size <= 0:
                            print("❌ 1 이상의 숫자를 입력해주세요.")
                            continue
                    except ValueError:
                        print("❌ 올바른 숫자를 입력해주세요.")
                        continue
                
                experiment = LLMClusteringExperiment()
                success = experiment.run_batch_experiment(batch_size)
                break
                
            else:
                print("❌ 1 또는 2를 입력해주세요.")
                continue
        
        if success:
            print("\n🎉 실험 성공!")
        else:
            print("\n❌ 실험 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()
