#!/usr/bin/env python3
"""
고급 클러스터링 파이프라인 스크립트
- HDBSCAN 클러스터링
- 키워드 기반 제목 생성
- 임베딩 기반 중복 통합
- 최종 이슈 저장
"""

import time
import numpy as np
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# 필요한 라이브러리 import

try:
    import umap
    import hdbscan
    from sklearn.metrics.pairwise import cosine_similarity
except ImportError:
    print("❌ 필요한 라이브러리가 설치되지 않았습니다.")
    print("pip install umap-learn hdbscan scikit-learn")
    exit(1)

from utils.supabase_manager import SupabaseManager


class AdvancedClusteringPipeline:
    """고급 클러스터링 파이프라인 클래스"""
    
    def __init__(self, batch_size: int = 100):
        """초기화"""
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
        
        self.batch_size = batch_size
        
        # UMAP 파라미터 (더 큰 클러스터 허용)
        self.umap_params = {
            'n_neighbors': 15,  # 5 → 15로 증가 (더 큰 클러스터)
            'n_components': 20,  # 10 → 20으로 증가
            'min_dist': 0.1,
            'metric': 'cosine',
            'random_state': 42
        }
        
        # HDBSCAN 파라미터 (더 큰 클러스터 허용)
        self.hdbscan_params = {
            'min_cluster_size': 8,  # 3 → 8로 증가 (더 큰 클러스터)
            'min_samples': 5,  # 2 → 5로 증가
            'metric': 'euclidean',
            'cluster_selection_epsilon': 0.2  # 0.1 → 0.2로 증가
        }
        
        # 통합 임계값 (더 강화된 통합)
        self.merge_threshold = 0.5  # 0.6 → 0.5로 감소 (더 많은 통합)
        self.separate_threshold = 0.4  # 0.5 → 0.4로 감소
    
    def fetch_articles_by_category(self, category: str) -> List[Dict[str, Any]]:
        """카테고리별 기사 조회 (임베딩 포함, 임베딩 없는 기사도 포함)"""
        try:
            result = self.supabase_manager.client.table('articles').select(
                'id, title, lead_paragraph, political_category, embedding'
            ).eq('political_category', category).eq('is_preprocessed', True).execute()
            
            return result.data
        except Exception as e:
            print(f"❌ {category} 카테고리 기사 조회 실패: {str(e)}")
            return []
    
    
    def reduce_dimensions(self, embeddings: np.ndarray) -> np.ndarray:
        """UMAP 차원 축소"""
        try:
            reducer = umap.UMAP(**self.umap_params)
            reduced_embeddings = reducer.fit_transform(embeddings)
            return reduced_embeddings
        except Exception as e:
            print(f"❌ 차원 축소 실패: {str(e)}")
            return embeddings
    
    def perform_clustering(self, embeddings: np.ndarray) -> np.ndarray:
        """HDBSCAN 군집화"""
        try:
            clusterer = hdbscan.HDBSCAN(**self.hdbscan_params)
            cluster_labels = clusterer.fit_predict(embeddings)
            return cluster_labels
        except Exception as e:
            print(f"❌ 군집화 실패: {str(e)}")
            return np.array([-1] * len(embeddings))
    
    def extract_keywords_from_articles(self, articles: List[Dict[str, Any]]) -> List[str]:
        """기사들에서 핵심 키워드 추출"""
        if not articles:
            return []
        
        # 모든 제목과 리드문단 수집
        all_texts = []
        for article in articles:
            all_texts.append(article['title'])
            if article.get('lead_paragraph'):
                all_texts.append(article['lead_paragraph'])
        
        # 단어 추출 및 정제
        words = []
        for text in all_texts:
            text_words = text.replace('"', '').replace("'", '').split()
            words.extend(text_words)
        
        # 불용어 제거 및 필터링
        stop_words = {'관련', '이슈', '기사', '뉴스', '보도', '논란', '사태', '문제', '이야기', '소식', '전망', '분석', '평가', '검토', '논의', '협의', '결정', '발표', '공개', '확인', '조사', '수사', '재판', '판결', '기소', '구속', '체포', '수사', '조사', '확인', '발표', '공개', '결정', '논의', '협의', '평가', '검토', '분석', '전망', '소식', '이야기', '이슈', '문제', '사태', '논란', '뉴스', '기사', '보도'}
        
        # 빈도수 계산
        word_freq = {}
        for word in words:
            word = word.strip('.,!?()[]{}"\'')
            if len(word) > 1 and word not in stop_words and not word.isdigit():
                word_freq[word] = word_freq.get(word, 0) + 1
        
        # 상위 키워드 반환 (빈도순, 더 많은 키워드 추출)
        top_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:15]
        return [word for word, freq in top_words if freq > 1]
    
    def create_keyword_based_title(self, articles: List[Dict[str, Any]]) -> str:
        """키워드 기반 제목 생성 (5-10개 키워드 사용)"""
        if not articles:
            return "미분류 이슈"
        
        keywords = self.extract_keywords_from_articles(articles)
        
        # 핵심 키워드 추출 (5-10개)
        if len(keywords) >= 10:
            top_keywords = keywords[:10]  # 최대 10개
        elif len(keywords) >= 5:
            top_keywords = keywords[:len(keywords)]  # 5개 이상이면 모두 사용
        else:
            top_keywords = keywords  # 5개 미만이면 있는 만큼
        
        # 키워드 기반 제목 생성
        if len(top_keywords) >= 5:
            # 5개 이상의 키워드가 있으면 핵심 키워드들을 조합
            # 상위 3개는 주요 키워드로, 나머지는 보조 키워드로
            main_keywords = top_keywords[:3]
            sub_keywords = top_keywords[3:8] if len(top_keywords) > 3 else []
            
            title_parts = []
            title_parts.extend(main_keywords)
            
            # 보조 키워드 중 중요한 것들 추가 (최대 2개)
            if sub_keywords:
                title_parts.extend(sub_keywords[:2])
            
            return " ".join(title_parts) + " 관련 이슈"
        elif len(top_keywords) >= 2:
            # 2-4개 키워드가 있으면 모두 사용
            return " ".join(top_keywords) + " 관련 이슈"
        elif len(top_keywords) == 1:
            # 하나의 키워드만 있으면 단독
            return f"{top_keywords[0]} 관련 이슈"
        else:
            # 키워드가 없으면 기사 수로
            return f"{len(articles)}개 기사 클러스터"
    
    def calculate_title_similarity(self, title1: str, title2: str) -> float:
        """제목 유사도 계산 (키워드 기반)"""
        # 제목에서 키워드 추출
        keywords1 = set(title1.replace('관련 이슈', '').replace('클러스터', '').split())
        keywords2 = set(title2.replace('관련 이슈', '').replace('클러스터', '').split())
        
        # 공통 키워드 계산
        common_keywords = keywords1.intersection(keywords2)
        total_keywords = keywords1.union(keywords2)
        
        if len(total_keywords) == 0:
            return 0.0
        
        similarity = len(common_keywords) / len(total_keywords)
        return similarity
    
    def group_similar_titles(self, clusters: List[Dict[str, Any]]) -> List[List[int]]:
        """비슷한 제목끼리 그룹핑"""
        groups = []
        used_indices = set()
        
        for i, cluster in enumerate(clusters):
            if i in used_indices:
                continue
            
            # 현재 클러스터와 유사한 클러스터들 찾기
            similar_group = [i]
            used_indices.add(i)
            
            for j, other_cluster in enumerate(clusters):
                if j in used_indices:
                    continue
                
                similarity = self.calculate_title_similarity(
                    cluster['title'], other_cluster['title']
                )
                
                if similarity >= 0.15:  # 15% 이상 유사하면 그룹에 추가 (더욱 강화된 통합)
                    similar_group.append(j)
                    used_indices.add(j)
            
            groups.append(similar_group)
        
        return groups
    
    def calculate_embedding_similarity(self, articles1: List[Dict[str, Any]], articles2: List[Dict[str, Any]]) -> float:
        """두 클러스터의 기사들 간 임베딩 유사도 계산 (저장된 임베딩 사용)"""
        try:
            import json
            
            # 저장된 임베딩 추출 및 파싱
            embeddings1 = []
            embeddings2 = []
            
            for article in articles1:
                if article.get('embedding'):
                    try:
                        # JSON 문자열을 파싱하여 리스트로 변환
                        embedding_data = json.loads(article['embedding'])
                        embeddings1.append(embedding_data)
                    except:
                        continue
            
            for article in articles2:
                if article.get('embedding'):
                    try:
                        # JSON 문자열을 파싱하여 리스트로 변환
                        embedding_data = json.loads(article['embedding'])
                        embeddings2.append(embedding_data)
                    except:
                        continue
            
            if len(embeddings1) == 0 or len(embeddings2) == 0:
                return 0.0
            
            embeddings1 = np.array(embeddings1)
            embeddings2 = np.array(embeddings2)
            
            # 각 클러스터의 평균 임베딩 계산
            avg_embedding1 = np.mean(embeddings1, axis=0)
            avg_embedding2 = np.mean(embeddings2, axis=0)
            
            # 코사인 유사도 계산
            similarity = cosine_similarity([avg_embedding1], [avg_embedding2])[0][0]
            
            return float(similarity)
            
        except Exception as e:
            print(f"❌ 임베딩 유사도 계산 실패: {str(e)}")
            return 0.0
    
    def merge_similar_clusters(self, clusters: List[Dict[str, Any]], groups: List[List[int]]) -> List[Dict[str, Any]]:
        """유사한 클러스터들 통합"""
        merged_clusters = []
        
        for group in groups:
            if len(group) == 1:
                # 그룹에 클러스터가 하나만 있으면 그대로 유지
                merged_clusters.append(clusters[group[0]])
            else:
                # 여러 클러스터가 있으면 임베딩 유사도로 통합 여부 결정
                print(f"  🔍 {len(group)}개 클러스터 그룹 검토 중...")
                
                # 첫 번째 클러스터를 기준으로 시작
                merged_cluster = clusters[group[0]].copy()
                merged_articles = merged_cluster['articles'].copy()
                
                # 나머지 클러스터들과 유사도 계산
                for i in range(1, len(group)):
                    current_cluster = clusters[group[i]]
                    
                    # 임베딩 유사도 계산
                    similarity = self.calculate_embedding_similarity(
                        merged_articles, current_cluster['articles']
                    )
                    
                    print(f"    📊 유사도: {similarity:.3f}")
                    
                    if similarity >= self.merge_threshold:
                        # 통합
                        merged_articles.extend(current_cluster['articles'])
                        print(f"    ✅ 통합: {current_cluster['title']}")
                    else:
                        # 분리 (별도 클러스터로 유지)
                        merged_clusters.append(current_cluster)
                        print(f"    ❌ 분리: {current_cluster['title']}")
                
                # 통합된 클러스터 업데이트
                merged_cluster['articles'] = merged_articles
                merged_cluster['title'] = self.create_keyword_based_title(merged_articles)
                merged_clusters.append(merged_cluster)
        
        return merged_clusters
    
    def categorize_articles_by_bias(self, articles: List[Dict[str, Any]]) -> Tuple[int, int, int]:
        """기사를 성향별로 분류하고 숫자 반환"""
        total_articles = len(articles)
        
        # 균등 분류 (실제로는 성향 분석 모델 사용)
        left_count = total_articles // 3
        center_count = total_articles // 3
        right_count = total_articles - left_count - center_count
        
        return left_count, center_count, right_count
    
    def save_cluster_to_issues(self, cluster_articles: List[Dict[str, Any]], cluster_id: int) -> Optional[str]:
        """클러스터를 issues 테이블에 저장"""
        try:
            # 이슈 제목 생성
            issue_title = self.create_keyword_based_title(cluster_articles)
            
            # 성향별 분류 (숫자로 반환)
            left_count, center_count, right_count = self.categorize_articles_by_bias(cluster_articles)
            total_count = len(cluster_articles)
            
            # issues 테이블에 저장
            issue_data = {
                'title': issue_title,
                'left_source': str(left_count),
                'center_source': str(center_count),
                'right_source': str(right_count),
                'source': str(total_count),
                'created_at': datetime.now().isoformat()
            }
            
            result = self.supabase_manager.client.table('issues').insert(issue_data).execute()
            issue_id = result.data[0]['id']
            
            # issue_articles 테이블에 저장
            issue_articles_data = []
            for article in cluster_articles:
                issue_articles_data.append({
                    'issue_id': issue_id,
                    'article_id': article['id']
                })
            
            if issue_articles_data:
                self.supabase_manager.client.table('issue_articles').insert(issue_articles_data).execute()
            
            return issue_id
            
        except Exception as e:
            print(f"❌ 클러스터 저장 실패: {str(e)}")
            return None
    
    def process_category(self, category: str) -> Dict[str, Any]:
        """카테고리별 고급 클러스터링 처리"""
        print(f"📊 {category} 카테고리 처리 시작...")
        
        # 1. 기사 조회
        articles = self.fetch_articles_by_category(category)
        if not articles:
            print(f"❌ {category} 카테고리에 처리할 기사가 없습니다.")
            return {'success': False, 'clusters': 0}
        
        print(f"  📰 조회된 기사: {len(articles):,}개")
        
        # 2. 임베딩 처리 (저장된 임베딩만 사용)
        print(f"  🔄 임베딩 처리 중...")
        
        # 임베딩이 있는 기사만 필터링
        articles_with_embedding = []
        for article in articles:
            if article.get('embedding'):
                articles_with_embedding.append(article)
        
        print(f"    📊 임베딩 있는 기사: {len(articles_with_embedding)}개")
        
        if len(articles_with_embedding) == 0:
            print(f"❌ {category} 임베딩이 있는 기사가 없습니다. 먼저 generate_embeddings.py를 실행하세요.")
            return {'success': False, 'clusters': 0}
        
        # 임베딩 배열 생성 (vector 타입 처리)
        embeddings = []
        for article in articles_with_embedding:
            embedding_data = article['embedding']
            
            # vector 타입은 이미 리스트 형태로 반환됨
            if isinstance(embedding_data, list):
                embeddings.append(embedding_data)
            elif isinstance(embedding_data, str):
                # 문자열인 경우 파싱 시도
                try:
                    # JSON 형태의 문자열인지 확인
                    import json
                    embedding_list = json.loads(embedding_data)
                    embeddings.append(embedding_list)
                except:
                    try:
                        # 리스트 형태의 문자열인지 확인
                        embedding_list = eval(embedding_data)
                        embeddings.append(embedding_list)
                    except:
                        print(f"❌ 임베딩 파싱 실패: {article['id']}")
                        continue
            else:
                print(f"❌ 알 수 없는 임베딩 타입: {type(embedding_data)}")
                continue
        
        if len(embeddings) == 0:
            print(f"❌ {category} 유효한 임베딩이 없습니다.")
            return {'success': False, 'clusters': 0}
        
        embeddings = np.array(embeddings)
        print(f"    📊 임베딩 배열 형태: {embeddings.shape}")
        print(f"    📊 임베딩 차원: {embeddings.shape[1] if len(embeddings.shape) > 1 else 'N/A'}")
        
        # 임베딩이 있는 기사들만 처리
        articles = articles_with_embedding
        
        # 4. 차원 축소
        print(f"  📉 차원 축소 중...")
        reduced_embeddings = self.reduce_dimensions(embeddings)
        
        # 5. 군집화
        print(f"  🎯 군집화 중...")
        cluster_labels = self.perform_clustering(reduced_embeddings)
        
        # 6. 클러스터별로 기사들 그룹핑
        unique_clusters = np.unique(cluster_labels)
        clusters = []
        
        for cluster_id in unique_clusters:
            if cluster_id == -1:  # 노이즈 스킵
                continue
            
            cluster_mask = cluster_labels == cluster_id
            cluster_articles = [articles[i] for i in range(len(articles)) if cluster_mask[i]]
            
            if len(cluster_articles) >= 5:  # 최소 5개 기사 이상인 클러스터만
                cluster_title = self.create_keyword_based_title(cluster_articles)
                clusters.append({
                    'id': cluster_id,
                    'title': cluster_title,
                    'articles': cluster_articles,
                    'size': len(cluster_articles)
                })
        
        print(f"  📊 초기 클러스터: {len(clusters)}개")
        
        # 7. 비슷한 제목끼리 그룹핑
        print(f"  🔍 비슷한 제목 그룹핑 중...")
        groups = self.group_similar_titles(clusters)
        print(f"  📊 제목 그룹: {len(groups)}개")
        
        # 8. 임베딩 기반 통합
        print(f"  🔄 임베딩 기반 통합 중...")
        merged_clusters = self.merge_similar_clusters(clusters, groups)
        print(f"  📊 최종 클러스터: {len(merged_clusters)}개")
        
        # 9. 클러스터별로 저장
        saved_clusters = 0
        for cluster in merged_clusters:
            issue_id = self.save_cluster_to_issues(cluster['articles'], cluster['id'])
            if issue_id:
                saved_clusters += 1
        
        print(f"  ✅ {category} 완료: {saved_clusters}개 이슈 생성")
        return {'success': True, 'clusters': saved_clusters}
    
    def run_full_pipeline(self, categories: Optional[List[str]] = None) -> bool:
        """전체 고급 클러스터링 파이프라인 실행"""
        try:
            print("=" * 60)
            print("🎯 고급 클러스터링 파이프라인 시작")
            print("=" * 60)
            
            # 처리할 카테고리 결정
            if categories is None:
                categories = ['국회/정당', '행정부', '사법/검찰', '외교/안보', '정책/경제사회', '선거', '지역정치']
            
            total_clusters = 0
            start_time = time.time()
            
            for category in categories:
                result = self.process_category(category)
                if result['success']:
                    total_clusters += result['clusters']
            
            # 최종 결과
            total_time = time.time() - start_time
            print(f"\n🎉 고급 클러스터링 완료!")
            print(f"✅ 총 생성된 이슈: {total_clusters}개")
            print(f"⏱️  총 소요시간: {total_time/60:.1f}분")
            
            return total_clusters > 0
            
        except Exception as e:
            print(f"❌ 고급 클러스터링 파이프라인 실패: {str(e)}")
            return False


def main():
    """메인 함수"""
    try:
        # 고급 클러스터링 파이프라인 실행
        pipeline = AdvancedClusteringPipeline(batch_size=50)
        success = pipeline.run_full_pipeline()
        
        if success:
            print(f"\n✅ 고급 클러스터링 완료!")
        else:
            print(f"\n❌ 고급 클러스터링 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")


if __name__ == "__main__":
    main()
