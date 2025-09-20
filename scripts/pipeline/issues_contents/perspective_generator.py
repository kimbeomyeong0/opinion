#!/usr/bin/env python3
"""
이슈별 좌파/우파 관점 추출 스크립트
- 각 이슈의 성향별 기사들을 분석하여 관점 추출
- GPT-4o 모델 사용으로 고품질 분석
- 구조화된 5단계 프롬프트로 객관적 분석
- 토큰 제한 고려한 효율적 기사 압축
"""

import sys
import os
import json
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from sklearn.metrics.pairwise import cosine_similarity
import warnings
warnings.filterwarnings('ignore')

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, project_root)

# 필요한 라이브러리 import
try:
    from openai import OpenAI
    from utils.supabase_manager import SupabaseManager
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
except ImportError as e:
    print(f"❌ 필요한 라이브러리가 설치되지 않았습니다: {e}")
    print("pip install openai rich scikit-learn")
    sys.exit(1)

console = Console()


class PerspectiveGenerator:
    """좌파/우파 관점 추출 클래스"""
    
    def __init__(self):
        """초기화"""
        # Supabase 연결
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
        
        # OpenAI 클라이언트 초기화 (GPT-4o 사용)
        try:
            self.openai_client = OpenAI()
            console.print("✅ OpenAI 클라이언트 초기화 완료 (GPT-4o)")
        except Exception as e:
            console.print(f"❌ OpenAI 클라이언트 초기화 실패: {str(e)}")
            raise Exception("OpenAI 연결 실패")
        
        console.print("✅ PerspectiveGenerator 초기화 완료")
    
    def get_articles_by_bias(self, issue_id: str, bias_type: str) -> List[Dict[str, Any]]:
        """특정 이슈의 특정 성향 기사들 조회"""
        try:
            console.print(f"🔍 이슈 {issue_id}의 {bias_type} 성향 기사 조회 중...")
            
            # articles와 media_outlets 조인하여 조회
            query = """
            SELECT a.id, a.title, a.content, a.media_id, a.embedding, a.published_at, m.bias
            FROM articles a
            JOIN media_outlets m ON a.media_id = m.id
            WHERE a.issue_id = %s AND m.bias = %s AND a.content IS NOT NULL
            """
            
            # Supabase에서는 직접 SQL 실행이 제한적이므로 단계별 조회
            # 1. 해당 이슈의 모든 기사 조회
            all_articles_result = self.supabase_manager.client.table('articles').select(
                'id, title, content, media_id, embedding, published_at'
            ).eq('issue_id', issue_id).not_.is_('content', 'null').execute()
            
            if not all_articles_result.data:
                console.print(f"⚠️ 이슈 {issue_id}에 기사가 없습니다.")
                return []
            
            # 2. 언론사 bias 정보 조회
            media_result = self.supabase_manager.client.table('media_outlets').select(
                'id, bias'
            ).execute()
            
            if not media_result.data:
                console.print("❌ 언론사 정보를 찾을 수 없습니다.")
                return []
            
            # 3. bias 매핑 생성
            bias_mapping = {outlet['id']: outlet['bias'] for outlet in media_result.data}
            
            # 4. 해당 성향 기사만 필터링
            filtered_articles = []
            for article in all_articles_result.data:
                media_id = article.get('media_id')
                if media_id and bias_mapping.get(media_id) == bias_type:
                    filtered_articles.append(article)
            
            console.print(f"✅ {bias_type} 성향 기사 {len(filtered_articles)}개 조회 완료")
            return filtered_articles
            
        except Exception as e:
            console.print(f"❌ {bias_type} 기사 조회 실패: {str(e)}")
            return []
    
    def select_representative_articles(self, articles: List[Dict[str, Any]], n: int = 3) -> List[Dict[str, Any]]:
        """임베딩 기반 대표 기사 선별"""
        try:
            if len(articles) <= n:
                console.print(f"📄 기사 수가 {n}개 이하이므로 모든 기사 사용")
                return articles
            
            console.print(f"🎯 {len(articles)}개 기사 중 대표 기사 {n}개 선별 중...")
            
            # 임베딩 벡터 추출
            embeddings = []
            valid_articles = []
            
            for article in articles:
                if article.get('embedding'):
                    try:
                        if isinstance(article['embedding'], str):
                            embedding = json.loads(article['embedding'])
                        else:
                            embedding = article['embedding']
                        
                        embeddings.append(embedding)
                        valid_articles.append(article)
                    except (json.JSONDecodeError, TypeError):
                        continue
            
            if len(valid_articles) <= n:
                console.print(f"⚠️ 유효한 임베딩을 가진 기사가 {n}개 이하")
                return valid_articles
            
            # numpy 배열로 변환
            embeddings_array = np.array(embeddings)
            
            # centroid 계산
            centroid = np.mean(embeddings_array, axis=0)
            
            # 각 기사와 centroid 간의 cosine similarity 계산
            similarities = cosine_similarity([centroid], embeddings_array)[0]
            
            # 유사도가 높은 순으로 상위 n개 선택
            top_indices = np.argsort(similarities)[::-1][:n]
            
            representative_articles = [valid_articles[i] for i in top_indices]
            
            console.print(f"✅ 대표 기사 {len(representative_articles)}개 선정 완료")
            for i, article in enumerate(representative_articles, 1):
                console.print(f"  {i}. {article['title'][:50]}...")
            
            return representative_articles
            
        except Exception as e:
            console.print(f"❌ 대표 기사 선정 실패: {str(e)}")
            # 실패 시 처음 n개 반환
            return articles[:n]
    
    def compress_articles_data(self, articles: List[Dict[str, Any]]) -> str:
        """기사 데이터 압축 (토큰 제한 고려)"""
        try:
            if len(articles) <= 5:
                # 5개 이하: 전체 내용 사용
                console.print("📝 기사 수가 적어 전체 내용 사용")
                compressed_data = ""
                for i, article in enumerate(articles, 1):
                    content = article.get('content', '')[:1000]  # 1000자 제한
                    compressed_data += f"기사 {i}: {article['title']}\n{content}\n\n"
                return compressed_data
            
            else:
                # 6개 이상: 대표 3개 + 나머지 요약
                console.print("📝 기사 압축: 대표 3개 + 나머지 요약")
                
                # 대표 기사 3개 선별
                representatives = self.select_representative_articles(articles, 3)
                
                # 대표 기사 전체 내용
                compressed_data = "=== 대표 기사 ===\n"
                for i, article in enumerate(representatives, 1):
                    content = article.get('content', '')[:1000]  # 1000자 제한
                    compressed_data += f"기사 {i}: {article['title']}\n{content}\n\n"
                
                # 나머지 기사들 제목만 추가
                remaining = [a for a in articles if a['id'] not in [r['id'] for r in representatives]]
                if remaining:
                    compressed_data += "=== 기타 기사 제목 ===\n"
                    for i, article in enumerate(remaining, 1):
                        compressed_data += f"{i}. {article['title']}\n"
                
                return compressed_data
                
        except Exception as e:
            console.print(f"❌ 기사 데이터 압축 실패: {str(e)}")
            # 백업: 제목만 사용
            return "\n".join([f"- {article['title']}" for article in articles[:10]])
    
    def generate_perspective_with_llm(self, compressed_data: str, bias_type: str) -> str:
        """GPT-4o를 활용한 성향별 관점 생성"""
        try:
            console.print(f"🤖 GPT-4o로 {bias_type} 성향 관점 생성 중...")
            
            bias_name = "진보" if bias_type == "left" else "보수"
            
            prompt = f"""다음 {bias_name} 성향 언론사들의 기사를 분석하여 객관적으로 답변해주세요:

{compressed_data}

분석 기준:
1. 이 성향이 이 이슈를 어떤 '문제'로 정의하는가?
2. 이 성향이 제시하는 '원인'은 무엇인가?
3. 이 성향이 강조하는 '중요한 측면'은 무엇인가?
4. 이 성향이 사용하는 '핵심 키워드'는 무엇인가?
5. 이 성향이 제시하는 '해결방향'은 무엇인가?

요구사항:
- 300자 내외로 작성
- 실제 기사에서 드러난 특징만 반영
- "이 성향은" 같은 표현 없이 자연스러운 문장으로 작성
- 중립적이고 객관적인 톤으로 서술

관점 요약:"""

            # OpenAI API 호출 (GPT-4o 사용)
            response = self.openai_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "당신은 한국 정치 뉴스 분석 전문가입니다. 객관적이고 균형잡힌 시각으로 언론사별 접근 방식의 차이를 분석해주세요."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,  # 일관성을 위해 낮은 온도
                max_tokens=250
            )
            
            perspective = response.choices[0].message.content.strip()
            
            # 따옴표 제거
            perspective = perspective.strip('"\'')
            
            # 300자 제한 확인
            if len(perspective) > 300:
                perspective = perspective[:297] + "..."
            
            console.print(f"✅ {bias_type} 관점 생성 완료 ({len(perspective)}자)")
            console.print(f"📝 결과: {perspective}")
            return perspective
            
        except Exception as e:
            console.print(f"❌ {bias_type} 관점 생성 실패: {str(e)}")
            return self._fallback_perspective_generation(bias_type)
    
    def _fallback_perspective_generation(self, bias_type: str) -> str:
        """LLM 실패 시 백업 관점 생성"""
        bias_name = "진보" if bias_type == "left" else "보수"
        return f"{bias_name} 성향 언론사들의 관점 분석 중 오류가 발생했습니다."
    
    def update_issue_perspectives(self, issue_id: str, left_perspective: str, right_perspective: str) -> bool:
        """이슈 테이블의 관점 컬럼 업데이트"""
        try:
            result = self.supabase_manager.client.table('issues').update({
                'left_perspective': left_perspective,
                'right_perspective': right_perspective
            }).eq('id', issue_id).execute()
            
            if result.data:
                console.print(f"✅ 이슈 {issue_id} 관점 업데이트 완료")
                return True
            else:
                console.print(f"❌ 이슈 {issue_id} 관점 업데이트 실패")
                return False
                
        except Exception as e:
            console.print(f"❌ 데이터베이스 업데이트 실패: {str(e)}")
            return False
    
    def process_single_issue(self, issue_id: str) -> bool:
        """단일 이슈의 좌파/우파 관점 생성"""
        try:
            console.print(f"\n🔄 이슈 {issue_id} 관점 생성 시작")
            console.print("=" * 60)
            
            # 1. 좌파 기사 조회 및 관점 생성
            left_articles = self.get_articles_by_bias(issue_id, 'left')
            if len(left_articles) < 3:
                console.print(f"⚠️ 좌파 기사가 부족합니다 ({len(left_articles)}개)")
                left_perspective = "좌파 성향 기사가 부족하여 관점 분석이 제한됩니다."
            else:
                left_compressed = self.compress_articles_data(left_articles)
                left_perspective = self.generate_perspective_with_llm(left_compressed, 'left')
            
            # 2. 우파 기사 조회 및 관점 생성
            right_articles = self.get_articles_by_bias(issue_id, 'right')
            if len(right_articles) < 3:
                console.print(f"⚠️ 우파 기사가 부족합니다 ({len(right_articles)}개)")
                right_perspective = "우파 성향 기사가 부족하여 관점 분석이 제한됩니다."
            else:
                right_compressed = self.compress_articles_data(right_articles)
                right_perspective = self.generate_perspective_with_llm(right_compressed, 'right')
            
            # 3. 데이터베이스 업데이트
            success = self.update_issue_perspectives(issue_id, left_perspective, right_perspective)
            
            if success:
                console.print(f"🎯 이슈 {issue_id} 관점 생성 완료")
                return True
            else:
                console.print(f"❌ 이슈 {issue_id} 관점 생성 실패")
                return False
                
        except Exception as e:
            console.print(f"❌ 이슈 {issue_id} 처리 중 오류: {str(e)}")
            return False
    
    def process_all_issues(self) -> None:
        """모든 이슈의 관점을 생성하고 업데이트"""
        try:
            console.print("🔄 모든 이슈 관점 생성 시작")
            console.print("=" * 60)
            
            # 모든 이슈 조회
            issues = self.supabase_manager.client.table('issues').select('id, title').execute()
            
            if not issues.data:
                console.print("❌ 이슈 데이터를 찾을 수 없습니다.")
                return
            
            console.print(f"📰 총 {len(issues.data)}개 이슈 발견")
            
            # 진행률 표시
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                transient=True,
            ) as progress:
                
                task = progress.add_task("이슈 관점 생성 중...", total=len(issues.data))
                
                success_count = 0
                
                for i, issue in enumerate(issues.data, 1):
                    issue_id = issue['id']
                    issue_title = issue['title']
                    
                    progress.update(task, description=f"처리 중: {issue_title[:30]}...")
                    
                    # 이슈 처리
                    if self.process_single_issue(issue_id):
                        success_count += 1
                    
                    progress.update(task, advance=1)
            
            console.print(f"\n🎯 관점 생성 완료: {success_count}/{len(issues.data)}개 이슈")
            
        except Exception as e:
            console.print(f"❌ 전체 이슈 관점 생성 실패: {str(e)}")


def main():
    """메인 함수 - 이슈별 관점 생성 실행"""
    try:
        console.print("🧪 이슈별 좌파/우파 관점 생성 스크립트")
        console.print("=" * 60)
        
        # 관점 생성기 초기화
        generator = PerspectiveGenerator()
        
        # 모든 이슈 관점 생성 실행
        generator.process_all_issues()
        
    except Exception as e:
        console.print(f"❌ 실행 실패: {str(e)}")


if __name__ == "__main__":
    main()
