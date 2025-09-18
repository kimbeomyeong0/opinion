#!/usr/bin/env python3
"""
이슈 요약 생성 스크립트
- 클러스터 내 대표 기사 3개 선정 (centroid 기반 cosine similarity)
- LLM을 활용한 이슈 종합 요약 생성 (300자 내외)
- OpenAI GPT-4o-mini 모델 사용
"""

import sys
import os
import json
import numpy as np
from typing import List, Dict, Any, Tuple
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
    from rich.progress import Progress, SpinnerColumn, TextColumn
except ImportError as e:
    print(f"❌ 필요한 라이브러리가 설치되지 않았습니다: {e}")
    print("pip install openai rich scikit-learn")
    sys.exit(1)

console = Console()


class IssueSummaryGenerator:
    """이슈 요약 생성 클래스"""
    
    def __init__(self):
        """초기화"""
        # Supabase 연결
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
        
        console.print("✅ IssueSummaryGenerator 초기화 완료")
    
    def get_issue_articles(self, issue_id: str) -> List[Dict[str, Any]]:
        """특정 이슈의 모든 기사 조회 (embedding 포함)"""
        try:
            console.print(f"🔍 이슈 {issue_id}의 기사 조회 중...")
            
            all_articles = []
            page_size = 1000
            offset = 0
            
            while True:
                result = self.supabase_manager.client.table('articles').select(
                    'id, title, content, media_id, embedding, published_at'
                ).eq('issue_id', issue_id).not_.is_('embedding', 'null').not_.is_('content', 'null').range(
                    offset, offset + page_size - 1
                ).execute()
                
                if not result.data:
                    break
                
                all_articles.extend(result.data)
                
                if len(result.data) < page_size:
                    break
                
                offset += page_size
                console.print(f"📄 페이지 조회 중... {len(all_articles)}개 수집됨")
            
            console.print(f"✅ 이슈 {issue_id}: {len(all_articles)}개 기사 조회 완료")
            return all_articles
            
        except Exception as e:
            console.print(f"❌ 기사 조회 실패: {str(e)}")
            return []
    
    def select_representative_articles(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """클러스터 centroid와 가장 유사한 대표 기사 3개 선정"""
        try:
            if len(articles) < 3:
                console.print(f"⚠️ 기사 수가 부족합니다 ({len(articles)}개). 모든 기사를 사용합니다.")
                return articles
            
            console.print(f"🎯 {len(articles)}개 기사 중 대표 기사 3개 선정 중...")
            
            # 임베딩 벡터 추출
            embeddings = []
            valid_articles = []
            
            for article in articles:
                if article.get('embedding') and article.get('content'):
                    try:
                        # embedding이 문자열인 경우 JSON으로 파싱
                        if isinstance(article['embedding'], str):
                            embedding = json.loads(article['embedding'])
                        else:
                            embedding = article['embedding']
                        
                        embeddings.append(embedding)
                        valid_articles.append(article)
                    except (json.JSONDecodeError, TypeError) as e:
                        console.print(f"⚠️ 임베딩 파싱 실패: {article['id']}")
                        continue
            
            if len(valid_articles) < 3:
                console.print(f"⚠️ 유효한 기사 수가 부족합니다 ({len(valid_articles)}개)")
                return valid_articles
            
            # numpy 배열로 변환
            embeddings_array = np.array(embeddings)
            
            # centroid 계산 (평균 벡터)
            centroid = np.mean(embeddings_array, axis=0)
            
            # 각 기사와 centroid 간의 cosine similarity 계산
            similarities = cosine_similarity([centroid], embeddings_array)[0]
            
            # 유사도가 높은 순으로 정렬하여 상위 3개 선택
            top_indices = np.argsort(similarities)[::-1][:3]
            
            representative_articles = [valid_articles[i] for i in top_indices]
            
            console.print("✅ 대표 기사 3개 선정 완료:")
            for i, article in enumerate(representative_articles, 1):
                similarity_score = similarities[top_indices[i-1]]
                console.print(f"  {i}. {article['title'][:50]}... (유사도: {similarity_score:.3f})")
            
            return representative_articles
            
        except Exception as e:
            console.print(f"❌ 대표 기사 선정 실패: {str(e)}")
            # 실패 시 처음 3개 기사 반환
            return articles[:3] if len(articles) >= 3 else articles
    
    def generate_summary_with_llm(self, articles: List[Dict[str, Any]]) -> str:
        """LLM을 활용한 이슈 종합 요약 생성"""
        try:
            # 기사 내용 정리
            articles_text = ""
            for i, article in enumerate(articles, 1):
                content = article.get('content', '')[:1000]  # 내용 길이 제한
                articles_text += f"기사 {i}:\n{content}\n\n"
            
            prompt = f"""위 세 기사를 모두 고르게 반영해, 이슈 전체를 설명하는 요약문을 작성하라.

{articles_text}

요구사항:
1. 300자 내외로 작성
2. 사건의 배경과 경과, 관련 인물들의 입장과 갈등, 현재 진행 상황을 자연스럽게 서술
3. 중립적이고 설명형 톤으로 작성
4. 특정 기사 하나에 치우치지 말고 공통 핵심을 종합
5. 불필요한 수식어나 감정적 표현 제거
6. 태그나 특수 기호 없이 자연스러운 문장으로 작성

예시:
"○○ 사건과 관련하여 ××의 의혹이 제기되면서 정치권에서 논란이 일고 있다. △△은 이에 대해 강하게 반발하며 □□을 요구하고 있는 반면, ◇◇은 ▽▽라는 입장을 보이고 있다. 현재 이 사안을 둘러싸고 여야 간 공방이 계속되고 있는 상황이다."

요약문만 출력해주세요:"""

            # OpenAI API 호출
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "당신은 한국 정치 뉴스 전문 요약 작성자입니다. 객관적이고 균형잡힌 시각으로 이슈를 종합 분석해주세요."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,  # 일관성을 위해 낮은 온도
                max_tokens=200
            )
            
            summary = response.choices[0].message.content.strip()
            
            # 따옴표 제거
            summary = summary.strip('"\'')
            
            # 300자 제한 확인
            if len(summary) > 300:
                summary = summary[:297] + "..."
            
            console.print(f"✅ 생성된 요약 ({len(summary)}자): {summary}")
            return summary
            
        except Exception as e:
            console.print(f"❌ LLM 요약 생성 실패: {str(e)}")
            return self._fallback_summary_generation(articles)
    
    def _fallback_summary_generation(self, articles: List[Dict[str, Any]]) -> str:
        """LLM 실패 시 백업 요약 생성"""
        try:
            # 기사 제목들로 간단한 요약 생성
            titles = [article.get('title', '') for article in articles if article.get('title')]
            
            if not titles:
                return "정치 이슈에 대한 논의가 계속되고 있다."
            
            # 공통 키워드 추출 (간단한 방식)
            all_text = ' '.join(titles)
            
            # 기본 요약문 생성
            summary = f"정치권에서 {len(articles)}개 언론사가 보도한 이슈에 대한 논의가 진행 중이다."
            
            return summary[:300]
            
        except Exception as e:
            console.print(f"❌ 백업 요약 생성 실패: {str(e)}")
            return "정치 이슈에 대한 논의가 계속되고 있다."
    
    def update_issue_summary(self, issue_id: str, summary: str) -> bool:
        """이슈 테이블의 issue_summary 컬럼 업데이트"""
        try:
            result = self.supabase_manager.client.table('issues').update({
                'issue_summary': summary
            }).eq('id', issue_id).execute()
            
            if result.data:
                console.print(f"✅ 이슈 {issue_id} 요약 업데이트 완료")
                return True
            else:
                console.print(f"❌ 이슈 {issue_id} 요약 업데이트 실패")
                return False
                
        except Exception as e:
            console.print(f"❌ 데이터베이스 업데이트 실패: {str(e)}")
            return False
    
    def process_issue(self, issue_id: str) -> bool:
        """단일 이슈 처리: 대표 기사 선정 → 요약 생성 → 업데이트"""
        try:
            console.print(f"\n🔄 이슈 {issue_id} 처리 시작")
            console.print("=" * 50)
            
            # 1. 이슈의 모든 기사 조회
            articles = self.get_issue_articles(issue_id)
            
            if not articles:
                console.print(f"⚠️ 이슈 {issue_id}: 기사 데이터 없음 - 건너뜀")
                return False
            
            # 2. 대표 기사 3개 선정
            representative_articles = self.select_representative_articles(articles)
            
            if not representative_articles:
                console.print(f"⚠️ 이슈 {issue_id}: 대표 기사 선정 실패 - 건너뜀")
                return False
            
            # 3. LLM 요약 생성
            summary = self.generate_summary_with_llm(representative_articles)
            
            # 4. 데이터베이스 업데이트
            success = self.update_issue_summary(issue_id, summary)
            
            if success:
                console.print(f"🎯 이슈 {issue_id} 처리 완료")
                return True
            else:
                console.print(f"❌ 이슈 {issue_id} 처리 실패")
                return False
                
        except Exception as e:
            console.print(f"❌ 이슈 {issue_id} 처리 중 오류: {str(e)}")
            return False


def update_all_issue_summaries():
    """모든 이슈의 요약을 생성하고 업데이트"""
    try:
        console.print("🔄 모든 이슈 요약 업데이트 시작")
        console.print("=" * 50)
        
        # 요약 생성기 초기화
        generator = IssueSummaryGenerator()
        
        # 모든 이슈 조회
        issues = generator.supabase_manager.client.table('issues').select('id, title').execute()
        
        if not issues.data:
            console.print("❌ 이슈 데이터를 찾을 수 없습니다.")
            return
        
        console.print(f"📰 총 {len(issues.data)}개 이슈 발견")
        
        # 진행률 표시
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            
            task = progress.add_task("이슈 요약 생성 중...", total=len(issues.data))
            
            success_count = 0
            
            for i, issue in enumerate(issues.data, 1):
                issue_id = issue['id']
                issue_title = issue['title']
                
                progress.update(task, description=f"처리 중: {issue_title[:30]}...")
                
                # 이슈 처리
                if generator.process_issue(issue_id):
                    success_count += 1
                
                progress.update(task, advance=1)
        
        console.print(f"\n🎯 업데이트 완료: {success_count}/{len(issues.data)}개 이슈 요약 생성")
        
    except Exception as e:
        console.print(f"❌ 전체 이슈 요약 업데이트 실패: {str(e)}")


def main():
    """메인 함수 - 이슈 요약 생성 실행"""
    try:
        console.print("🧪 이슈 요약 생성 스크립트")
        console.print("=" * 50)
        
        # 모든 이슈 요약 업데이트 실행
        update_all_issue_summaries()
        
    except Exception as e:
        console.print(f"❌ 실행 실패: {str(e)}")


if __name__ == "__main__":
    main()
