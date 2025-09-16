#!/usr/bin/env python3
"""
기존 이슈들의 제목을 새로운 방식으로 업데이트하는 스크립트
- 키워드 추출 + LLM 조합으로 의미있는 제목 생성
"""

import time
from typing import List, Dict, Any
from utils.supabase_manager import SupabaseManager
from openai import OpenAI


class IssueTitleUpdater:
    """이슈 제목 업데이터 클래스"""
    
    def __init__(self):
        """초기화"""
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
        
        self.openai_client = OpenAI()
    
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
            # 간단한 토큰화
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
        
        # 상위 키워드 반환 (빈도순)
        top_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:10]
        return [word for word, freq in top_words if freq > 1]  # 2번 이상 나타난 키워드만
    
    def generate_meaningful_title_with_llm(self, articles: List[Dict[str, Any]], keywords: List[str]) -> str:
        """LLM을 활용한 의미있는 이슈 제목 생성"""
        try:
            if not articles:
                return "미분류 이슈"
            
            # 샘플 기사 제목들 수집 (최대 5개)
            sample_titles = [article['title'] for article in articles[:5]]
            
            # 키워드와 샘플 제목을 LLM에 전달
            prompt = f"""
다음은 같은 정치 이슈에 속한 기사들의 제목들입니다. 이 기사들의 공통 주제를 파악하고, 구체적이고 의미있는 이슈 제목을 생성해주세요.

핵심 키워드: {', '.join(keywords[:5])}

기사 제목들:
{chr(10).join([f"- {title}" for title in sample_titles])}

요구사항:
1. 25-35자로 적절한 길이 유지
2. 핵심 사건, 인물, 구체적 상황을 명시
3. "논란", "요구", "대립", "관련 이슈" 같은 모호한 표현 지양
4. 실제 정치적 맥락과 구체적 내용을 포함
5. 사건의 본질을 축약하지 말고 핵심을 드러내기
6. 구체적인 행위, 결정, 사건을 명시

예시:
❌ "이준석 관련 논란"
✅ "이준석 국민의힘 복당 신청과 당 내 갈등"

❌ "검찰 수사 요구"  
✅ "윤석열 대통령 검찰총장 임명 관련 수사 요구"

이슈 제목만 답변:
"""
            
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=50,
                temperature=0.3
            )
            
            title = response.choices[0].message.content.strip()
            # 따옴표 제거
            title = title.strip('"\'')
            return title
            
        except Exception as e:
            print(f"❌ LLM 제목 생성 실패: {str(e)}")
            # LLM 실패 시 키워드 기반 폴백
            if keywords:
                return f"{keywords[0]} 관련 이슈"
            else:
                return f"{len(articles)}개 기사 클러스터"
    
    def get_articles_for_issue(self, issue_id: str) -> List[Dict[str, Any]]:
        """이슈에 속한 기사들 조회"""
        try:
            # issue_articles에서 기사 ID들 조회
            issue_articles_result = self.supabase_manager.client.table('issue_articles').select('article_id').eq('issue_id', issue_id).execute()
            
            if not issue_articles_result.data:
                return []
            
            article_ids = [item['article_id'] for item in issue_articles_result.data]
            
            # 기사 정보 조회
            articles_result = self.supabase_manager.client.table('articles').select('id, title, lead_paragraph').in_('id', article_ids).execute()
            
            return articles_result.data
            
        except Exception as e:
            print(f"❌ 이슈 {issue_id} 기사 조회 실패: {str(e)}")
            return []
    
    def update_issue_title(self, issue_id: str, new_title: str) -> bool:
        """이슈 제목 업데이트"""
        try:
            result = self.supabase_manager.client.table('issues').update({
                'title': new_title
            }).eq('id', issue_id).execute()
            
            return len(result.data) > 0
            
        except Exception as e:
            print(f"❌ 이슈 {issue_id} 제목 업데이트 실패: {str(e)}")
            return False
    
    def process_all_issues(self) -> bool:
        """모든 이슈의 제목 업데이트"""
        try:
            print("=" * 60)
            print("🔄 이슈 제목 업데이트 시작")
            print("=" * 60)
            
            # 모든 이슈 조회
            issues_result = self.supabase_manager.client.table('issues').select('id, title').execute()
            
            if not issues_result.data:
                print("❌ 업데이트할 이슈가 없습니다.")
                return False
            
            total_issues = len(issues_result.data)
            updated_count = 0
            
            print(f"📊 총 {total_issues}개 이슈 처리 시작...")
            
            for i, issue in enumerate(issues_result.data):
                issue_id = issue['id']
                old_title = issue['title']
                
                print(f"\r🔄 진행률: {i+1}/{total_issues} | 업데이트: {updated_count}개", end="", flush=True)
                
                # 이슈에 속한 기사들 조회
                articles = self.get_articles_for_issue(issue_id)
                
                if not articles:
                    continue
                
                # 키워드 추출
                keywords = self.extract_keywords_from_articles(articles)
                
                # 새로운 제목 생성
                new_title = self.generate_meaningful_title_with_llm(articles, keywords)
                
                # 제목이 변경된 경우에만 업데이트
                if new_title != old_title:
                    if self.update_issue_title(issue_id, new_title):
                        updated_count += 1
                
                # API 제한 방지
                time.sleep(0.1)
            
            print(f"\n\n🎉 이슈 제목 업데이트 완료!")
            print(f"✅ 총 {total_issues}개 이슈 중 {updated_count}개 업데이트")
            
            return True
            
        except Exception as e:
            print(f"\n❌ 이슈 제목 업데이트 실패: {str(e)}")
            return False


def main():
    """메인 함수"""
    try:
        updater = IssueTitleUpdater()
        success = updater.process_all_issues()
        
        if success:
            print(f"\n✅ 이슈 제목 업데이트 완료!")
        else:
            print(f"\n❌ 이슈 제목 업데이트 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")


if __name__ == "__main__":
    main()
