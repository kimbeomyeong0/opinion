#!/usr/bin/env python3
"""
뉴스 기사 클러스터 제목 생성 스크립트
- 기사 제목들을 분석하여 대표적인 클러스터 제목 생성
- OpenAI GPT-4o-mini 활용
- 30자 이내 압축형 헤드라인 생성
"""

import sys
import os
import json
import re
from typing import List, Dict, Any
from collections import Counter
import warnings
warnings.filterwarnings('ignore')

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# 필요한 라이브러리 import
try:
    from openai import OpenAI
except ImportError:
    print("❌ OpenAI 라이브러리가 설치되지 않았습니다.")
    print("pip install openai")
    sys.exit(1)


class IssueTitleGenerator:
    """이슈 제목 생성 클래스"""
    
    def __init__(self):
        """초기화"""
        try:
            self.openai_client = OpenAI()
            print("✅ OpenAI 클라이언트 초기화 완료")
        except Exception as e:
            print(f"⚠️ OpenAI 클라이언트 초기화 실패: {str(e)}")
            print("⚠️ 백업 시스템으로 제목 생성합니다.")
            self.openai_client = None
    
    def extract_keywords(self, titles: List[str]) -> Dict[str, List[str]]:
        """기사 제목들에서 핵심 키워드 추출"""
        try:
            # 모든 제목을 하나의 텍스트로 합치기
            all_text = ' '.join(titles)
            
            # 한국어 키워드 추출 (2글자 이상)
            korean_words = re.findall(r'[가-힣]{2,}', all_text)
            
            # 빈도수 계산
            word_counts = Counter(korean_words)
            
            # 정치 관련 핵심 키워드 분류
            keywords = {
                'person': [],      # 인물
                'position': [],    # 직책/기관
                'event': [],       # 사건/의혹
                'reaction': []     # 반응/태도
            }
            
            # 키워드 분류 로직
            for word, count in word_counts.most_common(20):  # 상위 20개만
                if count >= 2:  # 2번 이상 언급된 키워드만
                    if self._is_person(word):
                        keywords['person'].append(word)
                    elif self._is_position(word):
                        keywords['position'].append(word)
                    elif self._is_event(word):
                        keywords['event'].append(word)
                    elif self._is_reaction(word):
                        keywords['reaction'].append(word)
            
            return keywords
            
        except Exception as e:
            print(f"❌ 키워드 추출 실패: {str(e)}")
            return {'person': [], 'position': [], 'event': [], 'reaction': []}
    
    def _is_person(self, word: str) -> bool:
        """인물명 판단"""
        person_patterns = [
            r'^[가-힣]{2,3}$',  # 2-3글자 한글 이름
            r'^[가-힣]+대통령$',  # 대통령
            r'^[가-힣]+의원$',   # 의원
            r'^[가-힣]+장$',     # 장
            r'^[가-힣]+총리$',   # 총리
            r'^[가-힣]+원장$'    # 원장
        ]
        return any(re.match(pattern, word) for pattern in person_patterns)
    
    def _is_position(self, word: str) -> bool:
        """직책/기관명 판단"""
        position_patterns = [
            r'.*부$',      # 부 (정부, 국방부 등)
            r'.*청$',      # 청 (검찰청, 국세청 등)
        r'.*원$',      # 원 (대법원, 헌법재판소 등)
            r'.*당$',      # 당 (민주당, 국민의힘 등)
            r'.*국$',      # 국 (국회, 국가 등)
            r'대법원장', '검찰청장', '국정원장', '경찰청장'
        ]
        return any(re.search(pattern, word) for pattern in position_patterns)
    
    def _is_event(self, word: str) -> bool:
        """사건/의혹 관련 키워드 판단"""
        event_patterns = [
            r'.*의혹$',    # 의혹
            r'.*사건$',    # 사건
            r'.*논란$',    # 논란
            r'.*특검$',    # 특검
            r'.*탄핵$',    # 탄핵
            r'.*구속$',    # 구속
            r'.*수사$',    # 수사
            r'.*청탁$',    # 청탁
            r'.*회동$',    # 회동
            r'.*만남$'     # 만남
        ]
        return any(re.search(pattern, word) for pattern in event_patterns)
    
    def _is_reaction(self, word: str) -> bool:
        """반응/태도 관련 키워드 판단"""
        reaction_patterns = [
            r'.*반발$',    # 반발
            r'.*비판$',    # 비판
            r'.*규탄$',    # 규탄
            r'.*압박$',    # 압박
            r'.*공방$',    # 공방
            r'.*대응$',    # 대응
            r'.*입장$',    # 입장
            r'.*발표$',    # 발표
            r'.*거부$',    # 거부
            r'.*수용$'     # 수용
        ]
        return any(re.search(pattern, word) for pattern in reaction_patterns)
    
    def generate_title_with_llm(self, titles: List[str], keywords: Dict[str, List[str]]) -> str:
        """LLM을 활용한 클러스터 제목 생성"""
        if self.openai_client is None:
            print("⚠️ OpenAI 클라이언트가 없어 백업 시스템을 사용합니다.")
            return self._fallback_title_generation(titles, keywords)
        
        try:
            # 제목들 텍스트 정리
            titles_text = '\n'.join([f"{i+1}. {title}" for i, title in enumerate(titles[:10])])  # 최대 10개만
            
            # 키워드 정리
            keyword_text = f"""
인물: {', '.join(keywords['person'][:3])}
직책/기관: {', '.join(keywords['position'][:3])}
사건/의혹: {', '.join(keywords['event'][:3])}
반응/태도: {', '.join(keywords['reaction'][:3])}
"""
            
            prompt = f"""다음 한국 정치 뉴스 기사 제목들을 분석하여 30자 이내의 압축형 헤드라인을 생성해주세요:

기사 제목들:
{titles_text}

추출된 키워드:
{keyword_text}

요구사항:
1. 30자 이내로 압축
2. 핵심 사건과 정치권 반응을 포함
3. 뉴스 헤드라인 스타일로 작성
4. 불필요한 수식어 제거
5. 핵심 키워드 중심으로 구성

예시:
- "조희대 회동 의혹…특검·탄핵 공방 격화"
- "권성동 구속에 與野 정치탄압 공방"
- "김건희 특검 수사 확대…국힘 강력 반발"

제목만 출력해주세요:"""

            # OpenAI API 호출
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "당신은 한국 정치 뉴스 헤드라인 전문가입니다. 간결하고 임팩트 있는 제목을 작성해주세요."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,  # 일관성을 위해 낮은 온도
                max_tokens=100
            )
            
            title = response.choices[0].message.content.strip()
            
            # 따옴표 제거
            title = title.strip('"\'')
            
            # 30자 제한 확인
            if len(title) > 30:
                title = title[:28] + ".."
            
            return title
            
        except Exception as e:
            print(f"❌ LLM 제목 생성 실패: {str(e)}")
            return self._fallback_title_generation(titles, keywords)
    
    def _fallback_title_generation(self, titles: List[str], keywords: Dict[str, List[str]]) -> str:
        """LLM 실패 시 백업 제목 생성"""
        try:
            # 핵심 키워드 조합
            person = keywords['person'][0] if keywords['person'] else ""
            event = keywords['event'][0] if keywords['event'] else "이슈"
            reaction = keywords['reaction'][0] if keywords['reaction'] else "논란"
            
            if person and event:
                title = f"{person} {event}…{reaction}"
            elif event:
                title = f"{event}…{reaction}"
            else:
                title = "정치 이슈 논란"
            
            # 30자 제한
            if len(title) > 30:
                title = title[:28] + ".."
            
            return title
            
        except Exception as e:
            print(f"❌ 백업 제목 생성 실패: {str(e)}")
            return "정치 이슈"
    
    def generate_title(self, articles: List[Dict[str, Any]]) -> str:
        """메인 함수: 기사 리스트로부터 클러스터 제목 생성"""
        try:
            if not articles:
                return "정치 이슈"
            
            # 제목 추출
            titles = [article.get('title', '') for article in articles if article.get('title')]
            
            if not titles:
                return "정치 이슈"
            
            print(f"📝 {len(titles)}개 기사 제목 분석 중...")
            
            # 1단계: 키워드 추출
            keywords = self.extract_keywords(titles)
            print(f"🔍 추출된 키워드: {keywords}")
            
            # 2단계: LLM 제목 생성
            title = self.generate_title_with_llm(titles, keywords)
            
            print(f"✅ 생성된 제목: {title} ({len(title)}자)")
            return title
            
        except Exception as e:
            print(f"❌ 제목 생성 실패: {str(e)}")
            return "정치 이슈"


def update_issues_titles():
    """issues 테이블의 모든 이슈 제목을 새로 생성된 제목으로 업데이트"""
    try:
        from utils.supabase_manager import SupabaseManager
        
        # Supabase 연결
        supabase = SupabaseManager()
        
        print("🔄 이슈 제목 업데이트 시작")
        print("=" * 50)
        
        # 이슈 생성기 초기화
        generator = IssueTitleGenerator()
        
        # 모든 이슈 조회
        issues = supabase.client.table('issues').select('id,title').execute()
        
        if not issues.data:
            print("❌ 이슈 데이터를 찾을 수 없습니다.")
            return
        
        print(f"📰 총 {len(issues.data)}개 이슈 발견")
        
        updated_count = 0
        
        for i, issue in enumerate(issues.data, 1):
            issue_id = issue['id']
            old_title = issue['title']
            
            # 해당 이슈의 기사들 조회
            articles = supabase.client.table('articles').select('title').eq('issue_id', issue_id).execute()
            
            if not articles.data:
                print(f"⚠️ 이슈 {i}: 기사 데이터 없음 - 건너뜀")
                continue
            
            article_titles = [{'title': article['title']} for article in articles.data]
            
            # 새 제목 생성
            new_title = generator.generate_title(article_titles)
            
            # 제목이 변경된 경우에만 업데이트
            if new_title != old_title:
                # 데이터베이스 업데이트
                result = supabase.client.table('issues').update({
                    'title': new_title
                }).eq('id', issue_id).execute()
                
                if result.data:
                    print(f"✅ 이슈 {i}: '{old_title}' → '{new_title}'")
                    updated_count += 1
                else:
                    print(f"❌ 이슈 {i}: 업데이트 실패")
            else:
                print(f"⏭️ 이슈 {i}: 제목 변경 없음 - '{old_title}'")
        
        print(f"\n🎯 업데이트 완료: {updated_count}개 이슈 제목 변경")
        
    except Exception as e:
        print(f"❌ 이슈 제목 업데이트 실패: {str(e)}")


def main():
    """메인 함수 - 이슈 제목 업데이트 실행"""
    try:
        print("🧪 이슈 제목 업데이트 스크립트")
        print("=" * 50)
        
        # 이슈 제목 업데이트 실행
        update_issues_titles()
        
    except Exception as e:
        print(f"❌ 실행 실패: {str(e)}")


if __name__ == "__main__":
    main()
