#!/usr/bin/env python3
"""
이슈 제목 생성기
- 기존 이슈의 기사 제목들을 분석하여 더 명확한 이슈 제목 생성
- '및' 문제 해결 및 단일 주제 강화
"""

import sys
import os
import json
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
class IssueTitleData:
    """이슈 제목 생성용 데이터 클래스"""
    issue_id: str
    current_title: str
    article_titles: List[str]
    article_count: int

class IssueTitleGenerator:
    """이슈 제목 생성기 클래스"""
    
    def __init__(self):
        """초기화"""
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
        
        if OPENAI_AVAILABLE:
            self.openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        else:
            self.openai_client = None
    
    def fetch_issues_with_articles(self) -> List[IssueTitleData]:
        """이슈와 관련 기사 제목들을 조회"""
        try:
            print("📋 이슈 및 관련 기사 조회 중...")
            
            # 이슈 조회
            issues_result = self.supabase_manager.client.table('issues').select('id, title').execute()
            if not issues_result.data:
                print("❌ 이슈가 없습니다.")
                return []
            
            issues_data = []
            for issue in issues_result.data:
                issue_id = issue['id']
                current_title = issue['title']
                
                # 해당 이슈의 기사 제목들 조회
                articles_result = self.supabase_manager.client.table('issue_articles').select(
                    'article_id, articles(title)'
                ).eq('issue_id', issue_id).execute()
                
                article_titles = []
                for item in articles_result.data:
                    if item['articles'] and item['articles'].get('title'):
                        article_titles.append(item['articles']['title'])
                
                if article_titles:
                    issues_data.append(IssueTitleData(
                        issue_id=issue_id,
                        current_title=current_title,
                        article_titles=article_titles,
                        article_count=len(article_titles)
                    ))
            
            print(f"✅ {len(issues_data)}개 이슈 조회 완료")
            return issues_data
            
        except Exception as e:
            print(f"❌ 이슈 조회 실패: {str(e)}")
            return []
    
    def create_title_generation_prompt(self, issue_data: IssueTitleData) -> str:
        """이슈 제목 생성 프롬프트 생성"""
        titles_text = ""
        for i, title in enumerate(issue_data.article_titles, 1):
            titles_text += f"{i}. {title}\n"
        
        prompt = f"""
다음은 하나의 정치 이슈에 속한 기사들의 제목들입니다. 이 기사들이 다루는 **핵심 정치 이슈**를 파악하여 명확하고 구체적인 이슈 제목을 생성해주세요.

현재 이슈 제목: "{issue_data.current_title}"
기사 수: {issue_data.article_count}개

기사 제목들:
{titles_text}

🚨 **중요한 제목 생성 원칙** 🚨

1. **단일 주제 원칙**: 반드시 **하나의 구체적인 정치 이슈**만 표현
   - ❌ "사법개혁 및 검찰 수사" → 2개 이슈가 섞임
   - ✅ "사법개혁" 또는 "검찰 수사" → 각각 별도 이슈

2. **구체적 사건/정책 중심**: 추상적이지 않고 구체적인 사건이나 정책으로 표현
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

5. **'및' 금지**: 절대로 여러 이슈를 하나로 합치지 마세요
   - 각 이슈는 명확한 단일 주제여야 함

6. **핵심 키워드 포함**: 가장 중요한 정치적 키워드들을 포함
   - 인물명, 정책명, 사건명 등

🎯 **목표**: '및', '그리고', '등' 없이 **명확한 단일 주제명**

🎨 **제목 스타일 가이드**:
- **진중함**: 전문적이고 신뢰감 있는 톤 유지
- **명확성**: 핵심 내용을 직접적이고 명확하게 전달
- **권위성**: 정치 뉴스다운 무게감과 전문성
- **간결성**: 불필요한 수식어나 과장된 표현 지양
- **정치적 맥락**: "정치적 갈등", "사법부 개혁", "정치적 파장" 등

**금지사항**:
- ❌ 의문문 형태 ("?", "과연", "정말", "가능성은?")
- ❌ 과도한 호기심 유발 ("진실은?", "반응은?", "드러날")
- ❌ 클릭베이트 스타일 ("충격", "놀라운", "깜짝")

예시:
- ❌ "한미 관세 협상" → ✅ "한미 관세 갈등과 협상 전개"
- ❌ "내란 전담 재판부 설치" → ✅ "내란 전담 재판부 설치와 정치적 갈등"
- ❌ "조희대 사퇴 요구" → ✅ "조희대 대법원장 사퇴 압박과 정치적 파장"

응답 형식 (JSON):
{{
    "new_title": "진중하고 전문적인 이슈 제목",
    "reasoning": "이 제목을 선택한 이유",
    "confidence": "high|medium|low",
    "keywords": ["핵심키워드1", "핵심키워드2", "핵심키워드3"]
}}
"""
        return prompt
    
    def generate_issue_title(self, issue_data: IssueTitleData) -> Optional[Dict[str, Any]]:
        """단일 이슈의 제목 생성"""
        if not self.openai_client:
            print("❌ OpenAI 클라이언트가 없습니다.")
            return None
        
        try:
            print(f"🎯 이슈 제목 생성 중: {issue_data.current_title}")
            
            # 프롬프트 생성
            prompt = self.create_title_generation_prompt(issue_data)
            
            # LLM 호출
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "당신은 정치 뉴스 분석 전문가입니다. 기사 제목들을 분석하여 명확한 단일 주제이면서도 진중하고 전문적인 이슈 제목을 생성해주세요. 절대로 여러 이슈를 하나로 합치지 마세요."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,  # 일관성을 위해 온도 낮춤
                max_tokens=1000
            )
            
            # 응답 파싱
            response_text = response.choices[0].message.content
            print("📝 LLM 응답 받음")
            
            # JSON 파싱 시도
            try:
                json_start = response_text.find('{')
                json_end = response_text.rfind('}') + 1
                json_text = response_text[json_start:json_end]
                
                result = json.loads(json_text)
                
                new_title = result.get('new_title', '')
                reasoning = result.get('reasoning', '')
                confidence = result.get('confidence', 'medium')
                keywords = result.get('keywords', [])
                
                # '및' 문제 검증
                warning_words = ['및', '그리고', '또한', '아울러', '동시에', '함께']
                has_warning = any(word in new_title for word in warning_words)
                
                if has_warning:
                    print(f"⚠️ 의심 제목: '{new_title}' - 복수 이슈 가능성")
                else:
                    print(f"✅ 명확한 제목: '{new_title}'")
                
                return {
                    'new_title': new_title,
                    'reasoning': reasoning,
                    'confidence': confidence,
                    'keywords': keywords,
                    'has_warning': has_warning
                }
                
            except json.JSONDecodeError as e:
                print(f"❌ JSON 파싱 실패: {str(e)}")
                print(f"응답 내용: {response_text[:500]}...")
                return None
                
        except Exception as e:
            print(f"❌ 제목 생성 실패: {str(e)}")
            return None
    
    def update_issue_title(self, issue_id: str, new_title: str) -> bool:
        """이슈 제목 업데이트"""
        try:
            result = self.supabase_manager.client.table('issues').update({
                'title': new_title
            }).eq('id', issue_id).execute()
            
            if result.data:
                print(f"✅ 이슈 제목 업데이트 완료: {new_title}")
                return True
            else:
                print(f"❌ 이슈 제목 업데이트 실패: {result.error}")
                return False
                
        except Exception as e:
            print(f"❌ 이슈 제목 업데이트 오류: {str(e)}")
            return False
    
    def run_title_generation(self) -> bool:
        """이슈 제목 생성 실행"""
        try:
            print("🎯 이슈 제목 생성기 시작")
            print("="*60)
            
            # 1. 이슈 및 기사 제목 조회
            issues_data = self.fetch_issues_with_articles()
            if not issues_data:
                return False
            
            # 2. 각 이슈별 제목 생성
            generated_titles = []
            for issue_data in issues_data:
                print(f"\n📝 이슈 처리 중: {issue_data.current_title}")
                print(f"   기사 수: {issue_data.article_count}개")
                
                result = self.generate_issue_title(issue_data)
                if result:
                    generated_titles.append({
                        'issue_id': issue_data.issue_id,
                        'current_title': issue_data.current_title,
                        'new_title': result['new_title'],
                        'reasoning': result['reasoning'],
                        'confidence': result['confidence'],
                        'keywords': result['keywords'],
                        'has_warning': result['has_warning']
                    })
            
            # 3. 결과 출력
            print(f"\n📊 제목 생성 결과:")
            print("="*60)
            for i, title_data in enumerate(generated_titles, 1):
                print(f"{i:2d}. 기존: {title_data['current_title']}")
                print(f"    신규: {title_data['new_title']}")
                print(f"    신뢰도: {title_data['confidence']}")
                print(f"    키워드: {', '.join(title_data['keywords'])}")
                if title_data['has_warning']:
                    print(f"    ⚠️ 복수 이슈 의심")
                print()
            
            # 4. 사용자 확인
            print("🔄 이슈 제목을 업데이트하시겠습니까? (y/n): ", end="")
            user_input = input().strip().lower()
            
            if user_input == 'y':
                # 5. 데이터베이스 업데이트
                update_count = 0
                for title_data in generated_titles:
                    if self.update_issue_title(title_data['issue_id'], title_data['new_title']):
                        update_count += 1
                
                print(f"\n🎉 이슈 제목 업데이트 완료!")
                print(f"📈 {len(generated_titles)}개 중 {update_count}개 업데이트 성공")
                return True
            else:
                print("❌ 업데이트 취소됨")
                return False
                
        except Exception as e:
            print(f"❌ 제목 생성 실패: {str(e)}")
            return False

def main():
    """메인 함수"""
    print("🎯 이슈 제목 생성기")
    print("="*60)
    
    try:
        # 제목 생성기 초기화
        generator = IssueTitleGenerator()
        
        # 제목 생성 실행
        success = generator.run_title_generation()
        
        if success:
            print("\n🎉 이슈 제목 생성 완료!")
        else:
            print("\n❌ 이슈 제목 생성 실패")
            
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()
