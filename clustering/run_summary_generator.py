#!/usr/bin/env python3
"""
Module 5: Summary Generator
이슈의 title, subtitle, left_view, center_view, right_view를 기반으로 
세 관점의 관계와 이슈 본질을 담은 summary를 생성합니다.
"""

import os
import sys
import re
from openai import OpenAI

# 상위 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_manager import SupabaseManager

class SummaryGenerator:
    def __init__(self):
        self.supabase = SupabaseManager()
        self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        # 하드코딩된 설정
        self.MODEL_NAME = "gpt-4o-mini"
        self.MAX_TOKENS = 1000
        self.TEMPERATURE = 0.7
        self.MAX_SUMMARY_LENGTH = 150  # 150자 제한
        
    def fetch_issue_data(self, issue_id):
        """이슈의 title, subtitle, background, views 데이터를 조회합니다."""
        try:
            response = self.supabase.client.table('issues').select(
                'id, title, subtitle, background, left_view, center_view, right_view'
            ).eq('id', issue_id).execute()
            
            if not response.data:
                print(f"❌ 이슈 {issue_id}를 찾을 수 없습니다.")
                return None
                
            return response.data[0]
            
        except Exception as e:
            print(f"❌ 이슈 데이터 조회 실패: {e}")
            return None
    
    def create_prompt(self, issue_data):
        """Summary 생성을 위한 프롬프트를 생성합니다."""
        title = issue_data.get('title', '')
        subtitle = issue_data.get('subtitle', '')
        background = issue_data.get('background', '')
        left_view = issue_data.get('left_view', '')
        center_view = issue_data.get('center_view', '')
        right_view = issue_data.get('right_view', '')
        
        prompt = f"""다음 이슈를 파이트클럽의 브래드피트 같은 파이터가 극강 비속어(씨발, 좆같은, 개새끼, 병신새끼, 지랄, 씨발놈 등)와 구어체로 150자 이내에 요약해줘. 아무것도 모르는 사람이 듣고도 바로 이해할 수 있게.

이슈 제목: {title}
이슈 부제목: {subtitle}

배경 정보: {background}

좌파 관점: {left_view}
우파 관점: {right_view}

요구사항:
1. 파이트클럽 브래드피트 같은 파이터 톤으로 작성 (씨발, 좆같은, 개새끼, 병신새끼, 지랄, 씨발놈, 좆도 모르는, 개판, 씨발 등 극강 비속어 포함)
2. 구어체로 자연스럽게 (야, 지, 거, 뭐, 뭔, 어, 이거 등)
3. 시작을 터프하게 다양하게 (절대 "야, 이거 진짜 개판이야" 반복 금지):
   - "뭔 개소리인지 모르겠어 씨발놈아"
   - "이거 진짜 좆같은 일이네"
   - "씨발, 이거 뭔 상황이야"
   - "야 이거 완전 개판이잖아"
   - "이거 뭔 좆같은 일이야"
   - "씨발놈들아 이거 뭔 상황이냐"
   - "이거 진짜 씨발 개판이네"
   - "야 이거 뭔 개소리야"
   - "이거 완전 좆같은 일이야"
   - "씨발 이거 뭔 상황이냐"
4. 정치 초보자도 이해할 수 있게 쉬운 말로 설명
5. 이슈가 뭔지, 왜 싸우는지, 누가 뭘 주장하는지 명확하게
6. GPT스러운 정중한 말투 절대 금지 - 진짜 파이터처럼
7. 150자 이내로 작성 (절대 초과 금지)
8. 한자, 정치용어 최대한 피하고 쉬운 말로
9. 좌파는 뭘 원하고, 우파는 뭘 원하는지 명확히
10. 왜 이게 문제인지, 왜 싸우는지 핵심만
11. 시작 문장은 위 예시 중에서 랜덤하게 선택하되, 절대 "야, 이거 진짜 개판이야"는 사용하지 마세요

요약문:"""
        
        return prompt
    
    def generate_summary(self, issue_data):
        """GPT-4o Mini를 사용하여 summary를 생성합니다."""
        try:
            prompt = self.create_prompt(issue_data)
            
            response = self.client.chat.completions.create(
                model=self.MODEL_NAME,
                messages=[
                    {"role": "system", "content": "당신은 파이트클럽의 브래드피트 같은 극강 파이터입니다. 정치 이슈를 아무것도 모르는 사람도 이해할 수 있게 극강 비속어(씨발, 좆같은, 개새끼, 병신새끼, 지랄, 씨발놈, 좆도 모르는, 개판, 씨발 등)와 구어체(야, 지, 거, 뭐, 뭔, 어, 이거 등)를 사용해서 직설적으로 설명합니다. GPT스러운 정중한 말투는 절대 사용하지 않고, 시작 문장은 다양하게 사용하세요. 절대 '야, 이거 진짜 개판이야'로 시작하지 마세요."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.MAX_TOKENS,
                temperature=self.TEMPERATURE
            )
            
            summary = response.choices[0].message.content.strip()
            
            # 길이 검증 (참고용)
            if len(summary) > self.MAX_SUMMARY_LENGTH:
                print(f"⚠️  요약문이 {self.MAX_SUMMARY_LENGTH}자를 초과했습니다: {len(summary)}자")
            
            return summary
            
        except Exception as e:
            print(f"❌ Summary 생성 실패: {e}")
            return None
    
    def update_issues_table(self, issue_id, summary):
        """생성된 summary를 issues 테이블에 업데이트합니다."""
        try:
            response = self.supabase.client.table('issues').update({
                'summary': summary
            }).eq('id', issue_id).execute()
            
            if response.data:
                print(f"✅ Summary 업데이트 완료")
                return True
            else:
                print(f"❌ Summary 업데이트 실패")
                return False
                
        except Exception as e:
            print(f"❌ Summary 업데이트 실패: {e}")
            return False
    
    def process_issue(self, issue_id):
        """단일 이슈의 summary를 생성하고 업데이트합니다."""
        print(f"📋 이슈 {issue_id} 처리 중...")
        
        # 1. 이슈 데이터 조회
        issue_data = self.fetch_issue_data(issue_id)
        if not issue_data:
            return False
        
        # 2. Summary 생성
        summary = self.generate_summary(issue_data)
        if not summary:
            return False
        
        print(f"📝 생성된 Summary: {summary}")
        
        # 3. DB 업데이트
        success = self.update_issues_table(issue_id, summary)
        return success
    
    def process_all_issues(self):
        """모든 이슈의 summary를 생성합니다."""
        try:
            # 모든 이슈 조회
            response = self.supabase.client.table('issues').select('id').execute()
            
            if not response.data:
                print("❌ 처리할 이슈가 없습니다.")
                return
            
            issue_ids = [issue['id'] for issue in response.data]
            total_issues = len(issue_ids)
            
            print(f"🎯 총 {total_issues}개 이슈의 Summary 생성 시작...")
            print()
            
            success_count = 0
            
            for i, issue_id in enumerate(issue_ids, 1):
                print(f"[{i}/{total_issues}] 이슈 {issue_id}")
                
                success = self.process_issue(issue_id)
                if success:
                    success_count += 1
                
                print("-" * 50)
            
            print(f"🎉 Summary 생성 완료!")
            print(f"✅ 성공: {success_count}/{total_issues}")
            print(f"❌ 실패: {total_issues - success_count}/{total_issues}")
            
        except Exception as e:
            print(f"❌ 전체 처리 실패: {e}")

def test_single_issue():
    """단일 이슈 테스트 함수"""
    print("=" * 60)
    print("🧪 단일 이슈 Summary 테스트 모드")
    print("=" * 60)
    
    try:
        # 생성기 초기화
        generator = SummaryGenerator()
        
        # 첫 번째 이슈 조회
        result = generator.supabase.client.table('issues').select('id, title').limit(1).execute()
        
        if not result.data:
            print("❌ 테스트할 이슈가 없습니다.")
            return
        
        issue_id = result.data[0]['id']
        issue_title = result.data[0]['title']
        
        print(f"📋 테스트 이슈: {issue_title} (ID: {issue_id})")
        
        # 단일 이슈 처리
        success = generator.process_issue(issue_id)
        
        if success:
            print("\n✅ 단일 이슈 Summary 테스트 완료!")
            
            # 결과 확인
            result = generator.supabase.client.table('issues').select(
                'summary'
            ).eq('id', issue_id).execute()
            
            if result.data:
                summary = result.data[0].get('summary', 'N/A')
                print(f"\n📊 생성된 Summary ({len(summary)}자): {summary}")
        else:
            print("\n❌ 단일 이슈 Summary 테스트 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")

def main():
    """메인 함수"""
    print("📊 Module 5: Summary Generator 시작")
    print("=" * 50)
    
    generator = SummaryGenerator()
    generator.process_all_issues()

if __name__ == "__main__":
    import sys
    
    # 명령행 인수로 테스트 모드 확인
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_single_issue()
    else:
        main()
