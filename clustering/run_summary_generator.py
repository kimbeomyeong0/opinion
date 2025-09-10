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
        """이슈의 title, subtitle, views 데이터를 조회합니다."""
        try:
            response = self.supabase.client.table('issues').select(
                'id, title, subtitle, left_view, center_view, right_view'
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
        left_view = issue_data.get('left_view', '')
        center_view = issue_data.get('center_view', '')
        right_view = issue_data.get('right_view', '')
        
        prompt = f"""다음 이슈에 대한 좌파와 우파 두 관점을 분석하여 정확히 150자 이내의 강렬하고 논리적인 요약문을 작성해주세요.

이슈 제목: {title}
이슈 부제목: {subtitle}

좌파 관점: {left_view}
우파 관점: {right_view}

요구사항:
1. 이슈의 구체적 핵심을 강조하여 시작
2. 좌파와 우파 간의 대립/갈등/합의 관계를 명확히 드러내기
3. 논리적 흐름과 긴장감 있는 문장 구조로 작성
4. 단순 나열이 아닌 두 관점 간의 상호작용 표현
5. 정치적 갈등의 본질과 쟁점을 생생하게 전달
6. 반드시 150자 이내로 완성된 문장으로 작성
7. 인용부호나 특수 기호 사용 금지

요약문:"""
        
        return prompt
    
    def generate_summary(self, issue_data):
        """GPT-4o Mini를 사용하여 summary를 생성합니다."""
        try:
            prompt = self.create_prompt(issue_data)
            
            response = self.client.chat.completions.create(
                model=self.MODEL_NAME,
                messages=[
                    {"role": "system", "content": "당신은 정치 이슈를 분석하고 요약하는 전문가입니다. 객관적이고 균형 잡힌 시각으로 세 가지 관점을 종합하여 요약합니다."},
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

def main():
    """메인 함수"""
    print("📊 Module 5: Summary Generator 시작")
    print("=" * 50)
    
    generator = SummaryGenerator()
    generator.process_all_issues()

if __name__ == "__main__":
    main()
