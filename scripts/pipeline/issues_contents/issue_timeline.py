#!/usr/bin/env python3
"""
이슈 타임라인 생성 스크립트 (GPT-4o-mini만 사용)
- 이슈 요약에서 핵심 사건들을 시간순으로 추출
- issues 테이블의 issue_timeline 컬럼에 저장
"""

import sys
import os
import json
from typing import List, Dict, Any
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
    print("pip install openai rich")
    sys.exit(1)

console = Console()


class IssueTimelineGenerator:
    """이슈 타임라인 생성 클래스 (GPT-4o-mini만 사용)"""
    
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
        
        console.print("✅ IssueTimelineGenerator 초기화 완료")
    
    def check_issues_table(self):
        """issues 테이블의 issue_timeline 컬럼 확인"""
        try:
            console.print("📋 issues 테이블 확인 중...")
            
            result = self.supabase_manager.client.table('issues').select('id, issue_timeline').limit(1).execute()
            console.print("✅ issues 테이블과 issue_timeline 컬럼 확인 완료")
            return True
            
        except Exception as e:
            console.print(f"❌ issues 테이블 확인 실패: {str(e)}")
            console.print("⚠️ issues 테이블에 issue_timeline 컬럼이 있는지 확인해주세요.")
            return False
    
    def get_all_issues(self) -> List[Dict[str, Any]]:
        """모든 이슈와 요약 조회"""
        try:
            console.print("🔍 모든 이슈 조회 중...")
            
            result = self.supabase_manager.client.table('issues').select(
                'id, title, issue_summary'
            ).not_.is_('issue_summary', 'null').execute()
            
            console.print(f"✅ {len(result.data)}개 이슈 조회 완료")
            return result.data
            
        except Exception as e:
            console.print(f"❌ 이슈 조회 실패: {str(e)}")
            return []
    
    def extract_timeline_from_summary(self, issue_summary: str) -> str:
        """이슈 요약에서 시간순 타임라인 생성"""
        try:
            prompt = f"""다음 정치 이슈 요약을 분석하여, 이 이슈를 완전히 이해하기 위해 필요한 핵심 사건들을 시간순으로 8개 내외 추출해주세요.

{issue_summary}

분석 지침:
1. 이슈의 배경부터 현재까지 전체적인 흐름을 파악할 수 있는 사건들
2. 완전한 문장으로 구체적이고 명확하게 작성 (30-40자 내외)
3. 시간순으로 배열 (과거 → 현재)
4. "누가 무엇을 했다" 형태의 완전한 문장으로 작성
5. 의혹, 주장, 반박 등은 "~했다는 의혹이 제기되었다", "~라고 주장했다" 형태로 명확히 표현
6. 각 사건을 한 줄씩 나열 (번호나 기호 없이)

출력 형식:
사건1
사건2
사건3
...

예시:
이재명 대통령의 공직선거법 위반 사건이 발생했다
조희대가 대법원장으로 취임했다
조희대 대법원장이 이재명 사건에 개입했다는 의혹이 제기되었다
조희대 대법원장이 개입 의혹을 공식 부인했다
정청래 민주당 대표가 조 대법원장의 과거 행적을 지적했다
정청래가 조 대법원장에 대한 특검 수사를 촉구했다
조희대 대법원장이 외부와의 논의가 없었다고 재반박했다
여야 간에 사법부 독립성을 둘러싼 논란이 확산되었다

타임라인:"""

            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "당신은 한국 정치 뉴스 분석 전문가입니다. 정치 이슈의 시간적 흐름과 인과관계를 정확히 파악하여 체계적인 타임라인을 생성해주세요."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=400
            )
            
            timeline_text = response.choices[0].message.content.strip()
            
            # 빈 줄 제거 및 정리
            lines = [line.strip() for line in timeline_text.split('\n') if line.strip()]
            
            # 번호나 특수문자 제거
            cleaned_lines = []
            for line in lines:
                # 맨 앞의 번호나 기호 제거
                import re
                cleaned_line = re.sub(r'^[\d\-\*\.\s]+', '', line).strip()
                if cleaned_line and len(cleaned_line) > 5:  # 너무 짧은 줄 제외
                    cleaned_lines.append(cleaned_line)
            
            # 최대 8개로 제한
            cleaned_lines = cleaned_lines[:8]
            
            final_timeline = '\n'.join(cleaned_lines)
            
            console.print(f"✅ 타임라인 생성 완료 ({len(cleaned_lines)}개 사건):")
            for i, event in enumerate(cleaned_lines, 1):
                console.print(f"  {i}. {event}")
            
            return final_timeline
            
        except Exception as e:
            console.print(f"❌ 타임라인 생성 실패: {str(e)}")
            return ""
    
    def save_timeline_to_db(self, issue_id: str, timeline_text: str) -> bool:
        """타임라인을 issues 테이블의 issue_timeline 컬럼에 저장"""
        try:
            result = self.supabase_manager.client.table('issues').update({
                'issue_timeline': timeline_text
            }).eq('id', issue_id).execute()
            
            if result.data:
                console.print(f"✅ 이슈 {issue_id}: 타임라인 저장 완료")
                return True
            else:
                console.print(f"❌ 이슈 {issue_id}: 타임라인 저장 실패")
                return False
                
        except Exception as e:
            console.print(f"❌ 데이터베이스 저장 실패: {str(e)}")
            return False
    
    def process_issue(self, issue: Dict[str, Any]) -> bool:
        """단일 이슈 처리: 요약 → 타임라인 생성 → 저장"""
        try:
            issue_id = issue['id']
            issue_title = issue['title']
            issue_summary = issue['issue_summary']
            
            console.print(f"\n🔄 이슈 처리 시작: {issue_title}")
            console.print("=" * 60)
            
            # 1. 요약에서 타임라인 생성
            timeline_text = self.extract_timeline_from_summary(issue_summary)
            
            if not timeline_text:
                console.print(f"⚠️ 이슈 {issue_title}: 타임라인 생성 실패 - 건너뜀")
                return False
            
            # 2. 데이터베이스 저장
            success = self.save_timeline_to_db(issue_id, timeline_text)
            
            if success:
                console.print(f"🎯 이슈 {issue_title}: 처리 완료")
                return True
            else:
                console.print(f"❌ 이슈 {issue_title}: 저장 실패")
                return False
                
        except Exception as e:
            console.print(f"❌ 이슈 처리 중 오류: {str(e)}")
            return False


def generate_all_issue_timelines():
    """모든 이슈의 타임라인 생성"""
    try:
        console.print("🔄 모든 이슈 타임라인 생성 시작")
        console.print("=" * 60)
        
        # 타임라인 생성기 초기화
        generator = IssueTimelineGenerator()
        
        # 테이블 확인
        if not generator.check_issues_table():
            console.print("❌ issues 테이블 확인 실패")
            return
        
        # 모든 이슈 조회
        issues = generator.get_all_issues()
        
        if not issues:
            console.print("❌ 이슈 데이터를 찾을 수 없습니다.")
            return
        
        console.print(f"📰 총 {len(issues)}개 이슈 발견")
        
        # 진행률 표시
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            
            task = progress.add_task("타임라인 생성 중...", total=len(issues))
            
            success_count = 0
            
            for i, issue in enumerate(issues, 1):
                issue_title = issue['title']
                
                progress.update(task, description=f"처리 중: {issue_title[:30]}...")
                
                # 이슈 처리
                if generator.process_issue(issue):
                    success_count += 1
                
                progress.update(task, advance=1)
        
        console.print(f"\n🎯 생성 완료: {success_count}/{len(issues)}개 이슈 타임라인 생성")
        
    except Exception as e:
        console.print(f"❌ 전체 타임라인 생성 실패: {str(e)}")


def main():
    """메인 함수 - 이슈 타임라인 생성 실행"""
    try:
        console.print("🧪 이슈 타임라인 생성 스크립트 (GPT-4o-mini)")
        console.print("=" * 60)
        
        # 모든 이슈 타임라인 생성 실행
        generate_all_issue_timelines()
        
    except Exception as e:
        console.print(f"❌ 실행 실패: {str(e)}")


if __name__ == "__main__":
    main()