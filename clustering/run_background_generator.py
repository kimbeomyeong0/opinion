#!/usr/bin/env python3
"""
Background 생성기 - 이슈의 객관적 배경 정보 생성
Perplexity API를 사용하여 issues 테이블의 background 컬럼을 채웁니다.
"""

import os
import time
from dotenv import load_dotenv
import openai
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.panel import Panel

# 프로젝트 모듈
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_manager import get_supabase_client

# 환경변수 로드
load_dotenv()

console = Console()

def generate_background(title, subtitle, left_view, right_view, summary):
    """
    Perplexity API를 사용하여 이슈의 background 생성
    
    Args:
        title: 이슈 제목
        subtitle: 이슈 부제목
        left_view: 좌파 관점
        right_view: 우파 관점
        summary: 이슈 요약
        
    Returns:
        str: 생성된 background 텍스트
    """
    try:
        # Perplexity API 클라이언트 설정
        perplexity_client = openai.OpenAI(
            api_key=os.getenv('PERPLEXITY_API_KEY'),
            base_url='https://api.perplexity.ai'
        )
        
        # 프롬프트 구성
        prompt = f"""다음 정치 이슈에 대해 20대~30대가 이해하기 쉬운 배경을 작성해주세요.

이슈 정보:
- 제목: {title}
- 부제목: {subtitle}
- 좌파 관점: {left_view}
- 우파 관점: {right_view}
- 요약: {summary}

요구사항:
1. 200자 내외로 작성
2. 이 이슈가 왜 논란이 되고 있는지, 무엇 때문에 싸우는지 명확히 설명
3. 좌파와 우파가 어떤 점에서 의견이 다른지, 어떤 점에서 같은지 분석
4. 이슈의 역사적 배경과 현재 상황을 간단히 설명
5. 어려운 정치용어는 자연스럽게 괄호로 설명하되, "20~30대가 이해하기 쉽게" 같은 표현은 사용하지 마세요:
   - '여야' → '여당과 야당(여야)'
   - '특검법' → '특별 수사 제도(특검법)'
   - '필리버스터' → '의도적으로 회의 시간 끄는 방식(필리버스터)'
   - '체포동의안' → '구속 허가 신청(체포동의안)'
   - '인사청문회' → '후보자 심사 회의(인사청문회)'
   - '과반수' → '절반 이상(과반수)'
   - '일방처리' → '한쪽이 강행(일방처리)'
   - '합의안' → '협의 결과(합의안)'
   - '재협상' → '다시 협의(재협상)'
   - '결렬' → '협의 깨짐(결렬)'
6. 편향 없이 사실 중심으로 작성
7. 간결하고 명확한 문장으로 자연스럽게 작성
8. 최신 정보 기반으로 작성
9. 문장 끝에 "20~30대가 이해하기 쉽게..." 같은 설명 문장을 추가하지 마세요

배경 정보:"""

        # API 호출
        response = perplexity_client.chat.completions.create(
            model="sonar",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
            temperature=0.3
        )
        
        background = response.choices[0].message.content.strip()
        
        # 참조 번호 제거 (예: [1][2][3] 등)
        import re
        background = re.sub(r'\[\d+\]', '', background)
        background = re.sub(r'\[\d+,\d+\]', '', background)
        background = re.sub(r'\[\d+-\d+\]', '', background)
        
        # 길이 검증 (참고용)
        if len(background) > 150:
            print(f"⚠️  Background가 150자를 초과했습니다: {len(background)}자")
        
        return background
        
    except Exception as e:
        console.print(f"❌ Background 생성 실패: {e}")
        return None

def update_issue_background(issue_id, background):
    """
    issues 테이블의 background 컬럼 업데이트
    
    Args:
        issue_id: 이슈 ID
        background: 생성된 background 텍스트
        
    Returns:
        bool: 업데이트 성공 여부
    """
    try:
        supabase = get_supabase_client()
        
        result = supabase.client.table('issues').update({
            'background': background
        }).eq('id', issue_id).execute()
        
        if result.data:
            return True
        else:
            console.print(f"❌ 이슈 {issue_id} 업데이트 실패")
            return False
            
    except Exception as e:
        console.print(f"❌ DB 업데이트 오류: {e}")
        return False

def process_all_issues():
    """모든 이슈에 대해 background 생성 및 업데이트"""
    try:
        supabase = get_supabase_client()
        
        # 모든 이슈 조회 (덮어쓰기 방식)
        console.print("🔍 모든 이슈의 background를 새로 생성합니다...")
        result = supabase.client.table('issues').select(
            'id, title, subtitle, left_view, right_view, summary, background'
        ).execute()
        
        if not result.data:
            console.print("❌ 처리할 이슈가 없습니다.")
            return False
        
        issues = result.data
        total_issues = len(issues)
        
        console.print(f"📝 총 {total_issues}개 이슈의 background 생성 시작...")
        
        success_count = 0
        failed_count = 0
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            
            task = progress.add_task("Background 생성 중...", total=total_issues)
            
            for i, issue in enumerate(issues, 1):
                issue_id = issue['id']
                title = issue['title']
                subtitle = issue.get('subtitle', '')
                left_view = issue.get('left_view', '')
                right_view = issue.get('right_view', '')
                summary = issue.get('summary', '')
                
                progress.update(task, description=f"[{i}/{total_issues}] {title[:30]}...")
                
                # Background 생성
                background = generate_background(title, subtitle, left_view, right_view, summary)
                
                if background:
                    # DB 업데이트
                    if update_issue_background(issue_id, background):
                        success_count += 1
                        console.print(f"✅ [{i}/{total_issues}] {title[:50]}...")
                    else:
                        failed_count += 1
                        console.print(f"❌ [{i}/{total_issues}] DB 업데이트 실패: {title[:50]}...")
                else:
                    failed_count += 1
                    console.print(f"❌ [{i}/{total_issues}] Background 생성 실패: {title[:50]}...")
                
                # Rate limit 대응
                time.sleep(1)
                progress.advance(task)
        
        # 결과 리포트
        console.print(f"\n📊 Background 생성 완료!")
        console.print(f"✅ 성공: {success_count}개")
        console.print(f"❌ 실패: {failed_count}개")
        
        return success_count > 0
        
    except Exception as e:
        console.print(f"❌ 전체 처리 중 오류 발생: {e}")
        return False

def show_sample_backgrounds():
    """생성된 background 샘플 표시"""
    try:
        supabase = get_supabase_client()
        
        result = supabase.client.table('issues').select(
            'title, background'
        ).not_.is_('background', 'null').limit(3).execute()
        
        if not result.data:
            console.print("❌ 생성된 background가 없습니다.")
            return
        
        console.print("\n📝 Background 샘플:")
        
        for i, issue in enumerate(result.data, 1):
            console.print(f"\n{i}. {issue['title']}")
            console.print(f"   {issue['background']}")
            
    except Exception as e:
        console.print(f"❌ 샘플 조회 실패: {e}")

def main():
    """메인 함수"""
    console.print(Panel.fit(
        "[bold blue]🎯 Background 생성기[/bold blue]\n"
        "이슈의 객관적 배경 정보를 생성합니다.",
        title="Background Generator"
    ))
    
    # 1. 전체 이슈 처리
    success = process_all_issues()
    
    if success:
        # 2. 샘플 표시
        show_sample_backgrounds()
        console.print("\n🎉 Background 생성이 완료되었습니다!")
    else:
        console.print("\n❌ Background 생성에 실패했습니다.")

if __name__ == "__main__":
    main()
