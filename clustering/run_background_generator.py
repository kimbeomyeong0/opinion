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

def generate_background(title, subtitle, summary):
    """
    Perplexity API를 사용하여 이슈의 background 생성
    
    Args:
        title: 이슈 제목
        subtitle: 이슈 부제목  
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
        prompt = f"""다음 정치 이슈에 대해 객관적이고 간결한 배경을 기승전결 구조로 3문장으로 작성해주세요.

구조:
- 1문장 (기): 이슈의 기본 상황과 배경
- 2문장 (승): 구체적인 사건과 전개 과정  
- 3문장 (전): 현재 상황과 갈등의 핵심

요구사항:
- 편향 없이 사실 중심
- 간결하고 명확한 문장
- 3문장 내외
- 최신 정보 기반
- 참조 번호나 인용 표시 없이 깔끔하게 작성

이슈: {title}
부제목: {subtitle}
요약: {summary}"""

        # API 호출
        response = perplexity_client.chat.completions.create(
            model="sonar",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=800,
            temperature=0.3
        )
        
        background = response.choices[0].message.content.strip()
        
        # 참조 번호 제거 (예: [1][2][3] 등)
        import re
        background = re.sub(r'\[\d+\]', '', background)
        background = re.sub(r'\[\d+,\d+\]', '', background)
        background = re.sub(r'\[\d+-\d+\]', '', background)
        
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
        
        # background가 None이거나 빈 문자열인 이슈들 조회
        console.print("🔍 Background가 없는 이슈들을 조회 중...")
        result = supabase.client.table('issues').select(
            'id, title, subtitle, summary, background'
        ).or_('background.is.null,background.eq.').execute()
        
        if not result.data:
            console.print("✅ 모든 이슈에 background가 이미 생성되어 있습니다.")
            return True
        
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
                summary = issue.get('summary', '')
                
                progress.update(task, description=f"[{i}/{total_issues}] {title[:30]}...")
                
                # Background 생성
                background = generate_background(title, subtitle, summary)
                
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
