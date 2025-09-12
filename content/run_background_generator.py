#!/usr/bin/env python3
"""
Background 생성기 - 이슈의 객관적 배경 정보 생성
2단계 프로세스: Perplexity(사실 수집) → GPT(핵심 선별)
"""

import os
import time
import re
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
from config.background_prompts import PERPLEXITY_RAW_PROMPT, GPT_REFINE_PROMPT, MODEL_CONFIGS

# 환경변수 로드
load_dotenv()

console = Console()

def generate_raw_bullets_with_perplexity(title, subtitle, left_view, right_view, summary):
    """
    1단계: Perplexity API를 사용하여 10개+ 불렛 생성
    
    Args:
        title: 이슈 제목
        subtitle: 이슈 부제목
        left_view: 좌파 관점
        right_view: 우파 관점
        summary: 이슈 요약
        
    Returns:
        str: 생성된 원본 불렛들
    """
    try:
        # Perplexity API 클라이언트 설정
        perplexity_client = openai.OpenAI(
            api_key=os.getenv('PERPLEXITY_API_KEY'),
            base_url='https://api.perplexity.ai'
        )
        
        # 프롬프트 구성
        prompt = PERPLEXITY_RAW_PROMPT.format(
            title=title,
            subtitle=subtitle,
            left_view=left_view,
            right_view=right_view,
            summary=summary
        )
        
        # API 호출
        config = MODEL_CONFIGS['perplexity']
        response = perplexity_client.chat.completions.create(
            model=config['model'],
            messages=[{"role": "user", "content": prompt}],
            max_tokens=config['max_tokens'],
            temperature=config['temperature']
        )
        
        raw_bullets = response.choices[0].message.content.strip()
        
        # 참조 번호 제거
        raw_bullets = re.sub(r'\[\d+\]', '', raw_bullets)
        raw_bullets = re.sub(r'\[\d+,\d+\]', '', raw_bullets)
        raw_bullets = re.sub(r'\[\d+-\d+\]', '', raw_bullets)
        
        return raw_bullets
        
    except Exception as e:
        console.print(f"❌ Perplexity 불렛 생성 실패: {e}")
        return None

def refine_bullets_with_gpt(raw_bullets, title, subtitle):
    """
    2단계: GPT를 사용하여 핵심 5개 불렛 선별/정리
    
    Args:
        raw_bullets: 1단계에서 생성된 원본 불렛들
        title: 이슈 제목
        subtitle: 이슈 부제목
        
    Returns:
        str: 정리된 핵심 5개 불렛
    """
    try:
        # GPT API 클라이언트 설정
        gpt_client = openai.OpenAI(
            api_key=os.getenv('OPENAI_API_KEY')
        )
        
        # 프롬프트 구성
        prompt = GPT_REFINE_PROMPT.format(
            title=title,
            subtitle=subtitle,
            raw_bullets=raw_bullets
        )
        
        # API 호출
        config = MODEL_CONFIGS['gpt']
        response = gpt_client.chat.completions.create(
            model=config['model'],
            messages=[{"role": "user", "content": prompt}],
            max_tokens=config['max_tokens'],
            temperature=config['temperature']
        )
        
        refined_bullets = response.choices[0].message.content.strip()
        return refined_bullets
        
    except Exception as e:
        console.print(f"❌ GPT 불렛 정리 실패: {e}")
        return None

def generate_background(title, subtitle, left_view, right_view, summary):
    """
    2단계 프로세스로 background 생성
    
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
        # 1단계: Perplexity로 10개+ 불렛 생성
        console.print("🔍 1단계: 사실 수집 중...")
        raw_bullets = generate_raw_bullets_with_perplexity(title, subtitle, left_view, right_view, summary)
        
        if not raw_bullets:
            console.print("❌ 1단계 실패")
            return None
        
        # 2단계: GPT로 핵심 5개 선별/정리
        console.print("🎯 2단계: 핵심 선별 중...")
        refined_bullets = refine_bullets_with_gpt(raw_bullets, title, subtitle)
        
        if not refined_bullets:
            console.print("❌ 2단계 실패")
            return None
        
        # 최종 결과
        background = refined_bullets
        
        # 길이 검증 (참고용)
        bullet_count = len([line for line in background.split('\n') if line.strip().startswith('•')])
        console.print(f"✅ 생성 완료: {bullet_count}개 불렛")
        
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
        error_details = []
        
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
                
                try:
                    # Background 생성
                    background = generate_background(title, subtitle, left_view, right_view, summary)
                    
                    if background:
                        # DB 업데이트
                        if update_issue_background(issue_id, background):
                            success_count += 1
                            console.print(f"✅ [{i}/{total_issues}] {title[:50]}...")
                        else:
                            failed_count += 1
                            error_details.append(f"DB 업데이트 실패: {title}")
                            console.print(f"❌ [{i}/{total_issues}] DB 업데이트 실패: {title[:50]}...")
                    else:
                        failed_count += 1
                        error_details.append(f"Background 생성 실패: {title}")
                        console.print(f"❌ [{i}/{total_issues}] Background 생성 실패: {title[:50]}...")
                        
                except Exception as e:
                    failed_count += 1
                    error_msg = f"처리 중 오류: {title} - {str(e)}"
                    error_details.append(error_msg)
                    console.print(f"❌ [{i}/{total_issues}] {error_msg}")
                
                # Rate limit 대응
                time.sleep(1)
                progress.advance(task)
        
        # 결과 리포트
        console.print(f"\n📊 Background 생성 완료!")
        console.print(f"✅ 성공: {success_count}개")
        console.print(f"❌ 실패: {failed_count}개")
        
        # 에러 상세 정보 (실패한 경우만)
        if error_details:
            console.print(f"\n🔍 실패 상세:")
            for error in error_details[:5]:  # 최대 5개만 표시
                console.print(f"  • {error}")
            if len(error_details) > 5:
                console.print(f"  • ... 외 {len(error_details) - 5}개")
        
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
        "[bold blue]🎯 Background 생성기 (2단계 프로세스)[/bold blue]\n"
        "1단계: Perplexity로 사실 수집 → 2단계: GPT로 핵심 선별",
        title="Background Generator v2.0"
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
