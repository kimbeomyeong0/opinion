#!/usr/bin/env python3
"""
Background 생성기 - 이슈의 객관적 배경 정보 생성
Perplexity로 5개 핵심 사실 생성
"""

import os
import time
import re
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from config.background_prompts import PERPLEXITY_RAW_PROMPT, MODEL_CONFIGS

# 환경변수 로드
load_dotenv()

console = Console()

def generate_background_with_perplexity(title, subtitle, left_view, right_view, center_view):
    """
    Perplexity API를 사용하여 5개 핵심 사실 생성
    
    Args:
        title: 이슈 제목
        subtitle: 이슈 부제목
        left_view: 좌파 관점
        right_view: 우파 관점
        center_view: 중도 관점
        
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
        prompt = PERPLEXITY_RAW_PROMPT.format(
            title=title,
            subtitle=subtitle,
            left_view=left_view,
            right_view=right_view,
            center_view=center_view
        )
        
        # API 호출
        config = MODEL_CONFIGS['perplexity']
        response = perplexity_client.chat.completions.create(
            model=config['model'],
            messages=[{"role": "user", "content": prompt}],
            max_tokens=config['max_tokens'],
            temperature=config['temperature']
        )
        
        background = response.choices[0].message.content.strip()
        
        # 참조 번호 제거
        background = re.sub(r'\[\d+\]', '', background)
        background = re.sub(r'\[\d+,\d+\]', '', background)
        background = re.sub(r'\[\d+-\d+\]', '', background)
        
        return background
        
    except Exception as e:
        console.print(f"❌ Perplexity background 생성 실패: {e}")
        return None

def generate_background(title, subtitle, left_view, right_view, center_view):
    """
    Perplexity로 background 생성
    
    Args:
        title: 이슈 제목
        subtitle: 이슈 부제목
        left_view: 좌파 관점
        right_view: 우파 관점
        center_view: 중도 관점
        
    Returns:
        str: 생성된 background 텍스트
    """
    try:
        # Perplexity로 5개 핵심 사실 생성
        console.print("🔍 핵심 사실 생성 중...")
        background = generate_background_with_perplexity(title, subtitle, left_view, right_view, center_view)
        
        if not background:
            console.print("❌ Background 생성 실패")
            return None
        
        # 불렛 개수 검증 (참고용)
        bullet_count = len([line for line in background.split('\n') if line.strip() and (line.strip().startswith('•') or line.strip()[0].isdigit() and '. ' in line.strip())])
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

def process_single_issue(issue, index, total):
    """단일 이슈 처리 (병렬 처리용)"""
    issue_id = issue['id']
    title = issue['title']
    subtitle = issue.get('subtitle', '')
    left_view = issue.get('left_view', '')
    right_view = issue.get('right_view', '')
    center_view = issue.get('center_view', '')
    
    try:
        # Background 생성
        background = generate_background(title, subtitle, left_view, right_view, center_view)
        
        if background:
            # DB 업데이트
            if update_issue_background(issue_id, background):
                return {
                    'success': True,
                    'index': index,
                    'title': title,
                    'message': f"✅ [{index}/{total}] {title[:50]}..."
                }
            else:
                return {
                    'success': False,
                    'index': index,
                    'title': title,
                    'message': f"❌ [{index}/{total}] DB 업데이트 실패: {title[:50]}...",
                    'error': f"DB 업데이트 실패: {title}"
                }
        else:
            return {
                'success': False,
                'index': index,
                'title': title,
                'message': f"❌ [{index}/{total}] Background 생성 실패: {title[:50]}...",
                'error': f"Background 생성 실패: {title}"
            }
            
    except Exception as e:
        return {
            'success': False,
            'index': index,
            'title': title,
            'message': f"❌ [{index}/{total}] 처리 중 오류: {title[:50]}...",
            'error': f"처리 중 오류: {title} - {str(e)}"
        }

def process_all_issues():
    """모든 이슈에 대해 background 생성 및 업데이트 (병렬 처리)"""
    try:
        supabase = get_supabase_client()
        
        # 모든 이슈 조회 (덮어쓰기 방식)
        console.print("🔍 모든 이슈의 background를 새로 생성합니다...")
        result = supabase.client.table('issues').select(
            'id, title, subtitle, left_view, right_view, center_view, background'
        ).execute()
        
        if not result.data:
            console.print("❌ 처리할 이슈가 없습니다.")
            return False
        
        issues = result.data
        total_issues = len(issues)
        
        console.print(f"📝 총 {total_issues}개 이슈의 background 생성 시작...")
        console.print("🚀 병렬 처리 모드 (최대 3개 동시 처리)")
        console.print("📊 수치 중심: 시간, 숫자, 논리적 순서 포함")
        
        success_count = 0
        failed_count = 0
        error_details = []
        
        # 병렬 처리 (최대 3개 동시 실행)
        with ThreadPoolExecutor(max_workers=3) as executor:
            # 작업 제출
            future_to_issue = {
                executor.submit(process_single_issue, issue, i+1, total_issues): issue 
                for i, issue in enumerate(issues)
            }
            
            # 결과 수집
            for future in as_completed(future_to_issue):
                result = future.result()
                
                # 결과 출력
                console.print(result['message'])
                
                if result['success']:
                    success_count += 1
                else:
                    failed_count += 1
                    if 'error' in result:
                        error_details.append(result['error'])
                
                # Rate limit 대응 (0.5초로 단축)
                time.sleep(0.5)
        
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
        "[bold blue]🎯 Background 생성기 (수치 중심)[/bold blue]\n"
        "Perplexity로 5개 핵심 사실 생성 (시간, 숫자, 논리적 순서)",
        title="Background Generator v3.0"
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
