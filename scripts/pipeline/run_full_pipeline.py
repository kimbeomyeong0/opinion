#!/usr/bin/env python3
"""
전체 파이프라인 통합 실행 스크립트
1. 크롤링 → 2. 전처리 → 3. 클러스터링 → 4. 콘텐츠 생성
"""

import sys
import os
import argparse
import asyncio
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

console = Console()

async def run_crawling():
    """1단계: 크롤링 실행"""
    try:
        console.print("\n[bold blue]1단계: 크롤링 시작[/bold blue]")
        from scripts.run_crawler import main as crawler_main
        await crawler_main()
        console.print("✅ 크롤링 완료")
        return True
    except Exception as e:
        console.print(f"❌ 크롤링 실패: {e}")
        return False

def run_preprocessing():
    """2단계: 전처리 실행"""
    try:
        console.print("\n[bold blue]2단계: 전처리 시작[/bold blue]")
        from preprocessing.run_preprocessing import main as preprocessing_main
        preprocessing_main()
        console.print("✅ 전처리 완료")
        return True
    except Exception as e:
        console.print(f"❌ 전처리 실패: {e}")
        return False

def run_clustering():
    """3단계: 클러스터링 실행"""
    try:
        console.print("\n[bold blue]3단계: 클러스터링 시작[/bold blue]")
        from scripts.run_clustering import main as clustering_main
        clustering_main()
        console.print("✅ 클러스터링 완료")
        return True
    except Exception as e:
        console.print(f"❌ 클러스터링 실패: {e}")
        return False

def run_content_generation():
    """4단계: 콘텐츠 생성 실행"""
    try:
        console.print("\n[bold blue]4단계: 콘텐츠 생성 시작[/bold blue]")
        from scripts.run_content_generation import main as content_main
        content_main()
        console.print("✅ 콘텐츠 생성 완료")
        return True
    except Exception as e:
        console.print(f"❌ 콘텐츠 생성 실패: {e}")
        return False

async def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='전체 파이프라인 통합 실행')
    parser.add_argument('--step', type=int, choices=[1,2,3,4], 
                       help='실행할 단계 (1: 크롤링, 2: 전처리, 3: 클러스터링, 4: 콘텐츠생성)')
    parser.add_argument('--from-step', type=int, choices=[1,2,3,4], 
                       help='특정 단계부터 실행')
    parser.add_argument('--all', action='store_true', help='모든 단계 실행')
    parser.add_argument('--skip-crawling', action='store_true', help='크롤링 단계 건너뛰기')
    
    args = parser.parse_args()
    
    console.print(Panel.fit(
        "[bold green]🚀 정치 이슈 분석 시스템 - 전체 파이프라인[/bold green]\n"
        "크롤링 → 전처리 → 클러스터링 → 콘텐츠 생성",
        title="Full Pipeline"
    ))
    
    # 실행할 단계들 정의
    steps = [
        ("크롤링", run_crawling, True),  # (이름, 함수, 비동기여부)
        ("전처리", run_preprocessing, False),
        ("클러스터링", run_clustering, False),
        ("콘텐츠 생성", run_content_generation, False)
    ]
    
    if args.step:
        # 특정 단계만 실행
        step_name, step_func, is_async = steps[args.step - 1]
        
        console.print(f"\n[bold yellow]단계 {args.step}: {step_name} 실행[/bold yellow]")
        
        if is_async:
            success = await step_func()
        else:
            success = step_func()
            
        if success:
            console.print(f"✅ {step_name} 완료")
        else:
            console.print(f"❌ {step_name} 실패")
            sys.exit(1)
            
    elif args.from_step:
        # 특정 단계부터 실행
        start_idx = args.from_step - 1
        selected_steps = steps[start_idx:]
        
        console.print(f"\n[bold yellow]단계 {args.from_step}부터 실행 시작[/bold yellow]")
        
        success_count = 0
        total_count = len(selected_steps)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console
        ) as progress:
            task = progress.add_task("파이프라인 진행 중...", total=total_count)
            
            for i, (step_name, step_func, is_async) in enumerate(selected_steps):
                progress.update(task, description=f"실행 중: {step_name}")
                
                try:
                    if is_async:
                        success = await step_func()
                    else:
                        success = step_func()
                    
                    if success:
                        success_count += 1
                        progress.update(task, advance=1)
                    else:
                        console.print(f"\n❌ {step_name} 실패로 중단")
                        break
                        
                except Exception as e:
                    console.print(f"\n❌ {step_name} 실행 중 오류: {e}")
                    break
        
        console.print(f"\n📊 실행 결과: {success_count}/{total_count} 단계 성공")
        
        if success_count == total_count:
            console.print("🎉 선택된 단계들 모두 완료!")
        else:
            console.print("⚠️ 일부 단계가 실패했습니다.")
            sys.exit(1)
            
    elif args.all or not any([args.step, args.from_step]):
        # 모든 단계 실행
        if args.skip_crawling:
            steps = steps[1:]  # 크롤링 단계 제외
            console.print("\n[bold yellow]크롤링 단계를 건너뛰고 실행합니다.[/bold yellow]")
        
        success_count = 0
        total_count = len(steps)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console
        ) as progress:
            task = progress.add_task("전체 파이프라인 진행 중...", total=total_count)
            
            for i, (step_name, step_func, is_async) in enumerate(steps):
                progress.update(task, description=f"실행 중: {step_name}")
                
                try:
                    if is_async:
                        success = await step_func()
                    else:
                        success = step_func()
                    
                    if success:
                        success_count += 1
                        progress.update(task, advance=1)
                    else:
                        console.print(f"\n❌ {step_name} 실패로 중단")
                        break
                        
                except Exception as e:
                    console.print(f"\n❌ {step_name} 실행 중 오류: {e}")
                    break
        
        console.print(f"\n📊 전체 실행 결과: {success_count}/{total_count} 단계 성공")
        
        if success_count == total_count:
            console.print("🎉 전체 파이프라인 완료!")
        else:
            console.print("⚠️ 일부 단계가 실패했습니다.")
            sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
