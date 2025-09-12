#!/usr/bin/env python3
"""
콘텐츠 생성 통합 실행 스크립트
- Title/Subtitle 생성
- View 생성 (좌파/중립/우파)
- Summary 생성
- Background 생성
"""

import sys
import os
import argparse
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

console = Console()

def run_title_subtitle_generation():
    """Title/Subtitle 생성 실행"""
    try:
        console.print("\n[bold blue]1. Title/Subtitle 생성 시작[/bold blue]")
        from content.run_title_subtitle_generator import main as title_main
        title_main()
        console.print("✅ Title/Subtitle 생성 완료")
        return True
    except Exception as e:
        console.print(f"❌ Title/Subtitle 생성 실패: {e}")
        return False

def run_view_generation():
    """View 생성 실행"""
    try:
        console.print("\n[bold blue]2. View 생성 시작[/bold blue]")
        from content.run_view_generator import main as view_main
        view_main()
        console.print("✅ View 생성 완료")
        return True
    except Exception as e:
        console.print(f"❌ View 생성 실패: {e}")
        return False

def run_summary_generation():
    """Summary 생성 실행"""
    try:
        console.print("\n[bold blue]3. Summary 생성 시작[/bold blue]")
        from content.run_summary_generator import main as summary_main
        summary_main()
        console.print("✅ Summary 생성 완료")
        return True
    except Exception as e:
        console.print(f"❌ Summary 생성 실패: {e}")
        return False

def run_background_generation():
    """Background 생성 실행"""
    try:
        console.print("\n[bold blue]4. Background 생성 시작[/bold blue]")
        from content.run_background_generator import main as background_main
        background_main()
        console.print("✅ Background 생성 완료")
        return True
    except Exception as e:
        console.print(f"❌ Background 생성 실패: {e}")
        return False

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='콘텐츠 생성 통합 실행')
    parser.add_argument('--step', type=int, choices=[1,2,3,4], 
                       help='실행할 단계 (1: Title/Subtitle, 2: View, 3: Summary, 4: Background)')
    parser.add_argument('--all', action='store_true', help='모든 단계 실행')
    
    args = parser.parse_args()
    
    console.print(Panel.fit(
        "[bold green]🎯 콘텐츠 생성 통합 실행기[/bold green]\n"
        "이슈별 제목, 부제목, 관점, 요약, 배경을 생성합니다.",
        title="Content Generation Pipeline"
    ))
    
    if args.step:
        # 특정 단계만 실행
        steps = {
            1: run_title_subtitle_generation,
            2: run_view_generation,
            3: run_summary_generation,
            4: run_background_generation
        }
        
        if steps[args.step]():
            console.print(f"\n✅ 단계 {args.step} 실행 완료")
        else:
            console.print(f"\n❌ 단계 {args.step} 실행 실패")
            sys.exit(1)
            
    elif args.all:
        # 모든 단계 순차 실행
        steps = [
            ("Title/Subtitle 생성", run_title_subtitle_generation),
            ("View 생성", run_view_generation),
            ("Summary 생성", run_summary_generation),
            ("Background 생성", run_background_generation)
        ]
        
        success_count = 0
        total_count = len(steps)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("콘텐츠 생성 진행 중...", total=total_count)
            
            for step_name, step_func in steps:
                progress.update(task, description=f"실행 중: {step_name}")
                
                if step_func():
                    success_count += 1
                    progress.update(task, advance=1)
                else:
                    console.print(f"\n❌ {step_name} 실패로 중단")
                    break
        
        console.print(f"\n📊 실행 결과: {success_count}/{total_count} 단계 성공")
        
        if success_count == total_count:
            console.print("🎉 모든 콘텐츠 생성 완료!")
        else:
            console.print("⚠️ 일부 단계가 실패했습니다.")
            sys.exit(1)
    else:
        # 대화형 모드
        console.print("\n[bold yellow]실행할 단계를 선택하세요:[/bold yellow]")
        console.print("1. Title/Subtitle 생성")
        console.print("2. View 생성")
        console.print("3. Summary 생성")
        console.print("4. Background 생성")
        console.print("5. 모든 단계 실행")
        console.print("0. 종료")
        
        while True:
            try:
                choice = input("\n선택 (0-5): ").strip()
                
                if choice == "0":
                    console.print("👋 종료합니다.")
                    break
                elif choice == "1":
                    run_title_subtitle_generation()
                elif choice == "2":
                    run_view_generation()
                elif choice == "3":
                    run_summary_generation()
                elif choice == "4":
                    run_background_generation()
                elif choice == "5":
                    # 모든 단계 실행
                    steps = [
                        ("Title/Subtitle 생성", run_title_subtitle_generation),
                        ("View 생성", run_view_generation),
                        ("Summary 생성", run_summary_generation),
                        ("Background 생성", run_background_generation)
                    ]
                    
                    success_count = 0
                    for step_name, step_func in steps:
                        if step_func():
                            success_count += 1
                        else:
                            console.print(f"❌ {step_name} 실패로 중단")
                            break
                    
                    console.print(f"\n📊 실행 결과: {success_count}/{len(steps)} 단계 성공")
                    break
                else:
                    console.print("❌ 잘못된 선택입니다. 0-5 사이의 숫자를 입력하세요.")
                    
            except KeyboardInterrupt:
                console.print("\n👋 사용자에 의해 중단되었습니다.")
                break
            except Exception as e:
                console.print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    main()
