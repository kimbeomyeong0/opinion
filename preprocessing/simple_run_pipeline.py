#!/usr/bin/env python3
"""
단순화된 전처리 파이프라인 실행 스크립트 - KISS 원칙 적용
복잡한 명령행 인터페이스를 단순화하고 핵심 기능만 유지
"""

import argparse
import sys
import os
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from preprocessing.simple_pipeline import SimplePreprocessingPipeline

def print_banner():
    """배너 출력"""
    print("=" * 60)
    print("🔄 단순화된 뉴스 기사 전처리 파이프라인")
    print("=" * 60)
    print(f"📅 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

def print_help():
    """도움말 출력"""
    print("📖 사용법:")
    print("  python simple_run_pipeline.py                    # 전체 파이프라인 실행")
    print("  python simple_run_pipeline.py --stage 1         # 1단계만 실행")
    print("  python simple_run_pipeline.py --stage 2         # 2단계만 실행")
    print("  python simple_run_pipeline.py --stage 3         # 3단계만 실행")
    print("  python simple_run_pipeline.py --stage 4         # 4단계만 실행")
    print("  python simple_run_pipeline.py --status          # 상태만 확인")
    print()
    print("📋 단계별 설명:")
    print("  1단계: 중복 제거 + 기본 필터링")
    print("  2단계: 텍스트 정제")
    print("  3단계: 텍스트 정규화")
    print("  4단계: 제목+본문 통합")
    print()

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='단순화된 전처리 파이프라인')
    parser.add_argument('--stage', type=int, choices=[1, 2, 3, 4], 
                       help='실행할 단계 (1-4). 생략하면 전체 실행')
    parser.add_argument('--status', action='store_true', 
                       help='상태만 확인하고 종료')
    
    args = parser.parse_args()
    
    print_banner()
    
    # 파이프라인 초기화
    pipeline = SimplePreprocessingPipeline()
    
    # 상태 확인
    status = pipeline.get_pipeline_status()
    print(f"📊 현재 상태:")
    print(f"  전체 기사: {status.get('articles_total', 0)}개")
    print(f"  전처리된 기사: {status.get('articles_preprocessed', 0)}개")
    print(f"  정제된 기사: {status.get('cleaned_articles', 0)}개")
    print(f"  통합된 기사: {status.get('merged_articles', 0)}개")
    print()
    
    if args.status:
        print("✅ 상태 확인 완료")
        return
    
    # 단계별 실행
    if args.stage:
        stage_names = {
            1: 'duplicate_removal',
            2: 'text_cleaning', 
            3: 'text_normalization',
            4: 'content_merging'
        }
        
        stage_name = stage_names[args.stage]
        success = pipeline.run_single_stage(stage_name)
        
        if success:
            print(f"🎉 {args.stage}단계가 성공적으로 완료되었습니다!")
        else:
            print(f"💥 {args.stage}단계가 실패했습니다.")
    else:
        # 전체 파이프라인 실행
        success = pipeline.run_full_pipeline()
        
        if success:
            print("🎉 전체 전처리 파이프라인이 성공적으로 완료되었습니다!")
        else:
            print("💥 전체 전처리 파이프라인이 실패했습니다.")

if __name__ == "__main__":
    main()
