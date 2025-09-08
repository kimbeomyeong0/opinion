#!/usr/bin/env python3
"""
크롤러 실행 스크립트 - KISS 원칙 적용
사용법:
  python run_crawler_pipeline.py          # 전체 파이프라인 실행
  python run_crawler_pipeline.py 1        # 1단계만 실행 (단순한 크롤러)
  python run_crawler_pipeline.py 2        # 2단계만 실행 (복잡한 크롤러)
"""

import asyncio
import sys
import os
import argparse
from rich.console import Console

# 프로젝트 루트를 sys.path에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from crawler import CrawlerManager

console = Console()

async def run_stage(manager: CrawlerManager, stage: int):
    """특정 단계만 실행"""
    if stage == 1:
        await manager.run_simple_crawlers()
    elif stage == 2:
        await manager.run_complex_crawlers()
    else:
        console.print(f"❌ 잘못된 단계 번호: {stage}. 1-2 중에서 선택하세요.")
        return
    
    # 결과 요약 출력
    manager.print_summary()

async def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='크롤러 실행')
    parser.add_argument('stage', type=int, nargs='?', choices=[1, 2], 
                       help='실행할 단계 (1: 단순한 크롤러, 2: 복잡한 크롤러). 생략하면 전체 실행')
    
    args = parser.parse_args()
    
    stage_names = {
        1: "단순한 크롤러 (오마이뉴스, 연합뉴스, 한겨레, 뉴스원, 경향신문)",
        2: "복잡한 크롤러 (동아일보, 중앙일보, 뉴시스, 조선일보)"
    }
    
    manager = CrawlerManager()
    
    if args.stage:
        console.print(f"🎯 단계 {args.stage} 실행: {stage_names[args.stage]}")
        await run_stage(manager, args.stage)
    else:
        console.print("🚀 전체 크롤러 파이프라인을 시작합니다...")
        await manager.run_full_pipeline()

if __name__ == "__main__":
    asyncio.run(main())
