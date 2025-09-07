#!/usr/bin/env python3
"""
크롤러 단계별 실행 스크립트
사용법: python crawler/run_crawler_stage.py [stage_number]
예시: python crawler/run_crawler_stage.py 1
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
        await manager.run_stage1_simple_html()
    elif stage == 2:
        await manager.run_stage2_api_based()
    elif stage == 3:
        await manager.run_stage3_complex_html()
    elif stage == 4:
        await manager.run_stage4_complex_api()
    else:
        console.print(f"❌ 잘못된 단계 번호: {stage}. 1-4 중에서 선택하세요.")
        return
    
    # 결과 요약 출력
    manager.print_summary()

async def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='크롤러 단계별 실행')
    parser.add_argument('stage', type=int, choices=[1, 2, 3, 4], 
                       help='실행할 단계 (1: 단순 HTML, 2: API 기반, 3: 복잡한 HTML, 4: 복잡한 API)')
    
    args = parser.parse_args()
    
    stage_names = {
        1: "단순한 HTML 크롤러 (오마이뉴스, 연합뉴스)",
        2: "API 기반 크롤러 (한겨레, 뉴스원, 경향신문)",
        3: "복잡한 HTML 크롤러 (동아일보, 중앙일보, 뉴시스)",
        4: "복잡한 API 크롤러 (조선일보)"
    }
    
    console.print(f"🎯 단계 {args.stage} 실행: {stage_names[args.stage]}")
    
    manager = CrawlerManager()
    await run_stage(manager, args.stage)

if __name__ == "__main__":
    asyncio.run(main())
