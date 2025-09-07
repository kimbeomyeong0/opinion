#!/usr/bin/env python3
"""
크롤러 병렬 파이프라인 실행 스크립트
"""

import asyncio
import sys
import os
from rich.console import Console

# 프로젝트 루트를 sys.path에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from crawler import CrawlerManager

console = Console()

async def main():
    """메인 실행 함수"""
    console.print("🚀 크롤러 병렬 파이프라인을 시작합니다...")
    
    manager = CrawlerManager()
    await manager.run_full_pipeline()

if __name__ == "__main__":
    asyncio.run(main())
