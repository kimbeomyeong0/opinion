#!/usr/bin/env python3
"""
한겨레 정치 기사 크롤러
"""

import asyncio
from rich.console import Console

console = Console()

class HaniPoliticsCollector:
    """한겨레 정치 기사 수집기"""
    
    def __init__(self):
        self.media_name = "한겨레"
    
    async def run(self):
        """크롤러 실행"""
        console.print("🚀 한겨레 정치 기사 크롤링 시작")

if __name__ == "__main__":
    asyncio.run(HaniPoliticsCollector().run())
