#!/usr/bin/env python3
"""
크롤러 병렬 파이프라인 매니저
4단계로 나누어 크롤러들을 안정적으로 실행합니다.
"""

import asyncio
import sys
import os
import time
from datetime import datetime
from typing import List, Dict, Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import pytz

# 프로젝트 루트 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 설정 및 크롤러 모듈들 import
from config.crawler_config import CRAWLER_PARAMS, CRAWLER_GROUPS, PLAYWRIGHT_CRAWLERS, STAGE_DELAYS, RETRY_CONFIG
from .html_parsing.ohmynews_politics import OhmyNewsPoliticsCollector
from .html_parsing.yonhap_politics import YonhapPoliticsCollector
from .api_based.hani_politics import HaniPoliticsCollector
from .api_based.newsone_politics import NewsonePoliticsCollector
from .api_based.khan_politics import KhanPoliticsCollector
from .html_parsing.donga_politics import DongaPoliticsCollector
from .html_parsing.joongang_politics import JoongangPoliticsCollector
from .html_parsing.newsis_politics import NewsisPoliticsCollector
from .api_based.chosun_politics import ChosunPoliticsCollector

console = Console()
KST = pytz.timezone("Asia/Seoul")


class CrawlerResult:
    """크롤러 실행 결과를 저장하는 클래스"""
    
    def __init__(self, crawler_name: str):
        self.crawler_name = crawler_name
        self.status = "pending"
        self.start_time = None
        self.end_time = None
        self.duration = None
        self.error_message = None
        self.articles_collected = 0
        
    def start(self):
        """실행 시작"""
        self.start_time = datetime.now(KST)
        self.status = "running"
        
    def finish(self, success: bool = True, error_message: str = None, articles_count: int = 0):
        """실행 완료"""
        self.end_time = datetime.now(KST)
        self.duration = (self.end_time - self.start_time).total_seconds()
        self.status = "success" if success else "failed"
        self.error_message = error_message
        self.articles_collected = articles_count


class CrawlerManager:
    """크롤러 병렬 파이프라인 매니저"""
    
    def __init__(self):
        self.results: Dict[str, CrawlerResult] = {}
        self.semaphore = asyncio.Semaphore(3)  # 일반 크롤러 동시 실행 제한
        self.playwright_semaphore = asyncio.Semaphore(2)  # Playwright 크롤러 동시 실행 제한
        
        # 크롤러 클래스 매핑
        self.crawler_classes = {
            "ohmynews_politics": OhmyNewsPoliticsCollector,
            "yonhap_politics": YonhapPoliticsCollector,
            "hani_politics": HaniPoliticsCollector,
            "newsone_politics": NewsonePoliticsCollector,
            "khan_politics": KhanPoliticsCollector,
            "donga_politics": DongaPoliticsCollector,
            "joongang_politics": JoongangPoliticsCollector,
            "newsis_politics": NewsisPoliticsCollector,
            "chosun_politics": ChosunPoliticsCollector,
        }
        
        # 설정에서 크롤러 그룹 및 설정 가져오기
        self.crawler_groups = CRAWLER_GROUPS
        self.playwright_crawlers = PLAYWRIGHT_CRAWLERS
    
    def _get_crawler_params(self, crawler_name: str) -> Dict:
        """크롤러별 실행 파라미터 반환"""
        return CRAWLER_PARAMS.get(crawler_name, {})
    
    async def run_crawler(self, crawler_name: str) -> CrawlerResult:
        """단일 크롤러 실행"""
        result = CrawlerResult(crawler_name)
        self.results[crawler_name] = result
        
        try:
            console.print(f"🚀 {crawler_name} 크롤러 시작")
            result.start()
            
            # 크롤러 클래스 인스턴스 생성
            crawler_class = self.crawler_classes.get(crawler_name)
            if not crawler_class:
                raise ValueError(f"크롤러 클래스를 찾을 수 없습니다: {crawler_name}")
            
            crawler = crawler_class()
            params = self._get_crawler_params(crawler_name)
            
            # 크롤러 실행
            await crawler.run(**params)
            
            # 성공적으로 완료
            articles_count = len(getattr(crawler, 'articles', []))
            result.finish(success=True, articles_count=articles_count)
            console.print(f"✅ {crawler_name} 완료 - {articles_count}개 기사 수집")
            
        except Exception as e:
            error_msg = str(e)[:100] + "..." if len(str(e)) > 100 else str(e)
            result.finish(success=False, error_message=error_msg)
            console.print(f"❌ {crawler_name} 실패: {error_msg}")
            
        return result
    
    async def run_crawler_with_semaphore(self, crawler_name: str) -> CrawlerResult:
        """세마포어를 사용한 크롤러 실행"""
        if crawler_name in self.playwright_crawlers:
            async with self.playwright_semaphore:
                return await self.run_crawler(crawler_name)
        else:
            async with self.semaphore:
                return await self.run_crawler(crawler_name)
    
    async def run_simple_crawlers(self):
        """단순한 크롤러들 병렬 실행"""
        stage_info = self.crawler_groups["simple"]
        console.print(Panel.fit(f"🎯 1단계: {stage_info['description']}", style="bold blue"))
        
        crawlers = stage_info["crawlers"]
        console.print(f"실행할 크롤러: {', '.join(crawlers)}")
        
        # 병렬 실행
        tasks = [self.run_crawler_with_semaphore(crawler) for crawler in crawlers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 출력
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                console.print(f"❌ {crawlers[i]} 예외 발생: {result}")
            else:
                status = "✅ 성공" if result.status == "success" else "❌ 실패"
                console.print(f"{status} {result.crawler_name} - {result.articles_collected}개 기사")
    
    async def run_complex_crawlers(self):
        """복잡한 크롤러들 순차 실행 (Playwright 사용)"""
        stage_info = self.crawler_groups["complex"]
        console.print(Panel.fit(f"🎯 2단계: {stage_info['description']}", style="bold yellow"))
        
        crawlers = stage_info["crawlers"]
        console.print(f"실행할 크롤러: {', '.join(crawlers)} (순차 실행)")
        
        # 순차 실행 (Playwright 리소스 충돌 방지)
        for crawler in crawlers:
            console.print(f"🔄 {crawler} 실행 중...")
            result = await self.run_crawler_with_semaphore(crawler)
            
            status = "✅ 성공" if result.status == "success" else "❌ 실패"
            console.print(f"{status} {result.crawler_name} - {result.articles_collected}개 기사")
            
            # 크롤러 간 대기 시간
            await asyncio.sleep(2)
    
    def print_summary(self):
        """전체 실행 결과 요약 출력"""
        console.print(Panel.fit("📊 크롤러 실행 결과 요약", style="bold magenta"))
        
        table = Table(title="크롤러 실행 결과")
        table.add_column("크롤러", style="cyan")
        table.add_column("상태", style="green")
        table.add_column("수집 기사", style="blue")
        table.add_column("실행 시간", style="yellow")
        table.add_column("오류 메시지", style="red")
        
        total_articles = 0
        success_count = 0
        
        for crawler_name, result in self.results.items():
            status_icon = "✅" if result.status == "success" else "❌"
            duration_str = f"{result.duration:.1f}초" if result.duration else "N/A"
            error_str = result.error_message[:30] + "..." if result.error_message and len(result.error_message) > 30 else result.error_message or ""
            
            table.add_row(
                crawler_name,
                f"{status_icon} {result.status}",
                str(result.articles_collected),
                duration_str,
                error_str
            )
            
            total_articles += result.articles_collected
            if result.status == "success":
                success_count += 1
        
        console.print(table)
        
        # 전체 통계
        console.print(f"\n📈 전체 통계:")
        console.print(f"  총 크롤러: {len(self.results)}개")
        console.print(f"  성공: {success_count}개")
        console.print(f"  실패: {len(self.results) - success_count}개")
        console.print(f"  총 수집 기사: {total_articles}개")
        console.print(f"  성공률: {(success_count / len(self.results) * 100):.1f}%")
    
    async def run_full_pipeline(self):
        """전체 파이프라인 실행"""
        start_time = datetime.now(KST)
        console.print(Panel.fit("🚀 크롤러 파이프라인 시작", style="bold white"))
        console.print(f"시작 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # 1단계: 단순한 크롤러들
            await self.run_simple_crawlers()
            await asyncio.sleep(STAGE_DELAYS["simple"])
            
            # 2단계: 복잡한 크롤러들
            await self.run_complex_crawlers()
            await asyncio.sleep(STAGE_DELAYS["complex"])
            
        except KeyboardInterrupt:
            console.print("⏹️ 사용자에 의해 중단되었습니다")
        except Exception as e:
            console.print(f"❌ 파이프라인 실행 중 오류: {e}")
        finally:
            end_time = datetime.now(KST)
            total_duration = (end_time - start_time).total_seconds()
            
            console.print(f"\n🏁 파이프라인 완료")
            console.print(f"종료 시간: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            console.print(f"총 실행 시간: {total_duration:.1f}초")
            
            # 결과 요약 출력
            self.print_summary()


async def main():
    """메인 실행 함수"""
    manager = CrawlerManager()
    await manager.run_full_pipeline()


if __name__ == "__main__":
    asyncio.run(main())
