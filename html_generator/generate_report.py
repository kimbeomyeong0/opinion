#!/usr/bin/env python3
"""
HTML 이슈 레포트 생성기
메인 실행 스크립트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from datetime import datetime
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from html_generator.data_fetcher import IssueDataFetcher
from html_generator.template_engine import HTMLTemplateEngine
from html_generator.config import get_config

console = Console()

class HTMLReportGenerator:
    """HTML 레포트 생성기 메인 클래스"""
    
    def __init__(self):
        """초기화"""
        self.config = get_config()
        self.data_fetcher = IssueDataFetcher()
        self.template_engine = HTMLTemplateEngine()
    
    async def generate_report(self) -> bool:
        """전체 레포트 생성 프로세스"""
        try:
            console.print(Panel.fit("🚀 HTML 이슈 레포트 생성 시작", style="bold blue"))
            
            # 1단계: 데이터 조회
            console.print("\n📊 1단계: 이슈 데이터 조회")
            if not self.data_fetcher.fetch_issues():
                return False
            
            # 2단계: 데이터 가공
            console.print("\n🔄 2단계: 데이터 가공")
            issues_data = self.data_fetcher.process_issues_data()
            summary_data = self.data_fetcher.get_issues_summary()
            
            if not issues_data:
                console.print("❌ 처리할 이슈 데이터가 없습니다.")
                return False
            
            # 3단계: HTML 생성
            console.print("\n🎨 3단계: HTML 레포트 생성")
            
            # 출력 경로 설정
            output_dir = self.config['output_dir']
            output_filename = self.config['output_filename']
            output_path = os.path.join(output_dir, output_filename)
            
            # 레포트 생성
            if not self.template_engine.generate_report(issues_data, summary_data, output_path):
                return False
            
            # 4단계: 결과 요약
            console.print("\n📋 4단계: 생성 결과")
            self._print_generation_summary(issues_data, summary_data, output_path)
            
            console.print(Panel.fit("✅ HTML 레포트 생성 완료!", style="bold green"))
            return True
            
        except Exception as e:
            console.print(f"❌ 레포트 생성 실패: {e}")
            return False
    
    def _print_generation_summary(self, issues_data: list, summary_data: dict, output_path: str):
        """생성 결과 요약 출력"""
        console.print(f"📁 출력 파일: {output_path}")
        console.print(f"📊 처리된 이슈: {len(issues_data)}개")
        console.print(f"📰 총 기사 수: {summary_data['total_sources']}개")
        console.print(f"📅 분석 기간: {summary_data['date_range']}")
        console.print(f"⏰ 생성 시간: {summary_data['generation_date']}")
        
        # 관점별 통계
        total_views = 0
        for issue in issues_data:
            if issue['has_views']:
                total_views += 1
        
        console.print(f"👁️ 관점 분석 포함: {total_views}개 이슈")

async def main():
    """메인 실행 함수"""
    generator = HTMLReportGenerator()
    success = await generator.generate_report()
    
    if success:
        console.print("\n🎉 HTML 이슈 레포트가 성공적으로 생성되었습니다!")
        console.print("📂 reports/ 디렉토리에서 확인하세요.")
    else:
        console.print("\n💥 HTML 이슈 레포트 생성에 실패했습니다.")

if __name__ == "__main__":
    asyncio.run(main())
