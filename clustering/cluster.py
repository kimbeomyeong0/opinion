#!/usr/bin/env python3
"""
단순화된 클러스터링 스크립트 - KISS 원칙 적용
3개의 단순한 클래스를 조합하여 클러스터링 실행
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm

from data_loader import DataLoader
from cluster_processor import ClusterProcessor
from issue_generator import IssueGenerator

console = Console()

def get_date_filter_option():
    """사용자에게 날짜 필터 옵션 선택받기"""
    console.print(Panel.fit("📅 데이터 범위 선택", style="bold yellow"))
    console.print("어떤 기사들을 대상으로 클러스터링을 진행하시겠습니까?")
    console.print()
    console.print("1. 전체 기사 (모든 기사)")
    console.print("2. 전날 기사만 (KCT 기준 00:00-23:59)")
    console.print("3. 오늘 기사만 (00:00-현재)")
    console.print()
    
    while True:
        choice = Prompt.ask("선택하세요", choices=["1", "2", "3"], default="2")
        
        if choice == "1":
            return None
        elif choice == "2":
            return "yesterday"
        elif choice == "3":
            return "today"
        else:
            console.print("❌ 잘못된 선택입니다. 1, 2, 3 중에서 선택해주세요.")

class SimpleClusterer:
    """단순화된 클러스터러 - 3개 클래스를 조합"""
    
    def __init__(self, date_filter=None):
        """초기화"""
        self.data_loader = DataLoader(date_filter)
        self.cluster_processor = None
        self.issue_generator = None
    
    async def run_clustering(self) -> bool:
        """전체 클러스터링 프로세스 실행"""
        console.print(Panel.fit("🚀 단순화된 클러스터링 시작", style="bold blue"))
        
        try:
            # 1단계: 데이터 로드
            console.print("\n📊 1단계: 데이터 로드")
            if not self.data_loader.load_all_data():
                return False
            
            # 2단계: 클러스터링
            console.print("\n🔄 2단계: 클러스터링")
            self.cluster_processor = ClusterProcessor(
                self.data_loader.embeddings,
                self.data_loader.embeddings_data,
                self.data_loader.articles_data,
                self.data_loader.media_outlets
            )
            
            if not self.cluster_processor.process_clustering():
                return False
            
            # 3단계: 이슈 생성 및 저장
            console.print("\n🤖 3단계: 이슈 생성 및 저장")
            self.issue_generator = IssueGenerator(
                self.cluster_processor.clusters_info,
                self.data_loader.articles_data,
                self.data_loader.media_outlets
            )
            
            if not await self.issue_generator.save_issues_to_database():
                return False
            
            console.print(Panel.fit("✅ 클러스터링 완료!", style="bold green"))
            return True
            
        except Exception as e:
            console.print(f"❌ 클러스터링 실패: {e}")
            return False

async def main():
    """메인 실행 함수"""
    # 사용자에게 날짜 필터 옵션 선택받기
    date_filter = get_date_filter_option()
    
    # 선택된 옵션 표시
    if date_filter == "yesterday":
        console.print("📅 전날 기사만 처리합니다.")
    elif date_filter == "today":
        console.print("📅 오늘 기사만 처리합니다.")
    else:
        console.print("📅 전체 기사를 처리합니다.")
    
    console.print()
    
    # 클러스터링 실행
    clusterer = SimpleClusterer(date_filter)
    success = await clusterer.run_clustering()
    
    if success:
        console.print("🎉 클러스터링이 성공적으로 완료되었습니다!")
    else:
        console.print("💥 클러스터링이 실패했습니다.")

if __name__ == "__main__":
    asyncio.run(main())
