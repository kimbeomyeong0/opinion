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

from clustering.data_loader import DataLoader
from clustering.cluster_processor import ClusterProcessor
from clustering.issue_generator import IssueGenerator
from clustering.config import get_config

console = Console()

def get_date_filter_option():
    """항상 전체 기사 대상으로 처리 (비대화형)"""
    return None

class SimpleClusterer:
    """단순화된 클러스터러 - 3개 클래스를 조합"""
    
    def __init__(self, date_filter=None):
        """초기화"""
        self.config = get_config()
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
    # 항상 전체 기사 대상으로 처리
    date_filter = get_date_filter_option()
    
    # 선택된 옵션 표시
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
