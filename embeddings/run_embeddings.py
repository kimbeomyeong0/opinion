#!/usr/bin/env python3
"""
클러스터링 목적의 임베딩 생성 및 저장 스크립트
"""

import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from rich.console import Console
from rich.progress import Progress, TaskID, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.table import Table
from rich.panel import Panel

from embeddings.config import get_config
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_manager import get_supabase_client
import openai
from openai import OpenAI

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('embeddings.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

console = Console()

class EmbeddingProcessor:
    """임베딩 처리 메인 클래스"""
    
    def __init__(self):
        """임베딩 프로세서 초기화"""
        self.config = get_config()
        self.supabase = get_supabase_client()
        self.openai_client = None
        self._initialize_openai()
        
        # 통계 변수
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.start_time = None
    
    def _initialize_openai(self):
        """OpenAI 클라이언트 초기화"""
        api_key = self.config["openai_api_key"]
        if not api_key:
            raise ValueError("OpenAI API 키가 설정되지 않았습니다. OPENAI_API_KEY 환경변수를 설정해주세요.")
        
        self.openai_client = OpenAI(api_key=api_key)
        logger.info(f"OpenAI 클라이언트 초기화 완료 (모델: {self.config['embedding_model']})")
    
    def generate_embedding(self, text: str) -> Optional[List[float]]:
        """
        단일 텍스트에 대한 임베딩 생성
        
        Args:
            text: 임베딩을 생성할 텍스트
            
        Returns:
            임베딩 벡터 또는 None (실패시)
        """
        if not text or not text.strip():
            logger.warning("빈 텍스트로 인해 임베딩 생성을 건너뜁니다.")
            return None
        
        for attempt in range(self.config["max_retries"]):
            try:
                response = self.openai_client.embeddings.create(
                    model=self.config["embedding_model"],
                    input=text.strip(),
                    dimensions=self.config["embedding_dimensions"]
                )
                
                embedding = response.data[0].embedding
                logger.debug(f"임베딩 생성 성공 (차원: {len(embedding)})")
                return embedding
                
            except Exception as e:
                logger.warning(f"임베딩 생성 시도 {attempt + 1} 실패: {str(e)}")
                if attempt < self.config["max_retries"] - 1:
                    time.sleep(self.config["retry_delay"] * (2 ** attempt))  # 지수 백오프
                else:
                    logger.error(f"임베딩 생성 최종 실패: {str(e)}")
                    return None
        
        return None
    
    def generate_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """
        여러 텍스트에 대한 배치 임베딩 생성
        
        Args:
            texts: 임베딩을 생성할 텍스트 리스트
            
        Returns:
            임베딩 벡터 리스트 (실패한 경우 None 포함)
        """
        if not texts:
            return []
        
        # 빈 텍스트 필터링
        valid_texts = []
        valid_indices = []
        for i, text in enumerate(texts):
            if text and text.strip():
                valid_texts.append(text.strip())
                valid_indices.append(i)
        
        if not valid_texts:
            logger.warning("유효한 텍스트가 없어 배치 임베딩 생성을 건너뜁니다.")
            return [None] * len(texts)
        
        results = [None] * len(texts)
        
        for attempt in range(self.config["max_retries"]):
            try:
                response = self.openai_client.embeddings.create(
                    model=self.config["embedding_model"],
                    input=valid_texts,
                    dimensions=self.config["embedding_dimensions"]
                )
                
                # 결과를 원래 인덱스에 매핑
                for i, embedding_data in enumerate(response.data):
                    original_index = valid_indices[i]
                    results[original_index] = embedding_data.embedding
                
                logger.info(f"배치 임베딩 생성 성공 ({len(valid_texts)}개 텍스트)")
                return results
                
            except Exception as e:
                logger.warning(f"배치 임베딩 생성 시도 {attempt + 1} 실패: {str(e)}")
                if attempt < self.config["max_retries"] - 1:
                    time.sleep(self.config["retry_delay"] * (2 ** attempt))
                else:
                    logger.error(f"배치 임베딩 생성 최종 실패: {str(e)}")
                    # 개별 처리로 폴백
                    return self._generate_embeddings_individually(texts)
        
        return results
    
    def _generate_embeddings_individually(self, texts: List[str]) -> List[Optional[List[float]]]:
        """
        개별 텍스트에 대한 임베딩 생성 (배치 실패시 폴백)
        
        Args:
            texts: 임베딩을 생성할 텍스트 리스트
            
        Returns:
            임베딩 벡터 리스트
        """
        logger.info("개별 임베딩 생성으로 폴백합니다.")
        results = []
        
        for i, text in enumerate(texts):
            if i % 10 == 0:  # 진행상황 로깅
                logger.info(f"개별 임베딩 진행중: {i}/{len(texts)}")
            
            embedding = self.generate_embedding(text)
            results.append(embedding)
            
            # API 레이트 리미트 방지
            time.sleep(0.1)
        
        return results
    
    def process_embeddings(self, batch_size: int = None, max_articles: int = None):
        """
        임베딩 처리 메인 함수
        
        Args:
            batch_size: 배치 크기 (기본값: 설정 파일 값)
            max_articles: 처리할 최대 기사 수 (None이면 전체)
        """
        if batch_size is None:
            batch_size = self.config["batch_size"]
        
        self.start_time = datetime.now()
        
        console.print(Panel.fit(
            "[bold blue]🤖 클러스터링용 임베딩 생성 시작[/bold blue]\n"
            f"모델: {self.config['embedding_model']}\n"
            f"차원: {self.config['embedding_dimensions']}\n"
            f"배치 크기: {batch_size}",
            title="임베딩 처리 설정"
        ))
        
        try:
            # 전체 기사 수 확인
            total_articles = self.supabase.get_total_articles_count()
            if total_articles == 0:
                console.print("❌ 임베딩 가능한 기사가 없습니다.")
                return
            
            if max_articles:
                total_articles = min(total_articles, max_articles)
            
            console.print(f"📊 처리 대상: {total_articles:,}개 기사")
            
            # 진행상황 표시를 위한 Progress 설정
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                console=console
            ) as progress:
                
                main_task = progress.add_task(
                    f"임베딩 처리 중... (0/{total_articles})", 
                    total=total_articles
                )
                
                # 배치별 처리
                offset = 0
                while offset < total_articles:
                    # 현재 배치 크기 계산
                    current_batch_size = min(batch_size, total_articles - offset)
                    
                    # 기사 데이터 조회
                    articles = self.supabase.get_articles_for_embedding(offset, current_batch_size)
                    
                    if not articles:
                        break
                    
                    # 이미 임베딩이 있는 기사 필터링
                    article_ids = [article['id'] for article in articles]
                    existing_ids = self.supabase.check_existing_embeddings(article_ids, self.config["embedding_types"]["CLUSTERING"])
                    
                    # 새로운 임베딩이 필요한 기사만 필터링
                    new_articles = [article for article in articles if article['id'] not in existing_ids]
                    
                    if new_articles:
                        # 임베딩 생성 및 저장
                        self._process_batch(new_articles, progress, main_task)
                    else:
                        console.print(f"⏭️  배치 {offset//batch_size + 1}: 모든 기사가 이미 임베딩됨")
                    
                    offset += current_batch_size
                    
                    # API 레이트 리미트 방지
                    time.sleep(0.5)
            
            # 최종 결과 출력
            self._print_final_results()
            
        except Exception as e:
            logger.error(f"임베딩 처리 중 오류 발생: {str(e)}")
            console.print(f"❌ 오류 발생: {str(e)}")
    
    def _process_batch(self, articles: List[Dict[str, Any]], progress: Progress, main_task: TaskID):
        """배치 단위 임베딩 처리"""
        try:
            # 텍스트 추출
            texts = []
            for article in articles:
                merged_content = article.get('merged_content', '')
                if merged_content and merged_content.strip():
                    texts.append(merged_content.strip())
                else:
                    texts.append('')  # 빈 텍스트는 나중에 필터링됨
            
            # 임베딩 생성
            embeddings = self.generate_embeddings_batch(texts)
            
            # 임베딩 데이터 준비
            embedding_records = []
            for i, (article, embedding) in enumerate(zip(articles, embeddings)):
                if embedding:  # 성공한 임베딩만 저장
                    record = self.supabase.create_embedding_record(
                        cleaned_article_id=article['id'],
                        embedding_vector=embedding,
                        embedding_type=self.config["embedding_types"]["CLUSTERING"]
                    )
                    embedding_records.append(record)
                    self.success_count += 1
                else:
                    self.error_count += 1
                    logger.warning(f"임베딩 생성 실패: {article['id']}")
                
                self.processed_count += 1
            
            # 데이터베이스에 저장
            if embedding_records:
                success = self.supabase.save_embeddings(embedding_records)
                if not success:
                    logger.error("임베딩 저장 실패")
            
            # 진행상황 업데이트
            progress.update(main_task, 
                          completed=self.processed_count,
                          description=f"임베딩 처리 중... ({self.processed_count}/{progress.tasks[main_task].total})")
            
            console.print(f"✅ 배치 처리 완료: {len(embedding_records)}개 임베딩 저장")
            
        except Exception as e:
            logger.error(f"배치 처리 중 오류: {str(e)}")
            console.print(f"❌ 배치 처리 오류: {str(e)}")
    
    def _print_final_results(self):
        """최종 결과 출력"""
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        # 통계 테이블 생성
        stats_table = Table(title="임베딩 처리 결과")
        stats_table.add_column("항목", style="cyan")
        stats_table.add_column("값", style="green")
        
        stats_table.add_row("처리된 기사", f"{self.processed_count:,}개")
        stats_table.add_row("성공", f"{self.success_count:,}개")
        stats_table.add_row("실패", f"{self.error_count:,}개")
        stats_table.add_row("소요 시간", str(duration).split('.')[0])
        stats_table.add_row("처리 속도", f"{self.processed_count/duration.total_seconds():.1f}개/초")
        
        console.print(stats_table)
        
        # 데이터베이스 통계
        db_stats = self.supabase.get_embedding_statistics()
        console.print(f"\n📊 현재 데이터베이스 임베딩 현황:")
        console.print(f"• 전체 임베딩: {db_stats['total_embeddings']:,}개")
        console.print(f"• 클러스터링용: {db_stats['clustering_embeddings']:,}개")
        console.print(f"• 오늘 생성: {db_stats['today_embeddings']:,}개")

def main():
    """메인 실행 함수"""
    try:
        processor = EmbeddingProcessor()
        
        # 사용자에게 처리 옵션 확인
        console.print("\n[bold yellow]임베딩 처리 옵션을 선택하세요:[/bold yellow]")
        console.print("1. 전체 기사 처리 (기본)")
        console.print("2. 제한된 수의 기사 처리")
        console.print("3. 통계만 확인")
        
        choice = input("\n선택 (1-3): ").strip()
        
        if choice == "2":
            max_articles = int(input("처리할 최대 기사 수를 입력하세요: "))
            processor.process_embeddings(max_articles=max_articles)
        elif choice == "3":
            # 통계만 확인
            db_stats = processor.supabase.get_embedding_statistics()
            console.print(Panel.fit(
                f"[bold blue]📊 임베딩 현황[/bold blue]\n\n"
                f"전체 임베딩: {db_stats['total_embeddings']:,}개\n"
                f"클러스터링용: {db_stats['clustering_embeddings']:,}개\n"
                f"오늘 생성: {db_stats['today_embeddings']:,}개"
            ))
        else:
            # 기본: 전체 처리
            processor.process_embeddings()
            
    except KeyboardInterrupt:
        console.print("\n⚠️  사용자에 의해 중단되었습니다.")
    except Exception as e:
        console.print(f"❌ 실행 중 오류 발생: {str(e)}")
        logger.error(f"메인 실행 오류: {str(e)}")

if __name__ == "__main__":
    main()
