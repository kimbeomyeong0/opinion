#!/usr/bin/env python3
"""
클러스터링 목적의 임베딩 생성 및 저장 스크립트 (비동기 버전)
"""

import logging
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional
from rich.console import Console
from rich.progress import Progress, TaskID, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.table import Table
from rich.panel import Panel

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from embeddings.config import get_config
from utils.supabase_manager import get_async_supabase_client
from openai import AsyncOpenAI

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
    """임베딩 처리 메인 클래스 (비동기)"""
    
    def __init__(self):
        """임베딩 프로세서 초기화"""
        self.config = get_config()
        self.supabase = get_async_supabase_client()
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
        
        self.openai_client = AsyncOpenAI(api_key=api_key)
        logger.info(f"Async OpenAI 클라이언트 초기화 완료 (모델: {self.config['embedding_model']})")
    
    async def generate_embedding(self, text: str) -> Optional[List[float]]:
        """단일 텍스트에 대한 임베딩 생성"""
        if not text or not text.strip():
            logger.warning("빈 텍스트로 인해 임베딩 생성을 건너뜁니다.")
            return None
        
        for attempt in range(self.config["max_retries"]):
            try:
                response = await self.openai_client.embeddings.create(
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
                    await asyncio.sleep(self.config["retry_delay"] * (2 ** attempt))
                else:
                    logger.error(f"임베딩 생성 최종 실패: {str(e)}")
                    return None
        return None
    
    async def generate_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """여러 텍스트에 대한 배치 임베딩 생성"""
        if not texts:
            return []
        
        valid_texts = [t.strip() for t in texts if t and t.strip()]
        valid_indices = [i for i, t in enumerate(texts) if t and t.strip()]
        
        if not valid_texts:
            logger.warning("유효한 텍스트가 없어 배치 임베딩 생성을 건너뜁니다.")
            return [None] * len(texts)
        
        results = [None] * len(texts)
        
        for attempt in range(self.config["max_retries"]):
            try:
                response = await self.openai_client.embeddings.create(
                    model=self.config["embedding_model"],
                    input=valid_texts,
                    dimensions=self.config["embedding_dimensions"]
                )
                for i, embedding_data in enumerate(response.data):
                    original_index = valid_indices[i]
                    results[original_index] = embedding_data.embedding
                
                logger.info(f"배치 임베딩 생성 성공 ({len(valid_texts)}개 텍스트)")
                return results
            except Exception as e:
                logger.warning(f"배치 임베딩 생성 시도 {attempt + 1} 실패: {str(e)}")
                if attempt < self.config["max_retries"] - 1:
                    await asyncio.sleep(self.config["retry_delay"] * (2 ** attempt))
                else:
                    logger.error(f"배치 임베딩 생성 최종 실패: {str(e)}")
                    return await self._generate_embeddings_individually(texts)
        return results

    async def _generate_embeddings_individually(self, texts: List[str]) -> List[Optional[List[float]]]:
        """개별 텍스트에 대한 임베딩 생성 (폴백)"""
        logger.info("개별 임베딩 생성으로 폴백합니다.")
        tasks = [self.generate_embedding(text) for text in texts]
        results = await asyncio.gather(*tasks)
        return results
    
    async def process_embeddings(self, batch_size: int = None, max_articles: int = None):
        """임베딩 처리 메인 함수"""
        if batch_size is None:
            batch_size = self.config["batch_size"]
        
        self.start_time = datetime.now()
        
        console.print(Panel.fit(
            "[bold blue]🤖 클러스터링용 임베딩 생성 시작 (비동기)[/bold blue]\n"
            f"모델: {self.config['embedding_model']}\n"
            f"차원: {self.config['embedding_dimensions']}\n"
            f"배치 크기: {batch_size}",
            title="임베딩 처리 설정"
        ))
        
        try:
            total_articles = await self.supabase.get_total_articles_count()
            if total_articles == 0:
                console.print("❌ 임베딩 가능한 기사가 없습니다.")
                return
            
            if max_articles:
                total_articles = min(total_articles, max_articles)
            
            console.print(f"📊 처리 대상: {total_articles:,}개 기사")
            
            with Progress(
                SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(), console=console
            ) as progress:
                
                main_task = progress.add_task(f"임베딩 처리 중... (0/{total_articles})", total=total_articles)
                
                offset = 0
                while offset < total_articles:
                    current_batch_size = min(batch_size, total_articles - offset)
                    
                    articles = await self.supabase.get_articles_for_embedding(offset, current_batch_size)
                    if not articles: break
                    
                    article_ids = [article['id'] for article in articles]
                    existing_ids = await self.supabase.check_existing_embeddings(article_ids, self.config["embedding_types"]["CLUSTERING"])
                    
                    new_articles = [article for article in articles if article['id'] not in existing_ids]
                    
                    if new_articles:
                        await self._process_batch(new_articles, progress, main_task)
                    else:
                        console.print(f"⏭️  배치 {offset//batch_size + 1}: 모든 기사가 이미 임베딩됨")
                        self.processed_count += len(articles) # Update progress for skipped articles
                        progress.update(main_task, completed=self.processed_count)


                    offset += len(articles)
                    await asyncio.sleep(0.5)
            
            await self._print_final_results()
            
        except Exception as e:
            logger.error(f"임베딩 처리 중 오류 발생: {str(e)}", exc_info=True)
            console.print(f"❌ 오류 발생: {str(e)}")
    
    async def _process_batch(self, articles: List[Dict[str, Any]], progress: Progress, main_task: TaskID):
        """배치 단위 임베딩 처리"""
        try:
            texts = [article.get('merged_content', '') for article in articles]
            embeddings = await self.generate_embeddings_batch(texts)
            
            embedding_records = []
            for article, embedding in zip(articles, embeddings):
                if embedding:
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
            
            if embedding_records:
                success = await self.supabase.save_embeddings(embedding_records)
                if not success:
                    logger.error("임베딩 저장 실패")
            
            self.processed_count += len(articles)
            progress.update(main_task, completed=self.processed_count, description=f"임베딩 처리 중... ({self.processed_count}/{progress.tasks[main_task].total})")
            console.print(f"✅ 배치 처리 완료: {len(embedding_records)}개 임베딩 저장 (성공: {self.success_count}, 실패: {self.error_count})")
            
        except Exception as e:
            logger.error(f"배치 처리 중 오류: {str(e)}", exc_info=True)
            console.print(f"❌ 배치 처리 오류: {str(e)}")
    
    async def _print_final_results(self):
        """최종 결과 출력"""
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        stats_table = Table(title="임베딩 처리 결과")
        stats_table.add_column("항목", style="cyan")
        stats_table.add_column("값", style="green")
        
        stats_table.add_row("처리된 기사", f"{self.processed_count:,}개")
        stats_table.add_row("성공", f"{self.success_count:,}개")
        stats_table.add_row("실패", f"{self.error_count:,}개")
        stats_table.add_row("소요 시간", str(duration).split('.')[0])
        
        if duration.total_seconds() > 0:
            stats_table.add_row("처리 속도", f"{self.processed_count/duration.total_seconds():.1f}개/초")
        
        console.print(stats_table)
        
        db_stats = await self.supabase.get_embedding_statistics()
        console.print("\n📊 현재 데이터베이스 임베딩 현황:")
        console.print(f"• 전체 임베딩: {db_stats['total_embeddings']:,}개")
        console.print(f"• 클러스터링용: {db_stats['clustering_embeddings']:,}개")
        console.print(f"• 오늘 생성: {db_stats['today_embeddings']:,}개")

async def main():
    """메인 실행 함수"""
    try:
        processor = EmbeddingProcessor()
        
        console.print("\n[bold yellow]임베딩 처리 옵션을 선택하세요:[/bold yellow]")
        console.print("1. 전체 기사 처리 (기본)")
        console.print("2. 제한된 수의 기사 처리")
        console.print("3. 통계만 확인")
        
        choice = input("\n선택 (1-3): ").strip()
        
        if choice == "2":
            max_articles_str = input("처리할 최대 기사 수를 입력하세요: ")
            try:
                max_articles = int(max_articles_str)
                await processor.process_embeddings(max_articles=max_articles)
            except ValueError:
                console.print("❌ 유효한 숫자를 입력하세요.")
        elif choice == "3":
            db_stats = await processor.supabase.get_embedding_statistics()
            console.print(Panel.fit(
                f"[bold blue]📊 임베딩 현황[/bold blue]\n\n"
                f"전체 임베딩: {db_stats['total_embeddings']:,}개\n"
                f"클러스터링용: {db_stats['clustering_embeddings']:,}개\n"
                f"오늘 생성: {db_stats['today_embeddings']:,}개"
            ))
        else:
            await processor.process_embeddings()
            
    except KeyboardInterrupt:
        console.print("\n⚠️  사용자에 의해 중단되었습니다.")
    except Exception as e:
        console.print(f"❌ 실행 중 오류 발생: {str(e)}")
        logger.error(f"메인 실행 오류: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())