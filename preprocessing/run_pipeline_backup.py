#!/usr/bin/env python3
"""
전처리 파이프라인 실행 스크립트
- 명령행 인터페이스 제공
- 단계별 또는 전체 실행 지원
- 진행 상황 실시간 출력
"""

import argparse
import sys
import os
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from preprocessing.pipeline import PreprocessingPipeline

def print_banner():
    """배너 출력"""
    print("=" * 70)
    print("🔄 뉴스 기사 전처리 파이프라인")
    print("=" * 70)
    print(f"📅 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

def print_help():
    """도움말 출력"""
    print("📖 사용법:")
    print("  python3 preprocessing/run_pipeline.py [옵션]")
    print()
    print("🔧 옵션:")
    print("  --all                    전체 파이프라인 실행 (기본값)")
    print("  --stage STAGE_NAME       특정 단계만 실행")
    print("  --skip STAGE_NAME        특정 단계 건너뛰기")
    print("  --status                 현재 상태만 확인")
    print("  --help                   이 도움말 출력")
    print()
    print("📝 사용 가능한 단계:")
    print("  duplicate_removal        1단계: 중복 제거 + 기본 필터링")
    print("  text_cleaning            2단계: 텍스트 정제")
    print("  text_normalization       3단계: 텍스트 정규화")  
    print("  content_merging          4단계: 제목+본문 통합")
    print()
    print("💡 예시:")
    print("  python3 preprocessing/run_pipeline.py --all")
    print("  python3 preprocessing/run_pipeline.py --stage duplicate_removal")
    print("  python3 preprocessing/run_pipeline.py --skip text_cleaning")
    print("  python3 preprocessing/run_pipeline.py --status")

def print_status(pipeline):
    """현재 상태 출력"""
    print("📊 현재 파이프라인 상태")
    print("-" * 30)
    
    status = pipeline.get_pipeline_status()
    
    if not status.get('pipeline_ready', False):
        print(f"❌ 파이프라인 오류: {status.get('error', 'Unknown error')}")
        return
    
    print(f"📰 전체 기사: {status.get('articles_total', 0):,}개")
    print(f"🔄 전처리된 기사: {status.get('articles_preprocessed', 0):,}개")
    print(f"🧹 정제된 기사: {status.get('cleaned_articles', 0):,}개")
    print(f"🔗 통합된 기사: {status.get('merged_articles', 0):,}개")
    
    # 진행률 계산
    total = status.get('articles_total', 0)
    if total > 0:
        preprocessed_rate = (status.get('articles_preprocessed', 0) / total) * 100
        cleaned_rate = (status.get('cleaned_articles', 0) / total) * 100
        merged_rate = (status.get('merged_articles', 0) / total) * 100
        
        print()
        print("📈 진행률:")
        print(f"  전처리: {preprocessed_rate:.1f}%")
        print(f"  정제: {cleaned_rate:.1f}%")
        print(f"  통합: {merged_rate:.1f}%")

def print_stage_result(result):
    """단계 결과 출력"""
    print(f"\n📋 {result.stage} 결과:")
    print(f"  상태: {'✅ 성공' if result.success else '❌ 실패'}")
    print(f"  처리된 기사: {result.processed_articles:,}개 / {result.total_articles:,}개")
    print(f"  처리 시간: {result.processing_time:.2f}초")
    print(f"  메시지: {result.message}")
    
    if result.metadata:
        print("  📊 상세 정보:")
        for key, value in result.metadata.items():
            print(f"    {key}: {value}")
    
    if result.error_message:
        print(f"  ❌ 오류: {result.error_message}")

def print_full_result(result):
    """전체 결과 출력"""
    print("\n" + "=" * 50)
    print("🎯 전체 파이프라인 결과")
    print("=" * 50)
    
    print(f"전체 성공: {'✅ YES' if result.overall_success else '❌ NO'}")
    print(f"총 실행 시간: {result.total_execution_time:.2f}초")
    print(f"최종 기사 수: {result.final_article_count:,}개")
    print(f"완료된 단계: {len(result.stages_completed)}개")
    print(f"실패한 단계: {len(result.stages_failed)}개")
    
    if result.stages_completed:
        print(f"\n✅ 완료된 단계:")
        for stage in result.stages_completed:
            print(f"  - {stage}")
    
    if result.stages_failed:
        print(f"\n❌ 실패한 단계:")
        for stage in result.stages_failed:
            print(f"  - {stage}")
    
    print("\n📊 단계별 상세 결과:")
    for stage_name, stage_result in result.stage_results.items():
        print(f"\n{stage_name}:")
        print(f"  성공: {'✅' if stage_result.success else '❌'}")
        print(f"  처리: {stage_result.processed_articles}/{stage_result.total_articles}")
        print(f"  시간: {stage_result.processing_time:.2f}초")

def validate_environment():
    """실행 환경 검증"""
    try:
        # Python 버전 확인
        import sys
        if sys.version_info < (3, 8):
            print("❌ Python 3.8 이상이 필요합니다.")
            return False
        
        # 필수 모듈 확인
        required_modules = ['supabase', 'dataclasses', 'typing']
        missing_modules = []
        
        for module in required_modules:
            try:
                __import__(module)
            except ImportError:
                missing_modules.append(module)
        
        if missing_modules:
            print(f"❌ 필수 모듈이 없습니다: {', '.join(missing_modules)}")
            print("다음 명령어로 설치하세요: pip install -r requirements.txt")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ 환경 검증 실패: {e}")
        return False

def main():
    # 환경 검증
    if not validate_environment():
        return 1
    
    parser = argparse.ArgumentParser(
        description='뉴스 기사 전처리 파이프라인',
        add_help=False  # 커스텀 help 사용
    )
    
    parser.add_argument('--all', action='store_true', 
                       help='전체 파이프라인 실행 (기본값)')
    parser.add_argument('--stage', type=str, 
                       help='특정 단계만 실행')
    parser.add_argument('--skip', type=str, action='append',
                       help='특정 단계 건너뛰기 (여러 번 사용 가능)')
    parser.add_argument('--status', action='store_true',
                       help='현재 상태만 확인')
    parser.add_argument('--help', action='store_true',
                       help='도움말 출력')
    parser.add_argument('--dry-run', action='store_true',
                       help='실제 실행 없이 시뮬레이션만 실행')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='상세 로그 출력')
    
    try:
        args = parser.parse_args()
    except SystemExit:
        return 1
    
    # 도움말 출력
    if args.help:
        print_banner()
        print_help()
        return 0
    
    # 파이프라인 초기화
    try:
        pipeline = PreprocessingPipeline()
    except Exception as e:
        print(f"❌ 파이프라인 초기화 실패: {e}")
        print("💡 해결 방법:")
        print("  1. Supabase 연결 설정을 확인하세요")
        print("  2. 환경 변수 SUPABASE_URL, SUPABASE_KEY가 설정되었는지 확인하세요")
        print("  3. 네트워크 연결을 확인하세요")
        return 1
    
    print_banner()
    
    # 상태 확인만 실행
    if args.status:
        try:
            print_status(pipeline)
            return 0
        except Exception as e:
            print(f"❌ 상태 확인 실패: {e}")
            return 1
    
    # 단계별 실행
    if args.stage:
        available_stages = ['duplicate_removal', 'text_cleaning', 'text_normalization', 'content_merging']
        
        if args.stage not in available_stages:
            print(f"❌ 알 수 없는 단계: {args.stage}")
            print(f"사용 가능한 단계: {', '.join(available_stages)}")
            return 1
        
        print(f"🚀 단계별 실행: {args.stage}")
        
        try:
            print_status(pipeline)
            
            if args.dry_run:
                print("🔍 드라이런 모드: 실제 실행 없이 시뮬레이션만 수행")
                return 0
            
            result = pipeline.run_single_stage(args.stage)
            print_stage_result(result)
            
            return 0 if result.success else 1
            
        except Exception as e:
            print(f"❌ 단계 실행 실패: {e}")
            return 1
    
    # 전체 파이프라인 실행 (기본값)
    print("🚀 전체 파이프라인 실행")
    
    try:
        print_status(pipeline)
        
        skip_stages = args.skip or []
        if skip_stages:
            print(f"⏭️  건너뛸 단계: {', '.join(skip_stages)}")
        
        if args.dry_run:
            print("🔍 드라이런 모드: 실제 실행 없이 시뮬레이션만 수행")
            return 0
        
        print("\n" + "🔄 파이프라인 시작..." + "\n")
        
        result = pipeline.run_full_pipeline(skip_stages=skip_stages)
        print_full_result(result)
        
        return 0 if result.overall_success else 1
        
    except Exception as e:
        print(f"❌ 파이프라인 실행 실패: {e}")
        print("💡 해결 방법:")
        print("  1. 데이터베이스 연결을 확인하세요")
        print("  2. 충분한 디스크 공간이 있는지 확인하세요")
        print("  3. 메모리 사용량을 확인하세요")
        return 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n⏸️  사용자에 의해 중단됨")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 예상치 못한 오류: {e}")
        sys.exit(1)
