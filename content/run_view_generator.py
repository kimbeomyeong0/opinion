#!/usr/bin/env python3
"""
LLM 기반 지능형 View 생성기
이슈별 3가지 성향의 균형잡힌 관점 생성
"""

import sys
import os
import json
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Any, Optional

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utils.supabase_manager import SupabaseManager

# View Generator 모듈들 import
sys.path.append(os.path.join(os.path.dirname(__file__), 'view_generator'))
from issue_analyzer import LLMBasedIssueAnalyzer
from bias_interpreter import LLMBasedBiasInterpreter
from prompt_generator import IntelligentPromptGenerator
from quality_checker import LLMBasedQualityChecker

# OpenAI 설치 확인 및 import
try:
    import openai
except ImportError:
    print("❌ OpenAI가 설치되지 않았습니다.")
    print("설치 명령: pip install openai")
    sys.exit(1)

class IntelligentViewGenerator:
    """지능형 관점 생성 클래스"""
    
    def __init__(self):
        """초기화"""
        # OpenAI API 설정
        self.MODEL_NAME = "gpt-4o-mini"
        self.MAX_TOKENS = 1000
        self.TEMPERATURE = 0.3
        
        # Supabase 연결
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
        
        # OpenAI API 키 설정
        if not os.getenv('OPENAI_API_KEY'):
            raise Exception("OPENAI_API_KEY 환경변수가 설정되지 않았습니다.")
        
        # OpenAI 클라이언트 초기화
        from openai import OpenAI
        self.openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        # 모듈들 초기화
        self.issue_analyzer = LLMBasedIssueAnalyzer(self.openai_client)
        self.bias_interpreter = LLMBasedBiasInterpreter(self.openai_client)
        self.prompt_generator = IntelligentPromptGenerator(self.openai_client)
        self.quality_checker = LLMBasedQualityChecker(self.openai_client)
    
    def fetch_issue_data(self, issue_id: str) -> Optional[Dict]:
        """이슈 정보 조회"""
        try:
            result = self.supabase_manager.client.table('issues').select(
                'id, title, subtitle, background'
            ).eq('id', issue_id).execute()
            
            if result.data:
                return result.data[0]
            return None
            
        except Exception as e:
            print(f"❌ 이슈 정보 조회 실패: {str(e)}")
            return None
    
    def fetch_issue_articles(self, issue_id: str) -> Optional[List[Dict]]:
        """이슈의 기사들 조회"""
        try:
            print(f"📡 이슈 {issue_id}의 기사 데이터 조회 중...")
            
            result = self.supabase_manager.client.table('issue_articles').select(
                'article_id, cleaned_article_id, '
                'articles!inner(id, title, media_id, media_outlets!inner(name, bias)), '
                'articles_cleaned!inner(merged_content)'
            ).eq('issue_id', issue_id).execute()
            
            if not result.data:
                print(f"❌ 이슈 {issue_id}에 연결된 기사가 없습니다.")
                return None
            
            # 데이터 정리
            articles_data = []
            for item in result.data:
                article = item['articles']
                media = article['media_outlets']
                cleaned = item['articles_cleaned']
                
                articles_data.append({
                    'title': article['title'],
                    'merged_content': cleaned['merged_content'],
                    'media_name': media['name'],
                    'bias': media['bias']
                })
            
            print(f"✅ {len(articles_data)}개 기사 데이터 조회 완료")
            return articles_data
            
        except Exception as e:
            print(f"❌ 기사 데이터 조회 실패: {str(e)}")
            return None
    
    def generate_view_for_bias(self, issue_data: Dict[str, Any], articles_data: List[Dict], 
                             bias: str, issue_characteristics: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """특정 성향의 관점 생성"""
        try:
            print(f"🔍 {bias} 성향 관점 생성 시작...")
            
            # 1. 성향 해석
            bias_interpretation = self.bias_interpreter.interpret_bias_in_context(
                bias, issue_characteristics, articles_data
            )
            print(f"🎯 {bias} 성향 해석 완료")
            
            # 2. 프롬프트 생성
            prompt = self.prompt_generator.generate_adaptive_prompt(
                issue_data, articles_data, bias, issue_characteristics, bias_interpretation
            )
            print(f"📝 {bias} 프롬프트 생성 완료")
            
            # 3. LLM 호출
            response = self.openai_client.chat.completions.create(
                model=self.MODEL_NAME,
                messages=[
                    {"role": "system", "content": f"당신은 {bias} 성향의 경험이 풍부한 언론인입니다. 자연스럽고 읽기 쉬운 관점을 생성합니다."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.MAX_TOKENS,
                temperature=self.TEMPERATURE
            )
            
            # 4. 응답 파싱
            content = response.choices[0].message.content.strip()
            
            import re
            title_match = re.search(r'제목:\s*(.+)', content)
            content_match = re.search(r'내용:\s*(.+)', content, re.DOTALL)
            
            if title_match and content_match:
                title = title_match.group(1).strip()
                view_content = content_match.group(1).strip()
                
                print(f"🤖 {bias} 관점 생성 완료: {title[:30]}...")
                
                # 5. 품질 검증
                quality_passed, quality_results = self.quality_checker.validate_view_quality(
                    title, view_content, bias, issue_characteristics
                )
                
                print(f"✅ {bias} 품질 검증: {'통과' if quality_passed else '미통과'} (점수: {quality_results['total_score']}/60)")
                
                return {
                    "title": title,
                    "content": view_content,
                    "quality_passed": quality_passed,
                    "quality_score": quality_results['total_score'],
                    "quality_grade": quality_results.get('grade', 'C')
                }
            else:
                print(f"❌ {bias} 관점 파싱 실패")
                return None
                
        except Exception as e:
            print(f"❌ {bias} 관점 생성 실패: {str(e)}")
            return None
    
    def generate_views_for_issue(self, issue_id: str) -> Dict[str, Any]:
        """이슈별 3가지 관점 생성"""
        try:
            print(f"\n🔍 이슈 {issue_id} 지능형 관점 생성 시작...")
            
            # 1. 데이터 준비
            issue_data = self.fetch_issue_data(issue_id)
            if not issue_data:
                return {}
            
            articles_data = self.fetch_issue_articles(issue_id)
            if not articles_data:
                return {}
            
            print(f"📋 이슈: {issue_data['title']}")
            print(f"📰 기사 수: {len(articles_data)}개")
            
            # 2. 이슈 특성 분석
            issue_characteristics = self.issue_analyzer.analyze_issue_characteristics(issue_data, articles_data)
            print(f"📊 이슈 특성: {issue_characteristics['issue_type']} | {issue_characteristics['complexity']} 복잡도")
            
            # 3. 성향별 관점 병렬 생성
            views = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                futures = {}
                
                for bias in ['left', 'center', 'right']:
                    futures[bias] = executor.submit(
                        self.generate_view_for_bias, 
                        issue_data, articles_data, bias, issue_characteristics
                    )
                
                # 결과 수집
                for bias, future in futures.items():
                    try:
                        result = future.result(timeout=120)  # 120초 타임아웃
                        if result:
                            views[bias] = result
                            print(f"✅ {bias} 관점 생성 완료 (품질: {result['quality_grade']})")
                        else:
                            print(f"❌ {bias} 관점 생성 실패")
                    except Exception as e:
                        print(f"❌ {bias} 관점 생성 실패: {str(e)}")
            
            return views
            
        except Exception as e:
            print(f"❌ 이슈 {issue_id} 관점 생성 실패: {str(e)}")
            return {}
    
    def update_issues_table(self, issue_id: str, views: Dict[str, Any]) -> bool:
        """issues 테이블 업데이트"""
        try:
            update_data = {}
            
            for bias, view_result in views.items():
                if isinstance(view_result, dict) and 'title' in view_result:
                    if bias == 'left':
                        update_data['left_view'] = f"{view_result['title']}|||{view_result['content']}"
                    elif bias == 'center':
                        update_data['center_view'] = f"{view_result['title']}|||{view_result['content']}"
                    elif bias == 'right':
                        update_data['right_view'] = f"{view_result['title']}|||{view_result['content']}"
            
            if not update_data:
                print("❌ 업데이트할 관점이 없습니다.")
                return False
            
            result = self.supabase_manager.client.table('issues').update(
                update_data
            ).eq('id', issue_id).execute()
            
            if result.data:
                print(f"✅ 이슈 {issue_id} 관점 업데이트 완료")
                return True
            else:
                print(f"❌ 이슈 {issue_id} 관점 업데이트 실패")
                return False
                
        except Exception as e:
            print(f"❌ 이슈 {issue_id} 관점 업데이트 실패: {str(e)}")
            return False
    
    def process_issue(self, issue_id: str) -> bool:
        """단일 이슈 처리"""
        try:
            print(f"\n📋 이슈 {issue_id} 지능형 처리 시작...")
            
            # 관점 생성
            views = self.generate_views_for_issue(issue_id)
            
            if not views:
                print(f"❌ 이슈 {issue_id} 관점 생성 실패")
                return False
            
            # 품질 보고서 출력
            self._print_quality_report(views)
            
            # DB 업데이트
            success = self.update_issues_table(issue_id, views)
            
            if success:
                print(f"✅ 이슈 {issue_id} 처리 완료")
                return True
            else:
                print(f"❌ 이슈 {issue_id} 처리 실패")
                return False
                
        except Exception as e:
            print(f"❌ 이슈 {issue_id} 처리 실패: {str(e)}")
            return False
    
    def _print_quality_report(self, views: Dict[str, Any]):
        """품질 보고서 출력"""
        print("\n" + "="*60)
        print("📊 관점 품질 보고서")
        print("="*60)
        
        for bias, view_result in views.items():
            if isinstance(view_result, dict) and 'title' in view_result:
                print(f"\n🎭 {bias.upper()} 성향:")
                print(f"  제목: {view_result['title']}")
                print(f"  품질: {view_result['quality_grade']} ({view_result['quality_score']}/60)")
                print(f"  통과: {'✅' if view_result['quality_passed'] else '❌'}")
                print(f"  내용: {view_result['content'][:100]}...")
        
        print("="*60)
    
    def process_single_issue(self) -> bool:
        """첫 번째 이슈만 처리 (테스트용)"""
        try:
            print("🚀 첫 번째 이슈의 지능형 관점 생성 시작...")
            
            # 첫 번째 이슈 조회
            result = self.supabase_manager.client.table('issues').select('id, title').limit(1).execute()
            
            if not result.data:
                print("❌ 처리할 이슈가 없습니다.")
                return False
            
            issue = result.data[0]
            issue_id = issue['id']
            current_title = issue['title']
            
            print(f"🔍 이슈 {issue_id} 처리 시작...")
            
            # 기존 제목이 있으면 덮어쓰기 진행
            if current_title and not current_title.startswith('이슈 '):
                print(f"🔄 이슈 {issue_id} 덮어쓰기 진행 (기존 제목: {current_title})")
            
            success = self.process_issue(issue_id)
            
            if success:
                print("✅ 테스트 성공!")
            else:
                print("❌ 테스트 실패!")
            
            return success
            
        except Exception as e:
            print(f"❌ 테스트 실패: {str(e)}")
            return False
    
    def process_all_issues(self) -> bool:
        """모든 이슈 처리"""
        try:
            print("🚀 모든 이슈의 지능형 관점 생성 시작...")
            
            # 모든 이슈 조회
            result = self.supabase_manager.client.table('issues').select('id, title').execute()
            
            if not result.data:
                print("❌ 처리할 이슈가 없습니다.")
                return False
            
            print(f"📋 총 {len(result.data)}개 이슈 처리 예정")
            
            success_count = 0
            failed_count = 0
            
            for issue in result.data:
                issue_id = issue['id']
                current_title = issue['title']
                
                print(f"\n📋 처리 중: {current_title}")
                
                # 기존 제목이 있으면 덮어쓰기 진행
                if current_title and not current_title.startswith('이슈 '):
                    print(f"🔄 이슈 {issue_id} 덮어쓰기 진행 (기존 제목: {current_title})")
                
                success = self.process_issue(issue_id)
                if success:
                    success_count += 1
                else:
                    failed_count += 1
            
            print(f"\n📊 처리 결과:")
            print(f"  - 성공: {success_count}개")
            print(f"  - 실패: {failed_count}개")
            
            return success_count > 0
            
        except Exception as e:
            print(f"❌ 전체 처리 실패: {str(e)}")
            return False

def test_single_issue():
    """단일 이슈 테스트 함수"""
    print("=" * 60)
    print("🧪 단일 이슈 지능형 관점 테스트 모드")
    print("=" * 60)
    
    try:
        # 생성기 초기화
        generator = IntelligentViewGenerator()
        
        # 단일 이슈 처리
        success = generator.process_single_issue()
        
        if success:
            print("\n✅ 단일 이슈 테스트 완료!")
        else:
            print("\n❌ 단일 이슈 테스트 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")

def main():
    """메인 함수"""
    print("=" * 60)
    print("🎭 LLM 기반 지능형 View 생성 시스템")
    print("=" * 60)
    
    try:
        # 생성기 초기화
        generator = IntelligentViewGenerator()
        
        # 모든 이슈 처리
        success = generator.process_all_issues()
        
        if success:
            print("\n✅ 지능형 관점 생성 완료!")
        else:
            print("\n❌ 지능형 관점 생성 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    import sys
    
    # 명령행 인수로 테스트 모드 확인
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_single_issue()
    else:
        main()
