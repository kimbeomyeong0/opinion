#!/usr/bin/env python3
"""
맥락 기반 관점 생성 모듈
이슈 특성과 맥락을 고려한 지능형 관점 생성 시스템
"""

import sys
import os
import json
import re
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Any, Optional

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utils.supabase_manager import SupabaseManager
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'contextual_view_system'))

from issue_analyzer import IssueAnalyzer
from contextual_bias_interpreter import ContextualBiasInterpreter
from multi_layer_view_generator import MultiLayerViewGenerator
from intelligent_prompt_generator import IntelligentPromptGenerator
from view_quality_checker import ViewQualityChecker

# OpenAI 설치 확인 및 import
try:
    import openai
except ImportError:
    print("❌ OpenAI가 설치되지 않았습니다.")
    print("설치 명령: pip install openai")
    sys.exit(1)

class ContextualViewGenerator:
    """맥락 기반 관점 생성 클래스"""
    
    def __init__(self):
        """초기화"""
        # OpenAI API 설정
        self.MODEL_NAME = "gpt-4o-mini"
        self.MAX_TOKENS = 1000
        self.TEMPERATURE = 0.7
        
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
        
        # 새로운 모듈들 초기화
        self.issue_analyzer = IssueAnalyzer()
        self.bias_interpreter = ContextualBiasInterpreter()
        self.multi_layer_generator = MultiLayerViewGenerator()
        self.prompt_generator = IntelligentPromptGenerator()
        self.quality_checker = ViewQualityChecker()
    
    def fetch_issue_info(self, issue_id: str) -> Optional[Dict]:
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
    
    def fetch_articles_by_bias(self, issue_id: str, bias: str) -> List[Dict]:
        """성향별 기사 조회"""
        try:
            result = self.supabase_manager.client.table('issue_articles').select(
                """
                articles!inner(
                    id, content, published_at,
                    media_outlets!inner(
                        id, name, bias
                    )
                )
                """
            ).eq('issue_id', issue_id).execute()
            
            if not result.data:
                return []
            
            # 성향별 필터링
            articles = []
            for item in result.data:
                article = item['articles']
                media_bias = article['media_outlets']['bias']
                
                if media_bias == bias:
                    articles.append({
                        'id': article['id'],
                        'content': article['content'],
                        'published_at': article['published_at'],
                        'media_name': article['media_outlets']['name']
                    })
            
            return articles
            
        except Exception as e:
            print(f"❌ {bias} 성향 기사 조회 실패: {str(e)}")
            return []
    
    def generate_contextual_view(self, issue_data: Dict[str, Any], articles_data: List[Dict], bias: str) -> Optional[Dict[str, Any]]:
        """
        맥락 기반 관점 생성
        
        Args:
            issue_data: 이슈 데이터
            articles_data: 기사 데이터
            bias: 성향
            
        Returns:
            Dict: 생성된 관점과 품질 정보
        """
        try:
            print(f"🔍 {bias} 성향 맥락 기반 관점 생성 시작...")
            
            # 1. 이슈 특성 분석
            issue_characteristics = self.issue_analyzer.analyze_issue_characteristics(issue_data)
            print(f"📊 이슈 특성: {issue_characteristics['issue_type']} | {issue_characteristics['complexity_level']} 복잡도")
            
            # 2. 맥락 기반 성향 해석
            bias_interpretation = self.bias_interpreter.interpret_bias_in_context(bias, issue_characteristics)
            print(f"🎯 {bias} 성향 맥락적 해석 완료")
            
            # 3. 지능형 프롬프트 생성
            prompt = self.prompt_generator.generate_adaptive_prompt(issue_data, articles_data, bias)
            print(f"📝 적응형 프롬프트 생성 완료 (길이: {len(prompt)}자)")
            
            # 4. LLM을 통한 관점 생성
            view_text = self._generate_view_with_llm(prompt, bias)
            if not view_text:
                return None
            
            print(f"🤖 LLM 관점 생성 완료: {view_text[:50]}...")
            
            # 5. 품질 검증
            quality_passed, validation_results = self.quality_checker.validate_view_quality(
                view_text, bias, issue_characteristics
            )
            
            print(f"✅ 품질 검증: {'통과' if quality_passed else '미통과'} (점수: {validation_results['overall']['total_score']:.2f})")
            
            # 6. 다층적 관점 구조 생성 (선택적)
            multi_layer_view = None
            if len(view_text) > 50:  # 충분히 긴 경우에만 다층 구조 시도
                try:
                    multi_layer_view = self.multi_layer_generator.generate_multi_layer_view(
                        issue_data, articles_data, bias
                    )
                except Exception as e:
                    print(f"⚠️ 다층적 관점 생성 실패: {str(e)}")
            
            # 7. 결과 구성
            result = {
                "view_text": view_text,
                "quality_passed": quality_passed,
                "quality_score": validation_results['overall']['total_score'],
                "quality_grade": validation_results['overall']['grade'],
                "issue_characteristics": issue_characteristics,
                "bias_interpretation": bias_interpretation,
                "multi_layer_view": multi_layer_view,
                "validation_details": validation_results,
                "generated_at": datetime.now().isoformat()
            }
            
            return result
            
        except Exception as e:
            print(f"❌ {bias} 관점 생성 실패: {str(e)}")
            return None
    
    def _generate_view_with_llm(self, prompt: str, bias: str) -> Optional[str]:
        """LLM을 통한 관점 생성"""
        try:
            response = self.openai_client.chat.completions.create(
                model=self.MODEL_NAME,
                messages=[
                    {"role": "system", "content": f"당신은 {bias} 성향의 정치 분석가입니다. 이슈의 맥락을 고려하여 균형잡힌 관점을 제시합니다."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.MAX_TOKENS,
                temperature=self.TEMPERATURE
            )
            
            # 응답 파싱
            content = response.choices[0].message.content.strip()
            
            # 관점 추출
            view_match = re.search(rf'{bias} 관점:\s*(.+)', content)
            if not view_match:
                view_match = re.search(rf'{bias} 성향의 관점:\s*(.+)', content)
            if not view_match:
                view_match = re.search(r'^(.+)$', content.strip())
            
            if view_match:
                view = view_match.group(1).strip()
                return view
            else:
                print(f"❌ {bias} 관점 추출 실패")
                return None
                    
        except Exception as e:
            print(f"❌ {bias} 관점 LLM 생성 실패: {str(e)}")
            return None
    
    def generate_views_parallel(self, issue_id: str) -> Dict[str, Any]:
        """성향별 관점 병렬 생성"""
        try:
            print(f"\n🔍 이슈 {issue_id} 맥락 기반 관점 생성 시작...")
            
            # 이슈 정보 조회
            issue_info = self.fetch_issue_info(issue_id)
            if not issue_info:
                return {}
            
            print(f"📋 이슈: {issue_info['title']}")
            
            # 성향별 기사들 조회
            left_articles = self.fetch_articles_by_bias(issue_id, 'left')
            center_articles = self.fetch_articles_by_bias(issue_id, 'center')
            right_articles = self.fetch_articles_by_bias(issue_id, 'right')
            
            print(f"📰 기사 수: 좌파 {len(left_articles)}개, 중립 {len(center_articles)}개, 우파 {len(right_articles)}개")
            
            # 병렬 처리로 관점 생성
            views = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                futures = {}
                
                # 각 성향별로 관점 생성 작업 제출
                if left_articles:
                    futures['left'] = executor.submit(
                        self.generate_contextual_view, issue_info, left_articles, 'left'
                    )
                
                if center_articles:
                    futures['center'] = executor.submit(
                        self.generate_contextual_view, issue_info, center_articles, 'center'
                    )
                
                if right_articles:
                    futures['right'] = executor.submit(
                        self.generate_contextual_view, issue_info, right_articles, 'right'
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
                if isinstance(view_result, dict) and 'view_text' in view_result:
                    if bias == 'left':
                        update_data['left_view'] = view_result['view_text']
                    elif bias == 'center':
                        update_data['center_view'] = view_result['view_text']
                    elif bias == 'right':
                        update_data['right_view'] = view_result['view_text']
            
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
            print(f"\n📋 이슈 {issue_id} 맥락 기반 처리 시작...")
            
            # 관점 생성
            views = self.generate_views_parallel(issue_id)
            
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
            if isinstance(view_result, dict) and 'validation_details' in view_result:
                print(f"\n🎭 {bias.upper()} 성향:")
                print(f"  관점: {view_result['view_text']}")
                print(f"  품질: {view_result['quality_grade']} ({view_result['quality_score']:.2f})")
                print(f"  통과: {'✅' if view_result['quality_passed'] else '❌'}")
                
                # 이슈 특성 정보
                characteristics = view_result.get('issue_characteristics', {})
                print(f"  이슈 유형: {characteristics.get('issue_type', 'N/A')}")
                print(f"  복잡도: {characteristics.get('complexity_level', 'N/A')}")
        
        print("="*60)
    
    def process_all_issues(self) -> bool:
        """모든 이슈 처리"""
        try:
            print("🚀 모든 이슈의 맥락 기반 관점 생성 시작...")
            
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
    print("🧪 단일 이슈 맥락 기반 테스트 모드")
    print("=" * 60)
    
    try:
        # 생성기 초기화
        generator = ContextualViewGenerator()
        
        # 첫 번째 이슈 조회
        result = generator.supabase_manager.client.table('issues').select('id, title').limit(1).execute()
        
        if not result.data:
            print("❌ 테스트할 이슈가 없습니다.")
            return
        
        issue_id = result.data[0]['id']
        issue_title = result.data[0]['title']
        
        print(f"📋 테스트 이슈: {issue_title} (ID: {issue_id})")
        
        # 단일 이슈 처리
        success = generator.process_issue(issue_id)
        
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
    print("🎭 맥락 기반 관점 생성 시스템")
    print("=" * 60)
    
    try:
        # 생성기 초기화
        generator = ContextualViewGenerator()
        
        # 모든 이슈 처리
        success = generator.process_all_issues()
        
        if success:
            print("\n✅ 맥락 기반 관점 생성 완료!")
        else:
            print("\n❌ 맥락 기반 관점 생성 실패!")
            
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
