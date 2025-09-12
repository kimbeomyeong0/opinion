#!/usr/bin/env python3
"""
Module 2-4: View Generator
이슈별로 좌파, 중립, 우파 관점을 생성합니다.
articles.content 기반으로 LLM 처리하여 issues 테이블의 left_view, center_view, right_view를 업데이트합니다.
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

# OpenAI 설치 확인 및 import
try:
    import openai
except ImportError:
    print("❌ OpenAI가 설치되지 않았습니다.")
    print("설치 명령: pip install openai")
    sys.exit(1)

class ViewGenerator:
    """성향별 관점 생성 클래스"""
    
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
    
    def fetch_issue_info(self, issue_id: str) -> Optional[Dict]:
        """
        이슈 정보 조회
        
        Args:
            issue_id: 이슈 ID
            
        Returns:
            Dict: 이슈 정보 (title, subtitle)
        """
        try:
            result = self.supabase_manager.client.table('issues').select(
                'id, title, subtitle'
            ).eq('id', issue_id).execute()
            
            if result.data:
                return result.data[0]
            return None
            
        except Exception as e:
            print(f"❌ 이슈 정보 조회 실패: {str(e)}")
            return None
    
    def fetch_articles_by_bias(self, issue_id: str, bias: str) -> List[Dict]:
        """
        성향별 기사 조회
        
        Args:
            issue_id: 이슈 ID
            bias: 성향 (left, center, right)
            
        Returns:
            List[Dict]: 성향별 기사 목록
        """
        try:
            # issue_articles와 articles, media_outlets 조인하여 조회
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
    
    def create_prompt(self, articles_data: List[Dict], issue_info: Dict, bias: str) -> str:
        """
        프롬프트 생성
        
        Args:
            articles_data: 기사 데이터 목록
            issue_info: 이슈 정보
            bias: 성향
            
        Returns:
            str: 생성된 프롬프트
        """
        title = issue_info.get('title', '')
        subtitle = issue_info.get('subtitle', '')
        
        # 기사 내용들을 800자로 제한하여 결합 (모든 기사 참고)
        articles_text = ""
        print(f"📰 {bias} 성향 기사 {len(articles_data)}개 참고 중...")
        for i, article in enumerate(articles_data, 1):  # 모든 기사 참고
            content = article['content'][:800]  # 800자 제한
            media_name = article['media_name']
            articles_text += f"\n[기사 {i}] ({media_name})\n{content}\n"
        
        print(f"📝 프롬프트 길이: {len(articles_text)}자")
        
        # 중립 관점만 50자, 좌파/우파는 30자
        char_limit = 50 if bias == "center" else 30
        
        # 성향별 핵심 가치와 논조 정의
        bias_guidelines = {
            "left": {
                "values": "진보적 가치, 사회적 약자 보호, 평등과 정의, 정부 개입, 사회적 책임",
                "tone": "비판적, 개혁적, 사회정의 중심, 약자 편에서의 목소리",
                "stance": "정부와 기업의 책임 강조, 사회적 불평등 해결, 진보적 정책 지지"
            },
            "center": {
                "values": "균형과 절충, 실용주의, 합리적 접근, 양측 고려, 신중한 판단",
                "tone": "신중하고 균형잡힌, 객관적 분석, 양측 장단점 고려",
                "stance": "양측 입장을 모두 고려한 중도적 접근, 실질적 해결책 모색"
            },
            "right": {
                "values": "보수적 가치, 자유시장, 개인 책임, 전통과 질서, 효율성",
                "tone": "보수적, 실용적, 개인 책임 강조, 시장 원리 중시",
                "stance": "정부 개입 최소화, 개인과 기업의 자율성 강조, 보수적 정책 지지"
            }
        }
        
        guidelines = bias_guidelines[bias]
        
        prompt = f"""다음 이슈에 대한 {bias} 성향의 관점을 정확히 {char_limit}자로 작성해주세요.

이슈 제목: {title}
이슈 부제목: {subtitle}

관련 기사들 (총 {len(articles_data)}개):
{articles_text}

{bias.upper()} 성향의 핵심 가치: {guidelines['values']}
{bias.upper()} 성향의 논조: {guidelines['tone']}
{bias.upper()} 성향의 스탠스: {guidelines['stance']}

요구사항:
1. 위의 {bias} 성향 가치관에 따라 이슈를 분석하고 명확한 입장 제시
2. 정확히 {char_limit}자로 작성 (공백 포함, 절대 초과 금지)
3. 20대~30대 기준으로 어려운 정치용어는 풀어서 설명
4. 괄호 ()는 단어 바로 옆에 위치 (문장 마지막이 아님)
5. 정치용어, 한자 등만 설명:
   - '여야' → '여당과 야당(여야)'
   - '특검법' → '특별 수사 제도(특검법)'
   - '필리버스터' → '의도적으로 회의 시간 끄는 방식(필리버스터)'
   - '체포동의안' → '구속 허가 신청(체포동의안)'
   - '인사청문회' → '후보자 심사 회의(인사청문회)'
   - '과반수' → '절반 이상(과반수)'
   - '일방처리' → '한쪽이 강행(일방처리)'
   - '합의안' → '협의 결과(합의안)'
   - '재협상' → '다시 협의(재협상)'
   - '결렬' → '협의 깨짐(결렬)'
6. {bias} 성향의 논조와 스탠스가 명확히 드러나도록 작성
7. 단순히 '지지한다/반대한다' 식 표현 말고, {bias} 성향의 가치관에 기반한 구체적 이유 포함
8. 해시태그는 사용하지 마세요
9. 첫 문장은 이슈에 대한 {bias} 성향의 명확한 태도를 제시하되, 주어를 다양하게 사용하세요:
   - 좌파: "정부의 OO 정책은 문제가 있다", "OO의 주장은 타당하다", "이번 결정은 올바르다", "해당 사업은 부적절하다" 등
   - 중립: "이 문제는 신중한 접근이 필요하다", "양측의 입장을 모두 고려해야 한다", "사안의 복잡성을 고려해야 한다", "이번 사건은 주의깊게 봐야 한다" 등  
   - 우파: "정부의 OO 정책은 올바르다", "OO의 주장은 근거가 부족하다", "이번 조치는 적절하다", "해당 정책은 합리적이다" 등
10. 이슈의 핵심 주체(정부, 정치인, 정책 등)에 대한 {bias} 성향의 명확한 태도를 보여주세요
11. 주어를 다양하게 사용하여 가독성을 높이세요 (예: "정부는", "이번 결정은", "해당 정책은", "이 문제는" 등)
12. 반드시 {char_limit}자 이내로 작성하고, 초과 시 다시 작성하세요

{bias} 관점:"""
        
        return prompt
    
    def generate_view(self, articles_data: List[Dict], issue_info: Dict, bias: str) -> Optional[str]:
        """
        관점 생성
        
        Args:
            articles_data: 기사 데이터 목록
            issue_info: 이슈 정보
            bias: 성향
            
        Returns:
            str: 생성된 관점
        """
        try:
            prompt = self.create_prompt(articles_data, issue_info, bias)
            
            response = self.openai_client.chat.completions.create(
                model=self.MODEL_NAME,
                messages=[
                    {"role": "system", "content": f"당신은 {bias} 성향의 정치 해설자입니다. 정치에 관심 없는 사람도 이해할 수 있도록 쉬운 말로 풀어서 설명합니다."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.MAX_TOKENS,
                temperature=self.TEMPERATURE
            )
            
            # 응답 파싱
            content = response.choices[0].message.content.strip()
            
            # 관점 추출 - 프롬프트 끝부분 제거하고 실제 내용만 추출
            view_match = re.search(rf'{bias} 관점:\s*(.+)', content)
            if not view_match:
                # 다른 패턴으로 시도
                view_match = re.search(rf'{bias} 성향의 관점:\s*(.+)', content)
            if not view_match:
                # 전체 응답을 그대로 사용 (프롬프트가 포함되지 않은 경우)
                view_match = re.search(r'^(.+)$', content.strip())
            
            if view_match:
                view = view_match.group(1).strip()
                print(f"✅ {bias} 관점 생성 완료")
                return view
            else:
                print(f"❌ {bias} 관점 추출 실패")
                print(f"응답 내용: {content[:200]}...")
                return None
                    
        except Exception as e:
            print(f"❌ {bias} 관점 생성 실패: {str(e)}")
            return None
    
    def generate_views_parallel(self, issue_id: str) -> Dict[str, str]:
        """
        성향별 관점 병렬 생성
        
        Args:
            issue_id: 이슈 ID
            
        Returns:
            Dict[str, str]: 성향별 관점 딕셔너리
        """
        try:
            print(f"\n🔍 이슈 {issue_id} 관점 생성 시작...")
            
            # 이슈 정보 조회
            issue_info = self.fetch_issue_info(issue_id)
            if not issue_info:
                return {}
            
            # 성향별 기사들 조회
            left_articles = self.fetch_articles_by_bias(issue_id, 'left')
            center_articles = self.fetch_articles_by_bias(issue_id, 'center')
            right_articles = self.fetch_articles_by_bias(issue_id, 'right')
            
            # 병렬 처리로 관점 생성
            views = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                futures = {}
                
                # 각 성향별로 관점 생성 작업 제출
                if left_articles:
                    futures['left'] = executor.submit(
                        self.generate_view, left_articles, issue_info, 'left'
                    )
                
                if center_articles:
                    futures['center'] = executor.submit(
                        self.generate_view, center_articles, issue_info, 'center'
                    )
                
                if right_articles:
                    futures['right'] = executor.submit(
                        self.generate_view, right_articles, issue_info, 'right'
                    )
                
                # 결과 수집
                for bias, future in futures.items():
                    try:
                        view = future.result(timeout=60)  # 60초 타임아웃
                        if view:
                            views[bias] = view
                    except Exception as e:
                        print(f"❌ {bias} 관점 생성 실패: {str(e)}")
            
            return views
            
        except Exception as e:
            print(f"❌ 이슈 {issue_id} 관점 생성 실패: {str(e)}")
            return {}
    
    def update_issues_table(self, issue_id: str, views: Dict[str, str]) -> bool:
        """
        issues 테이블 업데이트
        
        Args:
            issue_id: 이슈 ID
            views: 성향별 관점 딕셔너리
            
        Returns:
            bool: 업데이트 성공 여부
        """
        try:
            update_data = {}
            
            if 'left' in views:
                update_data['left_view'] = views['left']
            if 'center' in views:
                update_data['center_view'] = views['center']
            if 'right' in views:
                update_data['right_view'] = views['right']
            
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
        """
        단일 이슈 처리
        
        Args:
            issue_id: 이슈 ID
            
        Returns:
            bool: 처리 성공 여부
        """
        try:
            print(f"\n📋 이슈 {issue_id} 처리 시작...")
            
            # 관점 생성
            views = self.generate_views_parallel(issue_id)
            
            if not views:
                print(f"❌ 이슈 {issue_id} 관점 생성 실패")
                return False
            
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
    
    def process_all_issues(self) -> bool:
        """
        모든 이슈 처리
        
        Returns:
            bool: 처리 성공 여부
        """
        try:
            print("🚀 모든 이슈의 성향별 관점 생성 시작...")
            
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
                
                # 기존 view가 있어도 덮어쓰기 (주석 처리)
                # issue_detail = self.supabase_manager.client.table('issues').select(
                #     'left_view, center_view, right_view'
                # ).eq('id', issue_id).execute()
                # 
                # if issue_detail.data:
                #     views = issue_detail.data[0]
                #     if views.get('left_view') and views.get('center_view') and views.get('right_view'):
                #         print(f"⏭️ 이슈 {issue_id}는 이미 view가 생성됨")
                #         continue
                
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
    print("🧪 단일 이슈 테스트 모드")
    print("=" * 60)
    
    try:
        # 생성기 초기화
        generator = ViewGenerator()
        
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
            
            # 결과 확인
            result = generator.supabase_manager.client.table('issues').select(
                'left_view, center_view, right_view'
            ).eq('id', issue_id).execute()
            
            if result.data:
                views = result.data[0]
                print("\n📊 생성된 관점들:")
                print(f"좌파 관점: {views.get('left_view', 'N/A')}")
                print(f"중립 관점: {views.get('center_view', 'N/A')}")
                print(f"우파 관점: {views.get('right_view', 'N/A')}")
        else:
            print("\n❌ 단일 이슈 테스트 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")

def main():
    """메인 함수"""
    print("=" * 60)
    print("🎭 모듈 2-4: 성향별 관점 생성 스크립트")
    print("=" * 60)
    
    try:
        # 생성기 초기화
        generator = ViewGenerator()
        
        # 모든 이슈 처리
        success = generator.process_all_issues()
        
        if success:
            print("\n✅ 성향별 관점 생성 완료!")
        else:
            print("\n❌ 성향별 관점 생성 실패!")
            
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