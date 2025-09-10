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
        
        # 기사 내용들을 800자로 제한하여 결합
        articles_text = ""
        for i, article in enumerate(articles_data[:5], 1):  # 최대 5개 기사
            content = article['content'][:800]  # 800자 제한
            media_name = article['media_name']
            articles_text += f"\n[기사 {i}] ({media_name})\n{content}\n"
        
        prompt = f"""다음 이슈에 대한 {bias} 성향의 관점을 150자 이내로 작성해주세요.

이슈 제목: {title}
이슈 부제목: {subtitle}

관련 기사들:
{articles_text}

요구사항:
1. {bias} 성향의 입장에서 이슈를 분석
2. 기사 내용을 바탕으로 구체적이고 논리적인 관점 제시
3. 150자 이내로 간결하게 작성
4. 해시태그 스타일로 스탠스 3개 추가 (예: #지지 #긍정적 #협력강화)
5. "{bias} 관점: [해시태그들] [관점 내용]" 형식으로 작성

해시태그 카테고리:
- 지지/긍정: #지지 #긍정적 #협력강화 #필요성 #옹호 #지원
- 비판/우려: #비판 #우려 #반대 #경계 #신중 #문제제기
- 비난/강경: #비난 #강경 #단호 #철저 #강력 #비판적
- 중립/균형: #중립 #균형 #신중 #절충 #조화 #공정

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
                    {"role": "system", "content": f"당신은 {bias} 성향의 정치 분석가입니다. 객관적이면서도 명확한 입장을 제시합니다."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.MAX_TOKENS,
                temperature=self.TEMPERATURE
            )
            
            # 응답 파싱
            content = response.choices[0].message.content.strip()
            
            # 관점 추출
            view_match = re.search(rf'{bias} 관점:\s*(.+)', content)
            
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
                
                # view가 이미 있는 이슈는 건너뛰기
                issue_detail = self.supabase_manager.client.table('issues').select(
                    'left_view, center_view, right_view'
                ).eq('id', issue_id).execute()
                
                if issue_detail.data:
                    views = issue_detail.data[0]
                    if views.get('left_view') and views.get('center_view') and views.get('right_view'):
                        print(f"⏭️ 이슈 {issue_id}는 이미 view가 생성됨")
                        continue
                
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
    main()