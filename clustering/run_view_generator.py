#!/usr/bin/env python3
"""
모듈 2-4: 성향별 관점 생성 스크립트
- 이슈별로 좌파, 중립, 우파 관점을 생성
- articles.content 기반으로 LLM 처리
- issues 테이블의 left_view, center_view, right_view 업데이트
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
        
        openai.api_key = os.getenv('OPENAI_API_KEY')
    
    def fetch_issue_info(self, issue_id: str) -> Optional[Dict]:
        """
        이슈 정보 조회
        
        Args:
            issue_id: 이슈 ID
            
        Returns:
            Dict: 이슈 정보 또는 None
        """
        try:
            result = self.supabase_manager.client.table('issues').select(
                'id, title, subtitle'
            ).eq('id', issue_id).execute()
            
            if result.data:
                return result.data[0]
            else:
                print(f"❌ 이슈 {issue_id} 정보를 찾을 수 없습니다.")
                return None
                
        except Exception as e:
            print(f"❌ 이슈 정보 조회 실패: {str(e)}")
            return None
    
    def fetch_articles_by_bias(self, issue_id: str, bias: str) -> List[Dict]:
        """
        성향별 기사들 조회
        
        Args:
            issue_id: 이슈 ID
            bias: 성향 ('left', 'center', 'right')
            
        Returns:
            List[Dict]: 기사 데이터 리스트
        """
        try:
            print(f"📡 {bias} 성향 기사들 조회 중...")
            
            # issue_articles → articles 조인하여 데이터 조회
            result = self.supabase_manager.client.table('issue_articles').select(
                'article_id, cleaned_article_id, '
                'articles!inner(id, title, content, media_id, media_outlets!inner(name, bias))'
            ).eq('issue_id', issue_id).eq('articles.media_outlets.bias', bias).execute()
            
            if not result.data:
                print(f"⚠️ {bias} 성향 기사가 없습니다.")
                return []
            
            # 데이터 정리
            articles_data = []
            for item in result.data:
                article = item['articles']
                media = article['media_outlets']
                
                # 기사 내용 800자로 제한
                content = article['content']
                if len(content) > 800:
                    content = content[:800] + "..."
                
                articles_data.append({
                    'title': article['title'],
                    'content': content,
                    'media_name': media['name'],
                    'bias': media['bias']
                })
            
            print(f"✅ {len(articles_data)}개 {bias} 성향 기사 조회 완료")
            return articles_data
            
        except Exception as e:
            print(f"❌ {bias} 성향 기사 조회 실패: {str(e)}")
            return []
    
    def create_prompt(self, articles_data: List[Dict], issue_info: Dict, bias: str) -> str:
        """
        LLM 프롬프트 생성
        
        Args:
            articles_data: 기사 데이터 리스트
            issue_info: 이슈 정보
            bias: 성향
            
        Returns:
            str: 프롬프트 문자열
        """
        # 기사 목록 구성
        articles_text = ""
        for i, article in enumerate(articles_data, 1):
            articles_text += f"{i}. ({article['media_name']})\n"
            articles_text += f"   내용: \"{article['content']}\"\n\n"
        
        # 성향별 지시사항
        bias_instructions = {
            'left': "비판적 관점으로 문제점을 지적하고 대안을 제시해주세요.",
            'center': "균형잡힌 시각으로 신중한 입장을 표현해주세요.",
            'right': "지지적 관점으로 필요성을 강조하고 추진을 지지해주세요."
        }
        
        prompt = f"""다음 이슈에 대한 {bias} 관점을 생성해주세요:

[이슈 정보]
- title: "{issue_info['title']}"
- subtitle: "{issue_info['subtitle']}"

[{bias} 기사들]
{articles_text}

{bias_instructions[bias]}

다음 형식으로 생성해주세요:
{bias} 관점: [150자, 기승전결 구조, {bias} 입장 명확하게]

구조:
- 기 (起): 상황 제시 (30자)
- 승 (承): 입장 명확화 (40자) 
- 전 (轉): 구체적 근거/비판 (50자)
- 결 (결): 결론/대안 (30자)

예시:
{bias} 관점: "정치개혁이 또 다른 정치 쇼가 되고 있다." 여당이 시민 의견을 무시한 채 성급하게 추진하면서 야당과 시민사회의 반발이 거세지고 있다. 민주적 절차를 무시한 성급한 추진은 진정한 개혁이 아니라 또 다른 정치적 갈등만을 만들고 있다."""
        
        return prompt
    
    def generate_view(self, articles_data: List[Dict], issue_info: Dict, bias: str) -> Optional[str]:
        """
        LLM으로 성향별 관점 생성
        
        Args:
            articles_data: 기사 데이터 리스트
            issue_info: 이슈 정보
            bias: 성향
            
        Returns:
            str: 생성된 관점 또는 None
        """
        try:
            print(f"🤖 {bias} 관점 생성 중...")
            
            prompt = self.create_prompt(articles_data, issue_info, bias)
            
            client = openai.OpenAI()
            response = client.chat.completions.create(
                model=self.MODEL_NAME,
                messages=[
                    {"role": "system", "content": f"당신은 {bias} 성향의 뉴스 분석 전문가입니다. {bias} 입장에서 명확하고 설득력 있는 관점을 제시하는 데 특화되어 있습니다."},
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
                
                if left_articles:
                    futures['left'] = executor.submit(self.generate_view, left_articles, issue_info, 'left')
                if center_articles:
                    futures['center'] = executor.submit(self.generate_view, center_articles, issue_info, 'center')
                if right_articles:
                    futures['right'] = executor.submit(self.generate_view, right_articles, issue_info, 'right')
                
                # 결과 수집
                for bias, future in futures.items():
                    try:
                        result = future.result(timeout=60)  # 60초 타임아웃
                        if result:
                            views[bias] = result
                    except Exception as e:
                        print(f"❌ {bias} 관점 생성 실패: {str(e)}")
            
            return views
            
        except Exception as e:
            print(f"❌ 관점 생성 실패: {str(e)}")
            return {}
    
    def update_views(self, issue_id: str, views: Dict[str, str]) -> bool:
        """
        issues 테이블의 view 컬럼들 업데이트
        
        Args:
            issue_id: 이슈 ID
            views: 성향별 관점 딕셔너리
            
        Returns:
            bool: 업데이트 성공 여부
        """
        try:
            print(f"💾 이슈 {issue_id} 관점 업데이트 중...")
            
            # 업데이트할 데이터 준비
            update_data = {}
            if 'left' in views:
                update_data['left_view'] = views['left']
            if 'center' in views:
                update_data['center_view'] = views['center']
            if 'right' in views:
                update_data['right_view'] = views['right']
            
            if not update_data:
                print("⚠️ 업데이트할 관점이 없습니다.")
                return False
            
            # DB 업데이트
            result = self.supabase_manager.client.table('issues').update(update_data).eq('id', issue_id).execute()
            
            if result.data:
                print(f"✅ 이슈 {issue_id} 관점 업데이트 완료")
                for bias, view in views.items():
                    print(f"  - {bias}_view: {view[:50]}...")
                return True
            else:
                print(f"❌ 이슈 {issue_id} 관점 업데이트 실패")
                return False
                
        except Exception as e:
            print(f"❌ 관점 업데이트 실패: {str(e)}")
            return False
    
    def process_issue(self, issue_id: str) -> bool:
        """
        이슈 처리 메인 프로세스
        
        Args:
            issue_id: 이슈 ID
            
        Returns:
            bool: 처리 성공 여부
        """
        try:
            # 관점 생성
            views = self.generate_views_parallel(issue_id)
            
            if not views:
                print(f"❌ 이슈 {issue_id} 관점 생성 실패")
                return False
            
            # DB 업데이트
            success = self.update_views(issue_id, views)
            
            return success
            
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
                
                # 이미 처리된 이슈는 건너뛰기 (임시 제목이 아닌 경우)
                if current_title and not current_title.startswith('이슈 '):
                    print(f"⏭️ 이슈 {issue_id}는 이미 처리됨 (제목: {current_title})")
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

모듈 2-4: 성향별 관점 생성 스크립트
- 이슈별로 좌파, 중립, 우파 관점을 생성
- articles.content 기반으로 LLM 처리
- issues 테이블의 left_view, center_view, right_view 업데이트
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
        
        openai.api_key = os.getenv('OPENAI_API_KEY')
    
    def fetch_issue_info(self, issue_id: str) -> Optional[Dict]:
        """
        이슈 정보 조회
        
        Args:
            issue_id: 이슈 ID
            
        Returns:
            Dict: 이슈 정보 또는 None
        """
        try:
            result = self.supabase_manager.client.table('issues').select(
                'id, title, subtitle'
            ).eq('id', issue_id).execute()
            
            if result.data:
                return result.data[0]
            else:
                print(f"❌ 이슈 {issue_id} 정보를 찾을 수 없습니다.")
                return None
                
        except Exception as e:
            print(f"❌ 이슈 정보 조회 실패: {str(e)}")
            return None
    
    def fetch_articles_by_bias(self, issue_id: str, bias: str) -> List[Dict]:
        """
        성향별 기사들 조회
        
        Args:
            issue_id: 이슈 ID
            bias: 성향 ('left', 'center', 'right')
            
        Returns:
            List[Dict]: 기사 데이터 리스트
        """
        try:
            print(f"📡 {bias} 성향 기사들 조회 중...")
            
            # issue_articles → articles 조인하여 데이터 조회
            result = self.supabase_manager.client.table('issue_articles').select(
                'article_id, cleaned_article_id, '
                'articles!inner(id, title, content, media_id, media_outlets!inner(name, bias))'
            ).eq('issue_id', issue_id).eq('articles.media_outlets.bias', bias).execute()
            
            if not result.data:
                print(f"⚠️ {bias} 성향 기사가 없습니다.")
                return []
            
            # 데이터 정리
            articles_data = []
            for item in result.data:
                article = item['articles']
                media = article['media_outlets']
                
                # 기사 내용 800자로 제한
                content = article['content']
                if len(content) > 800:
                    content = content[:800] + "..."
                
                articles_data.append({
                    'title': article['title'],
                    'content': content,
                    'media_name': media['name'],
                    'bias': media['bias']
                })
            
            print(f"✅ {len(articles_data)}개 {bias} 성향 기사 조회 완료")
            return articles_data
            
        except Exception as e:
            print(f"❌ {bias} 성향 기사 조회 실패: {str(e)}")
            return []
    
    def create_prompt(self, articles_data: List[Dict], issue_info: Dict, bias: str) -> str:
        """
        LLM 프롬프트 생성
        
        Args:
            articles_data: 기사 데이터 리스트
            issue_info: 이슈 정보
            bias: 성향
            
        Returns:
            str: 프롬프트 문자열
        """
        # 기사 목록 구성
        articles_text = ""
        for i, article in enumerate(articles_data, 1):
            articles_text += f"{i}. ({article['media_name']})\n"
            articles_text += f"   내용: \"{article['content']}\"\n\n"
        
        # 성향별 지시사항
        bias_instructions = {
            'left': "비판적 관점으로 문제점을 지적하고 대안을 제시해주세요.",
            'center': "균형잡힌 시각으로 신중한 입장을 표현해주세요.",
            'right': "지지적 관점으로 필요성을 강조하고 추진을 지지해주세요."
        }
        
        prompt = f"""다음 이슈에 대한 {bias} 관점을 생성해주세요:

[이슈 정보]
- title: "{issue_info['title']}"
- subtitle: "{issue_info['subtitle']}"

[{bias} 기사들]
{articles_text}

{bias_instructions[bias]}

다음 형식으로 생성해주세요:
{bias} 관점: [150자, 기승전결 구조, {bias} 입장 명확하게]

구조:
- 기 (起): 상황 제시 (30자)
- 승 (承): 입장 명확화 (40자) 
- 전 (轉): 구체적 근거/비판 (50자)
- 결 (결): 결론/대안 (30자)

예시:
{bias} 관점: "정치개혁이 또 다른 정치 쇼가 되고 있다." 여당이 시민 의견을 무시한 채 성급하게 추진하면서 야당과 시민사회의 반발이 거세지고 있다. 민주적 절차를 무시한 성급한 추진은 진정한 개혁이 아니라 또 다른 정치적 갈등만을 만들고 있다."""
        
        return prompt
    
    def generate_view(self, articles_data: List[Dict], issue_info: Dict, bias: str) -> Optional[str]:
        """
        LLM으로 성향별 관점 생성
        
        Args:
            articles_data: 기사 데이터 리스트
            issue_info: 이슈 정보
            bias: 성향
            
        Returns:
            str: 생성된 관점 또는 None
        """
        try:
            print(f"🤖 {bias} 관점 생성 중...")
            
            prompt = self.create_prompt(articles_data, issue_info, bias)
            
            client = openai.OpenAI()
            response = client.chat.completions.create(
                model=self.MODEL_NAME,
                messages=[
                    {"role": "system", "content": f"당신은 {bias} 성향의 뉴스 분석 전문가입니다. {bias} 입장에서 명확하고 설득력 있는 관점을 제시하는 데 특화되어 있습니다."},
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
                
                if left_articles:
                    futures['left'] = executor.submit(self.generate_view, left_articles, issue_info, 'left')
                if center_articles:
                    futures['center'] = executor.submit(self.generate_view, center_articles, issue_info, 'center')
                if right_articles:
                    futures['right'] = executor.submit(self.generate_view, right_articles, issue_info, 'right')
                
                # 결과 수집
                for bias, future in futures.items():
                    try:
                        result = future.result(timeout=60)  # 60초 타임아웃
                        if result:
                            views[bias] = result
                    except Exception as e:
                        print(f"❌ {bias} 관점 생성 실패: {str(e)}")
            
            return views
            
        except Exception as e:
            print(f"❌ 관점 생성 실패: {str(e)}")
            return {}
    
    def update_views(self, issue_id: str, views: Dict[str, str]) -> bool:
        """
        issues 테이블의 view 컬럼들 업데이트
        
        Args:
            issue_id: 이슈 ID
            views: 성향별 관점 딕셔너리
            
        Returns:
            bool: 업데이트 성공 여부
        """
        try:
            print(f"💾 이슈 {issue_id} 관점 업데이트 중...")
            
            # 업데이트할 데이터 준비
            update_data = {}
            if 'left' in views:
                update_data['left_view'] = views['left']
            if 'center' in views:
                update_data['center_view'] = views['center']
            if 'right' in views:
                update_data['right_view'] = views['right']
            
            if not update_data:
                print("⚠️ 업데이트할 관점이 없습니다.")
                return False
            
            # DB 업데이트
            result = self.supabase_manager.client.table('issues').update(update_data).eq('id', issue_id).execute()
            
            if result.data:
                print(f"✅ 이슈 {issue_id} 관점 업데이트 완료")
                for bias, view in views.items():
                    print(f"  - {bias}_view: {view[:50]}...")
                return True
            else:
                print(f"❌ 이슈 {issue_id} 관점 업데이트 실패")
                return False
                
        except Exception as e:
            print(f"❌ 관점 업데이트 실패: {str(e)}")
            return False
    
    def process_issue(self, issue_id: str) -> bool:
        """
        이슈 처리 메인 프로세스
        
        Args:
            issue_id: 이슈 ID
            
        Returns:
            bool: 처리 성공 여부
        """
        try:
            # 관점 생성
            views = self.generate_views_parallel(issue_id)
            
            if not views:
                print(f"❌ 이슈 {issue_id} 관점 생성 실패")
                return False
            
            # DB 업데이트
            success = self.update_views(issue_id, views)
            
            return success
            
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
                
                # 이미 처리된 이슈는 건너뛰기 (임시 제목이 아닌 경우)
                if current_title and not current_title.startswith('이슈 '):
                    print(f"⏭️ 이슈 {issue_id}는 이미 처리됨 (제목: {current_title})")
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
