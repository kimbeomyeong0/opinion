#!/usr/bin/env python3
"""
모듈 1: Title, Subtitle 생성 스크립트
- 이슈별 기사들을 분석해서 title, subtitle 생성
- merged_content 기반으로 LLM 처리
- issues 테이블 업데이트
"""

import sys
import os
import json
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

class TitleSubtitleGenerator:
    """Title, Subtitle 생성 클래스"""
    
    def __init__(self):
        """초기화"""
        # OpenAI API 설정
        self.MODEL_NAME = "gpt-4o-mini"
        self.MAX_TOKENS = 2000
        self.TEMPERATURE = 0.7
        
        # Supabase 연결
        self.supabase_manager = SupabaseManager()
        if not self.supabase_manager.client:
            raise Exception("Supabase 연결 실패")
        
        # OpenAI API 키 설정
        if not os.getenv('OPENAI_API_KEY'):
            raise Exception("OPENAI_API_KEY 환경변수가 설정되지 않았습니다.")
        
        openai.api_key = os.getenv('OPENAI_API_KEY')
    
    def fetch_issue_articles(self, issue_id: str) -> Optional[List[Dict]]:
        """
        이슈의 기사들 조회 (merged_content 포함)
        
        Args:
            issue_id: 이슈 ID
            
        Returns:
            List[Dict]: 기사 데이터 리스트 또는 None
        """
        try:
            print(f"📡 이슈 {issue_id}의 기사 데이터 조회 중...")
            
            # issue_articles → articles → articles_cleaned 조인하여 데이터 조회
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
    
    def create_prompt(self, articles_data: List[Dict]) -> str:
        """
        LLM 프롬프트 생성
        
        Args:
            articles_data: 기사 데이터 리스트
            
        Returns:
            str: 프롬프트 문자열
        """
        # 기사 목록 구성
        articles_text = ""
        for i, article in enumerate(articles_data, 1):
            articles_text += f"{i}. ({article['media_name']} - {article['bias']})\n"
            articles_text += f"   내용: \"{article['merged_content'][:200]}...\"\n\n"
        
        prompt = f"""다음 {len(articles_data)}개 기사를 분석해서 매력적이고 디테일한 이슈 제목과 부제목을 생성해주세요.

[기사 목록]
{articles_text}

생성해주세요:
- title: [감정 + 핵심 키워드 + 상황, 20-25자, 디테일하게]
- subtitle: [구체적 상황 + 갈등점 + 현재 상태, 30-40자, 쉬운 표현]

* 참고: 이슈의 성격에 따라 의문형, 비유, 반전 표현 등 창의적 변형을 활용해주세요.
* 갈등뿐만 아니라 타협, 진전, 막힘 등 다양한 상황을 반영해주세요.

반드시 다음 형식으로만 응답해주세요:

제목: [생성된 제목]
부제목: [생성된 부제목]

예시:
제목: 정치개혁 논의 본격화, 여야 갈등 심화하며 대립 격화
부제목: 여당은 법안 통과를 촉구하고 있지만, 야당은 충분한 논의를 요구하며 대립이 심화되고 있다"""
        
        return prompt
    
    def generate_title_subtitle(self, articles_data: List[Dict]) -> Optional[Dict[str, str]]:
        """
        LLM으로 title, subtitle 생성
        
        Args:
            articles_data: 기사 데이터 리스트
            
        Returns:
            Dict[str, str]: title, subtitle 딕셔너리 또는 None
        """
        try:
            print("🤖 LLM으로 title, subtitle 생성 중...")
            
            prompt = self.create_prompt(articles_data)
            
            client = openai.OpenAI()
            response = client.chat.completions.create(
                model=self.MODEL_NAME,
                messages=[
                    {"role": "system", "content": "당신은 뉴스 이슈 분석 전문가입니다. 매력적이고 읽기 좋은 제목을 생성하는 데 특화되어 있습니다."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.MAX_TOKENS,
                temperature=self.TEMPERATURE
            )
            
            # 응답 파싱
            content = response.choices[0].message.content.strip()
            
            # 텍스트에서 title, subtitle 추출
            import re
            
            # 제목 추출
            title_match = re.search(r'제목:\s*(.+)', content)
            subtitle_match = re.search(r'부제목:\s*(.+)', content)
            
            if title_match and subtitle_match:
                title = title_match.group(1).strip()
                subtitle = subtitle_match.group(1).strip()
                print("✅ title, subtitle 추출 완료")
                return {"title": title, "subtitle": subtitle}
            else:
                print("❌ title, subtitle 추출 실패")
                print(f"응답 내용: {content[:200]}...")
                return None
                    
        except Exception as e:
            print(f"❌ LLM 생성 실패: {str(e)}")
            return None
    
    def update_issues_table(self, issue_id: str, title: str, subtitle: str) -> bool:
        """
        issues 테이블 업데이트
        
        Args:
            issue_id: 이슈 ID
            title: 생성된 제목
            subtitle: 생성된 부제목
            
        Returns:
            bool: 업데이트 성공 여부
        """
        try:
            print(f"💾 이슈 {issue_id} 업데이트 중...")
            
            result = self.supabase_manager.client.table('issues').update({
                'title': title,
                'subtitle': subtitle
            }).eq('id', issue_id).execute()
            
            if result.data:
                print(f"✅ 이슈 {issue_id} 업데이트 완료")
                print(f"  - title: {title}")
                print(f"  - subtitle: {subtitle}")
                return True
            else:
                print(f"❌ 이슈 {issue_id} 업데이트 실패")
                return False
                
        except Exception as e:
            print(f"❌ 업데이트 실패: {str(e)}")
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
            print(f"\n🔍 이슈 {issue_id} 처리 시작...")
            
            # 1. 기사 데이터 조회
            articles_data = self.fetch_issue_articles(issue_id)
            if not articles_data:
                return False
            
            # 2. title, subtitle 생성
            result = self.generate_title_subtitle(articles_data)
            if not result:
                return False
            
            # 3. issues 테이블 업데이트
            success = self.update_issues_table(
                issue_id, 
                result['title'], 
                result['subtitle']
            )
            
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
            print("🚀 모든 이슈의 title, subtitle 생성 시작...")
            
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
    print("📝 모듈 1: Title, Subtitle 생성 스크립트")
    print("=" * 60)
    
    try:
        # 생성기 초기화
        generator = TitleSubtitleGenerator()
        
        # 모든 이슈 처리
        success = generator.process_all_issues()
        
        if success:
            print("\n✅ Title, Subtitle 생성 완료!")
        else:
            print("\n❌ Title, Subtitle 생성 실패!")
            
    except KeyboardInterrupt:
        print("\n\n👋 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()
