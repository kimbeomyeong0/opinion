#!/usr/bin/env python3
"""
Title, Subtitle 생성 스크립트 (개선된 버전)
- 이슈별 기사들의 앞 5문장을 분석해서 title, subtitle 생성
- 언론인 관점의 객관적이고 정확한 제목/부제목 생성
- GPT-4o-mini 기반 LLM 처리
- 객관성 검증 및 품질 보장
- issues 테이블 업데이트

개선사항:
- 기사 본문 앞 5문장만 추출하여 효율성 향상
- 감정적 표현 제거 및 객관성 확보
- 타이틀/서브타이틀 길이 최적화 (15-20자, 30-50자)
- 편향성 키워드 검증
- 테스트 모드 추가
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
        self.MAX_TOKENS = 1000  # 타이틀/서브타이틀 생성에는 충분
        self.TEMPERATURE = 0.3  # 일관성을 위해 낮은 값 사용
        
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
    
    def _extract_first_5_sentences(self, content: str) -> str:
        """
        기사 본문에서 앞 5문장만 추출
        
        Args:
            content: 기사 본문 내용
            
        Returns:
            str: 앞 5문장으로 구성된 텍스트
        """
        import re
        
        # 1. 문장 분리 (마침표, 느낌표, 물음표 기준)
        sentences = re.split(r'(?<!\d)[.!?]+(?!\d)', content)
        
        # 2. 문장 정리
        clean_sentences = []
        for sentence in sentences:
            sentence = sentence.strip()
            # 의미있는 문장만 선택 (10자 이상, 한글 포함)
            if len(sentence) > 10 and re.search(r'[가-힣]', sentence):
                clean_sentences.append(sentence)
        
        # 3. 앞 5문장 선택
        first_5 = clean_sentences[:5]
        
        # 4. 길이 제한 (너무 길면 자르기)
        result_sentences = []
        total_length = 0
        
        for sentence in first_5:
            if total_length + len(sentence) > 500:  # 총 500자 제한
                break
            result_sentences.append(sentence)
            total_length += len(sentence)
        
        # 5. 결합
        result = '. '.join(result_sentences)
        if result and not result.endswith('.'):
            result += '.'
        
        return result
    
    def _remove_emotional_expressions(self, text: str) -> str:
        """
        감정적 표현을 중립적 표현으로 변경
        
        Args:
            text: 원본 텍스트
            
        Returns:
            str: 중립적 표현으로 변경된 텍스트
        """
        emotional_words = {
            "충격": "중요",
            "폭발적": "주목할 만한",
            "격렬": "심각한",
            "심각": "중요한",
            "위험": "우려되는",
            "위기": "문제",
            "대폭": "크게",
            "급격": "빠른",
            "급증": "증가",
            "급감": "감소",
            "급상승": "상승",
            "급하락": "하락"
        }
        
        result = text
        for emotional, neutral in emotional_words.items():
            result = result.replace(emotional, neutral)
        
        return result
    
    def create_prompt(self, articles_data: List[Dict]) -> str:
        """
        LLM 프롬프트 생성 (개선된 버전)
        
        Args:
            articles_data: 기사 데이터 리스트
            
        Returns:
            str: 프롬프트 문자열
        """
        # 기사 목록 구성 (앞 5문장 기반)
        articles_text = ""
        for i, article in enumerate(articles_data, 1):
            # 기사 본문에서 앞 5문장만 추출
            first_5_sentences = self._extract_first_5_sentences(article['merged_content'])
            
            articles_text += f"{i}. ({article['media_name']} - {article['bias']})\n"
            articles_text += f"   내용: {first_5_sentences}\n\n"
        
        prompt = f"""당신은 경험이 풍부한 언론인입니다. 다음 {len(articles_data)}개 기사의 앞부분을 분석하여 정확하고 객관적인 제목과 부제목을 생성해주세요.

[기사 내용]
{articles_text}

생성 기준:
1. 제목: 15-20자 내외, 핵심 사건을 간결하게 표현
2. 부제목: 30-50자 내외, 배경과 맥락을 균형있게 설명
3. 객관성: 감정적 표현 배제, 사실 중심
4. 명확성: 모호하지 않은 구체적 표현
5. 시의성: 최신 정보와 맥락 반영

반드시 다음 형식으로만 응답해주세요:

제목: [생성된 제목]
부제목: [생성된 부제목]"""
        
        return prompt
    
    def _validate_objectivity(self, title: str, subtitle: str) -> Dict[str, str]:
        """
        생성된 제목/부제목의 객관성 검증 및 개선
        
        Args:
            title: 생성된 제목
            subtitle: 생성된 부제목
            
        Returns:
            Dict[str, str]: 검증 및 개선된 제목/부제목
        """
        # 1. 감정적 표현 제거
        neutral_title = self._remove_emotional_expressions(title)
        neutral_subtitle = self._remove_emotional_expressions(subtitle)
        
        # 2. 길이 검증 및 조정
        if len(neutral_title) > 20:
            print(f"⚠️ 제목이 너무 깁니다 ({len(neutral_title)}자): {neutral_title}")
        
        if len(neutral_subtitle) > 50:
            print(f"⚠️ 부제목이 너무 깁니다 ({len(neutral_subtitle)}자): {neutral_subtitle}")
        
        # 3. 편향성 키워드 검증
        bias_keywords = ["반드시", "당연히", "틀림없이", "확실히", "무조건"]
        for keyword in bias_keywords:
            if keyword in neutral_title or keyword in neutral_subtitle:
                print(f"⚠️ 편향성 키워드 발견: {keyword}")
        
        return {
            "title": neutral_title,
            "subtitle": neutral_subtitle
        }
    
    def generate_title_subtitle(self, articles_data: List[Dict]) -> Optional[Dict[str, str]]:
        """
        LLM으로 title, subtitle 생성 (개선된 버전)
        
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
                    {"role": "system", "content": "당신은 경험이 풍부한 언론인입니다. 정확하고 객관적인 제목과 부제목을 생성하는 데 특화되어 있습니다. 감정적 표현을 피하고 사실 중심의 명확한 표현을 사용합니다."},
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
                raw_title = title_match.group(1).strip()
                raw_subtitle = subtitle_match.group(1).strip()
                
                print("✅ title, subtitle 추출 완료")
                print(f"  - 원본 제목: {raw_title}")
                print(f"  - 원본 부제목: {raw_subtitle}")
                
                # 객관성 검증 및 개선
                validated = self._validate_objectivity(raw_title, raw_subtitle)
                
                print(f"  - 검증된 제목: {validated['title']}")
                print(f"  - 검증된 부제목: {validated['subtitle']}")
                
                return validated
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
    
    def process_single_issue(self) -> bool:
        """
        첫 번째 이슈만 처리 (테스트용)
        
        Returns:
            bool: 처리 성공 여부
        """
        try:
            print("🚀 첫 번째 이슈의 title, subtitle 생성 시작...")
            
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
    print("🧪 단일 이슈 Title, Subtitle 테스트 모드")
    print("=" * 60)
    
    try:
        # 생성기 초기화
        generator = TitleSubtitleGenerator()
        
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
    print("📝 모듈 1: Title, Subtitle 생성 스크립트 (개선된 버전)")
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
    import sys
    
    # 명령행 인수로 테스트 모드 확인
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_single_issue()
    else:
        main()
