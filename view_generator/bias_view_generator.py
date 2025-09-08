#!/usr/bin/env python3
"""
성향별 관점 생성기
"""

import asyncio
import os
import openai
from typing import Dict, List, Any, Optional
from datetime import datetime
from rich.console import Console
from rich.panel import Panel

from .config import get_config
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_manager import get_supabase_client

console = Console()

class BiasViewGenerator:
    """성향별 관점 생성 클래스"""
    
    def __init__(self):
        """초기화"""
        self.config = get_config()
        self.supabase = get_supabase_client()
        
        # OpenAI 클라이언트 설정
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            console.print("❌ OPENAI_API_KEY 환경변수가 설정되지 않았습니다.")
            raise ValueError("OpenAI API 키가 필요합니다.")
    
    def get_articles_by_bias(self, issue_id: str, bias: str) -> List[Dict[str, Any]]:
        """
        특정 이슈의 성향별 기사 조회
        
        Args:
            issue_id: 이슈 ID
            bias: 정치 성향 (left, center, right)
            
        Returns:
            기사 리스트
        """
        if not self.supabase.client:
            return []
        
        try:
            # issue_articles와 articles를 조인하여 성향별 기사 조회
            result = self.supabase.client.table('issue_articles')\
                .select('''
                    article_id,
                    articles!inner(
                        id,
                        title,
                        content,
                        media_outlets!inner(
                            name,
                            bias
                        )
                    )
                ''')\
                .eq('issue_id', issue_id)\
                .eq('articles.media_outlets.bias', bias)\
                .execute()
            
            articles = []
            for item in result.data:
                article_data = item['articles']
                if article_data and len(article_data.get('content', '')) > 50:  # 최소 길이만 체크
                    articles.append({
                        'id': article_data['id'],
                        'title': article_data['title'],
                        'content': article_data['content'],
                        'media_name': article_data['media_outlets']['name']
                    })
            
            console.print(f"📰 {bias} 성향 기사 {len(articles)}개 수집 완료")
            return articles
            
        except Exception as e:
            console.print(f"❌ {bias} 성향 기사 조회 실패: {str(e)}")
            return []
    
    def generate_bias_view(self, issue_data: Dict[str, Any], bias: str, articles: List[Dict[str, Any]]) -> str:
        """
        특정 성향의 관점 생성 (같은 성향 기사들의 본문 활용)
        
        Args:
            issue_data: 이슈 기본 정보
            bias: 정치 성향
            articles: 해당 성향의 기사 리스트
            
        Returns:
            생성된 관점 (불렛 포인트만)
        """
        if not articles:
            return "해당 성향의 기사가 충분하지 않아 관점을 생성할 수 없습니다."
        
        try:
            # 성향별 프롬프트 설정
            bias_config = self.config['bias_prompts'][bias]
            
            # 기사 내용 정리 (본문만 활용)
            articles_text = "\n\n".join([
                f"제목: {article['title']}\n내용: {article['content']}"
                for article in articles
            ])
            
            # 프롬프트 구성
            prompt = f"""
당신은 {bias_config['name']}의 관점에서 이슈를 분석하는 전문가입니다.

## 이슈 정보
- 제목: {issue_data.get('title', '')}
- 요약: {issue_data.get('summary', '')}
- 부제목: {issue_data.get('subtitle', '')}

## {bias_config['name']} 관점 분석 가이드라인
- {bias_config['description']}
- {bias_config['tone']}으로 작성
- 핵심 키워드: {', '.join(bias_config['keywords'])}

## 참고 기사들 (같은 성향)
{articles_text}

## 요구사항
위 기사들의 내용을 바탕으로 {bias_config['name']} 관점에서 이슈를 분석한 불렛 포인트를 1-3개 작성하세요.
- 각 불렛은 핵심 내용만 담고 간결하게 작성
- 특수기호나 장식적 표현 사용 금지
- 명확하고 직접적인 표현 사용
- 불렛 포인트만 출력하고 다른 설명은 하지 마세요

## 출력 형식
- [첫 번째 불렛 포인트]
- [두 번째 불렛 포인트]
- [세 번째 불렛 포인트] (선택사항)
"""
            
            # OpenAI API 호출 (최신 버전)
            client = openai.OpenAI(api_key=self.openai_api_key)
            response = client.chat.completions.create(
                model=self.config['llm_model'],
                messages=[
                    {"role": "system", "content": f"당신은 {bias_config['name']} 관점의 전문 분석가입니다."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.config['max_tokens'],
                temperature=self.config['temperature']
            )
            
            # 응답 파싱 (구조화된 관점 문서)
            content = response.choices[0].message.content.strip()
            
            # 하이픈 불렛 형식으로 정리
            lines = content.split('\n')
            formatted_lines = []
            
            for line in lines:
                line = line.strip()
                if line and not line.startswith('#'):  # 주석 제거
                    if line.startswith('-'):
                        formatted_lines.append(line)
                    else:
                        # 하이픈이 없는 줄은 하이픈 추가
                        formatted_lines.append(f"- {line}")
            
            # 순수 텍스트로 반환 (JSON 이스케이프 없음)
            result = '\n'.join(formatted_lines)
            return result
            
        except Exception as e:
            console.print(f"❌ {bias} 성향 관점 생성 실패: {str(e)}")
            return f"관점 생성 중 오류가 발생했습니다: {str(e)}"
    
    async def generate_all_bias_views(self, issue_id: str) -> Dict[str, str]:
        """
        모든 성향의 관점 생성 (같은 성향 기사들의 본문 활용)
        
        Args:
            issue_id: 이슈 ID
            
        Returns:
            성향별 관점 딕셔너리 (불렛 포인트만)
        """
        console.print(f"🎯 이슈 {issue_id}의 성향별 관점 생성 시작")
        
        # 이슈 기본 정보 조회
        try:
            issue_result = self.supabase.client.table('issues').select('*').eq('id', issue_id).execute()
            if not issue_result.data:
                console.print(f"❌ 이슈 {issue_id}를 찾을 수 없습니다.")
                return {}
            
            issue_data = issue_result.data[0]
        except Exception as e:
            console.print(f"❌ 이슈 정보 조회 실패: {str(e)}")
            return {}
        
        # 각 성향별 관점 생성
        bias_views = {}
        
        for bias in ['left', 'center', 'right']:
            console.print(f"📊 {bias} 성향 관점 생성 중...")
            
            # 해당 성향의 기사 조회
            articles = self.get_articles_by_bias(issue_id, bias)
            
            # 관점 생성 (같은 성향 기사들의 본문 활용)
            view = self.generate_bias_view(issue_data, bias, articles)
            bias_views[bias] = view
            
            console.print(f"✅ {bias} 성향 관점 생성 완료")
        
        return bias_views
    
    def update_issue_views(self, issue_id: str, bias_views: Dict[str, str]) -> bool:
        """
        이슈의 성향별 관점을 데이터베이스에 업데이트 (TEXT 타입)
        
        Args:
            issue_id: 이슈 ID
            bias_views: 성향별 관점 딕셔너리 (구조화된 텍스트)
            
        Returns:
            업데이트 성공 여부
        """
        if not self.supabase.client:
            return False
        
        try:
            # 업데이트할 데이터 구성 (TEXT 타입으로 저장)
            update_data = {}
            
            for bias in ['left', 'center', 'right']:
                if bias in bias_views:
                    # 구조화된 텍스트를 그대로 저장
                    update_data[f'{bias}_view'] = bias_views[bias]
            
            # 데이터베이스 업데이트
            result = self.supabase.client.table('issues')\
                .update(update_data)\
                .eq('id', issue_id)\
                .execute()
            
            if result.data:
                console.print(f"✅ 이슈 {issue_id} 성향별 관점 업데이트 완료")
                return True
            else:
                console.print(f"❌ 이슈 {issue_id} 성향별 관점 업데이트 실패")
                return False
                
        except Exception as e:
            console.print(f"❌ 성향별 관점 업데이트 오류: {str(e)}")
            return False

# 전역 인스턴스
_view_generator = None

def get_view_generator():
    """View Generator 인스턴스 반환 (싱글톤 패턴)"""
    global _view_generator
    if _view_generator is None:
        _view_generator = BiasViewGenerator()
    return _view_generator
