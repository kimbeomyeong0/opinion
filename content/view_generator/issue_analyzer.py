#!/usr/bin/env python3
"""
LLM 기반 이슈 특성 분석 모듈
하드코딩된 키워드 매칭 대신 LLM을 활용한 동적 분석
"""

import re
from typing import Dict, List, Any, Optional

class LLMBasedIssueAnalyzer:
    """LLM 기반 이슈 특성 분석 클래스"""
    
    def __init__(self, llm_client):
        """초기화"""
        self.llm_client = llm_client
        self.MODEL_NAME = "gpt-4o-mini"
        self.TEMPERATURE = 0.3
    
    def analyze_issue_characteristics(self, issue_data: Dict[str, Any], articles_data: List[Dict]) -> Dict[str, Any]:
        """
        LLM으로 이슈 특성 동적 분석
        
        Args:
            issue_data: 이슈 데이터
            articles_data: 기사 데이터 리스트
            
        Returns:
            Dict: 이슈 특성 분석 결과
        """
        try:
            # 기사 요약 생성
            articles_summary = self._summarize_articles(articles_data)
            
            # LLM 분석 프롬프트
            prompt = f"""다음 기사들을 분석하여 이슈의 특성을 파악해주세요.

[이슈 정보]
제목: {issue_data.get('title', '')}
부제목: {issue_data.get('subtitle', '')}

[기사 내용]
{articles_summary}

다음 항목들을 분석해주세요:

1. 이슈 유형: 경제, 정치, 사회, 환경, 안보, 기술, 법률, 외교 중에서 가장 적절한 유형
2. 핵심 이해관계자: 정부, 기업, 시민, 전문가, 국제기구 등 (최대 3개)
3. 주요 갈등 구조: 자유vs평등, 효율vs공정, 개인vs집단, 시장vs정부 등 (최대 2개)
4. 이슈의 복잡도: 단순, 중간, 복합
5. 시간적 맥락: 긴급성, 장기성, 과도기

다음 형식으로만 응답해주세요:
이슈 유형: [유형]
핵심 이해관계자: [이해관계자1, 이해관계자2, 이해관계자3]
주요 갈등: [갈등1, 갈등2]
복잡도: [단순/중간/복합]
시간적 맥락: [긴급성/장기성/과도기]"""
            
            response = self._call_llm(prompt)
            return self._parse_issue_analysis(response)
            
        except Exception as e:
            print(f"❌ 이슈 특성 분석 실패: {str(e)}")
            return self._get_default_characteristics()
    
    def _summarize_articles(self, articles_data: List[Dict]) -> str:
        """기사들을 요약하여 분석용 텍스트 생성"""
        
        summaries = []
        for i, article in enumerate(articles_data[:5], 1):  # 최대 5개 기사
            # 기사 앞 5문장 추출
            content = article.get('merged_content', '')
            first_sentences = self._extract_first_5_sentences(content)
            
            summaries.append(f"{i}. ({article.get('media_name', '')} - {article.get('bias', '')}): {first_sentences}")
        
        return "\n".join(summaries)
    
    def _extract_first_5_sentences(self, content: str) -> str:
        """기사 본문에서 앞 5문장만 추출"""
        
        # 문장 분리 (마침표, 느낌표, 물음표 기준)
        sentences = re.split(r'(?<!\d)[.!?]+(?!\d)', content)
        
        # 문장 정리
        clean_sentences = []
        for sentence in sentences:
            sentence = sentence.strip()
            # 의미있는 문장만 선택 (10자 이상, 한글 포함)
            if len(sentence) > 10 and re.search(r'[가-힣]', sentence):
                clean_sentences.append(sentence)
        
        # 앞 5문장 선택
        first_5 = clean_sentences[:5]
        
        # 길이 제한 (총 500자)
        result_sentences = []
        total_length = 0
        
        for sentence in first_5:
            if total_length + len(sentence) > 500:
                break
            result_sentences.append(sentence)
            total_length += len(sentence)
        
        # 결합
        result = '. '.join(result_sentences)
        if result and not result.endswith('.'):
            result += '.'
        
        return result
    
    def _call_llm(self, prompt: str) -> str:
        """LLM 호출"""
        
        response = self.llm_client.chat.completions.create(
            model=self.MODEL_NAME,
            messages=[
                {"role": "system", "content": "당신은 정치 이슈 분석 전문가입니다. 정확하고 객관적인 분석을 제공합니다."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=500,
            temperature=self.TEMPERATURE
        )
        
        return response.choices[0].message.content.strip()
    
    def _parse_issue_analysis(self, response: str) -> Dict[str, Any]:
        """LLM 응답을 파싱하여 구조화된 데이터로 변환"""
        
        try:
            # 각 항목 추출
            issue_type = self._extract_value(response, "이슈 유형:")
            stakeholders = self._extract_value(response, "핵심 이해관계자:").split(', ')
            conflicts = self._extract_value(response, "주요 갈등:").split(', ')
            complexity = self._extract_value(response, "복잡도:")
            temporal_context = self._extract_value(response, "시간적 맥락:")
            
            return {
                "issue_type": issue_type.strip(),
                "stakeholders": [s.strip() for s in stakeholders if s.strip()],
                "core_conflicts": [c.strip() for c in conflicts if c.strip()],
                "complexity": complexity.strip(),
                "temporal_context": temporal_context.strip(),
                "analysis_confidence": 0.8  # LLM 분석 신뢰도
            }
            
        except Exception as e:
            print(f"❌ 응답 파싱 실패: {str(e)}")
            return self._get_default_characteristics()
    
    def _extract_value(self, text: str, key: str) -> str:
        """텍스트에서 특정 키의 값 추출"""
        
        lines = text.split('\n')
        for line in lines:
            if line.strip().startswith(key):
                return line.replace(key, '').strip()
        
        return ""
    
    def _get_default_characteristics(self) -> Dict[str, Any]:
        """기본 이슈 특성 반환"""
        return {
            "issue_type": "정치",
            "stakeholders": ["정부", "시민"],
            "core_conflicts": ["자유 vs 평등"],
            "complexity": "중간",
            "temporal_context": "과도기",
            "analysis_confidence": 0.5
        }
