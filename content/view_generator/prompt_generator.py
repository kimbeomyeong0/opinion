#!/usr/bin/env python3
"""
동적 프롬프트 생성 모듈
이슈 특성과 성향 해석에 맞는 최적화된 프롬프트 생성
"""

from typing import Dict, List, Any, Optional

class IntelligentPromptGenerator:
    """지능형 프롬프트 생성 클래스"""
    
    def __init__(self, llm_client):
        """초기화"""
        self.llm_client = llm_client
        self.MODEL_NAME = "gpt-4o-mini"
        self.TEMPERATURE = 0.3
    
    def generate_adaptive_prompt(self, issue_data: Dict[str, Any], articles_data: List[Dict], 
                                bias: str, issue_characteristics: Dict[str, Any], 
                                bias_interpretation: Dict[str, str]) -> str:
        """
        이슈 특성에 맞는 동적 프롬프트 생성
        
        Args:
            issue_data: 이슈 데이터
            articles_data: 기사 데이터
            bias: 성향
            issue_characteristics: 이슈 특성 분석 결과
            bias_interpretation: 성향 해석 결과
            
        Returns:
            str: 최적화된 프롬프트
        """
        try:
            # 기사 요약 생성
            articles_summary = self._summarize_articles(articles_data)
            
            # 성향별 톤 가이드
            tone_guides = {
                "left": "사회적 약자 보호와 평등을 강조하되 균형잡힌 관점을 유지",
                "center": "양측 입장을 고려한 균형적이고 실용적인 관점을 제시",
                "right": "개인 책임과 시장 원리를 강조하되 사회적 책임도 인정"
            }
            
            prompt = f"""당신은 경험이 풍부한 언론인입니다. 다음 정보를 바탕으로 {bias} 성향의 자연스럽고 읽기 쉬운 관점을 생성해주세요.

[이슈 정보]
제목: {issue_data.get('title', '')}
부제목: {issue_data.get('subtitle', '')}

[이슈 특성]
- 유형: {issue_characteristics['issue_type']}
- 이해관계자: {', '.join(issue_characteristics['stakeholders'])}
- 갈등: {', '.join(issue_characteristics['core_conflicts'])}
- 복잡도: {issue_characteristics['complexity']}
- 시간적 맥락: {issue_characteristics['temporal_context']}

[성향 해석]
{bias_interpretation}

[관련 기사 요약]
{articles_summary}

생성 기준:
1. 제목: 20-30자 내외, 자연스럽고 직관적인 표현
2. 내용: 300자 내외, 자연스러운 문장 흐름 (기승전결 강제하지 않음)
3. 톤: {tone_guides.get(bias, '균형잡힌 관점')}
4. 객관성: 감정적 표현 배제, 사실 중심
5. 명확성: 모호하지 않은 구체적 표현

다음 형식으로만 응답해주세요:

제목: [자연스러운 제목]
내용: [자연스러운 흐름의 내용]"""
            
            return prompt
            
        except Exception as e:
            print(f"❌ 프롬프트 생성 실패: {str(e)}")
            return self._get_fallback_prompt(issue_data, bias)
    
    def _summarize_articles(self, articles_data: List[Dict]) -> str:
        """기사들을 요약하여 분석용 텍스트 생성"""
        
        summaries = []
        for i, article in enumerate(articles_data[:5], 1):  # 최대 5개 기사
            content = article.get('merged_content', '')
            # 기사 앞 3문장 추출
            first_sentences = self._extract_first_3_sentences(content)
            
            summaries.append(f"{i}. ({article.get('media_name', '')} - {article.get('bias', '')}): {first_sentences}")
        
        return "\n".join(summaries)
    
    def _extract_first_3_sentences(self, content: str) -> str:
        """기사 본문에서 앞 3문장만 추출"""
        
        import re
        
        # 문장 분리
        sentences = re.split(r'(?<!\d)[.!?]+(?!\d)', content)
        
        # 문장 정리
        clean_sentences = []
        for sentence in sentences:
            sentence = sentence.strip()
            if len(sentence) > 10 and re.search(r'[가-힣]', sentence):
                clean_sentences.append(sentence)
        
        # 앞 3문장 선택
        first_3 = clean_sentences[:3]
        
        # 길이 제한 (총 300자)
        result_sentences = []
        total_length = 0
        
        for sentence in first_3:
            if total_length + len(sentence) > 300:
                break
            result_sentences.append(sentence)
            total_length += len(sentence)
        
        # 결합
        result = '. '.join(result_sentences)
        if result and not result.endswith('.'):
            result += '.'
        
        return result
    
    def _get_fallback_prompt(self, issue_data: Dict[str, Any], bias: str) -> str:
        """기본 프롬프트 반환"""
        
        return f"""당신은 경험이 풍부한 언론인입니다. 다음 이슈에 대한 {bias} 성향의 관점을 생성해주세요.

[이슈 정보]
제목: {issue_data.get('title', '')}
부제목: {issue_data.get('subtitle', '')}

생성 기준:
1. 제목: 20-30자 내외, 자연스러운 표현
2. 내용: 300자 내외, 자연스러운 흐름
3. 객관성: 사실 중심의 균형잡힌 관점

다음 형식으로만 응답해주세요:

제목: [자연스러운 제목]
내용: [자연스러운 흐름의 내용]"""
