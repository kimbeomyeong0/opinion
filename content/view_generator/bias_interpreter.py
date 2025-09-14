#!/usr/bin/env python3
"""
LLM 기반 성향 해석 모듈
이슈 맥락에 맞는 동적 성향 해석
"""

from typing import Dict, List, Any, Optional

class LLMBasedBiasInterpreter:
    """LLM 기반 성향 해석 클래스"""
    
    def __init__(self, llm_client):
        """초기화"""
        self.llm_client = llm_client
        self.MODEL_NAME = "gpt-4o-mini"
        self.TEMPERATURE = 0.3
    
    def interpret_bias_in_context(self, bias: str, issue_characteristics: Dict[str, Any], articles_data: List[Dict]) -> Dict[str, str]:
        """
        이슈 맥락에 맞는 성향 해석
        
        Args:
            bias: 성향 (left, center, right)
            issue_characteristics: 이슈 특성 분석 결과
            articles_data: 기사 데이터
            
        Returns:
            Dict: 성향 해석 결과
        """
        try:
            # 기사 요약 생성
            articles_summary = self._summarize_articles(articles_data)
            
            # 성향별 시스템 메시지
            bias_personas = {
                "left": "당신은 진보적 가치를 중시하는 정치 분석가입니다. 사회적 약자 보호와 평등을 우선시합니다.",
                "center": "당신은 균형잡힌 시각의 정치 분석가입니다. 양측 입장을 고려한 실용적 접근을 중시합니다.",
                "right": "당신은 보수적 가치를 중시하는 정치 분석가입니다. 개인 책임과 시장 원리를 우선시합니다."
            }
            
            prompt = f"""다음 이슈 맥락을 고려하여 {bias} 성향의 관점을 해석해주세요.

[이슈 특성]
- 유형: {issue_characteristics['issue_type']}
- 이해관계자: {', '.join(issue_characteristics['stakeholders'])}
- 갈등 구조: {', '.join(issue_characteristics['core_conflicts'])}
- 복잡도: {issue_characteristics['complexity']}
- 시간적 맥락: {issue_characteristics['temporal_context']}

[관련 기사 요약]
{articles_summary}

{bias} 성향의 관점을 다음 형식으로 해석해주세요:

핵심 가치: [가장 중요한 가치관]
접근 방식: [문제 해결 접근법]
주요 관심사: [중점적으로 다루는 영역]
세밀한 입장: [다른 성향과의 차이점과 공통점]"""
            
            system_message = bias_personas.get(bias, bias_personas["center"])
            
            response = self._call_llm(prompt, system_message)
            return self._parse_bias_interpretation(response)
            
        except Exception as e:
            print(f"❌ 성향 해석 실패: {str(e)}")
            return self._get_fallback_interpretation(bias)
    
    def _summarize_articles(self, articles_data: List[Dict]) -> str:
        """기사들을 요약하여 분석용 텍스트 생성"""
        
        summaries = []
        for i, article in enumerate(articles_data[:3], 1):  # 최대 3개 기사
            content = article.get('merged_content', '')
            # 간단한 요약 (앞 200자)
            summary = content[:200] + "..." if len(content) > 200 else content
            
            summaries.append(f"{i}. ({article.get('media_name', '')} - {article.get('bias', '')}): {summary}")
        
        return "\n".join(summaries)
    
    def _call_llm(self, prompt: str, system_message: str) -> str:
        """LLM 호출"""
        
        response = self.llm_client.chat.completions.create(
            model=self.MODEL_NAME,
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": prompt}
            ],
            max_tokens=400,
            temperature=self.TEMPERATURE
        )
        
        return response.choices[0].message.content.strip()
    
    def _parse_bias_interpretation(self, response: str) -> Dict[str, str]:
        """LLM 응답을 파싱하여 구조화된 데이터로 변환"""
        
        try:
            # 각 항목 추출
            core_values = self._extract_value(response, "핵심 가치:")
            approach = self._extract_value(response, "접근 방식:")
            focus_areas = self._extract_value(response, "주요 관심사:")
            nuanced_stance = self._extract_value(response, "세밀한 입장:")
            
            return {
                "core_values": core_values.strip(),
                "approach_style": approach.strip(),
                "issue_specific_considerations": focus_areas.strip(),
                "nuanced_stance": nuanced_stance.strip()
            }
            
        except Exception as e:
            print(f"❌ 응답 파싱 실패: {str(e)}")
            return self._get_fallback_interpretation("center")
    
    def _extract_value(self, text: str, key: str) -> str:
        """텍스트에서 특정 키의 값 추출"""
        
        lines = text.split('\n')
        for line in lines:
            if line.strip().startswith(key):
                return line.replace(key, '').strip()
        
        return ""
    
    def _get_fallback_interpretation(self, bias: str) -> Dict[str, str]:
        """기본 성향 해석 반환"""
        
        fallback_interpretations = {
            "left": {
                "core_values": "사회적 공정성과 약자 보호",
                "approach_style": "정부 주도의 사회 개혁",
                "issue_specific_considerations": "구조적 불평등 해소",
                "nuanced_stance": "시장 원리는 인정하되 공정한 분배 우선"
            },
            "center": {
                "core_values": "균형과 절충, 실용적 접근",
                "approach_style": "양측 입장을 고려한 중도적 해결책",
                "issue_specific_considerations": "현실적이고 실행 가능한 방안",
                "nuanced_stance": "극단을 피하고 실용적인 해결책 추구"
            },
            "right": {
                "core_values": "개인 책임과 시장 원리",
                "approach_style": "시장 메커니즘을 통한 효율적 해결",
                "issue_specific_considerations": "정부 개입 최소화와 기업 환경 개선",
                "nuanced_stance": "시장 신뢰를 바탕으로 한 성장 우선"
            }
        }
        
        return fallback_interpretations.get(bias, fallback_interpretations["center"])
