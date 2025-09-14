#!/usr/bin/env python3
"""
다층적 관점 생성 모듈
기본입장-근거-대안의 3단계 구조로 깊이 있는 관점 생성
"""

import re
from typing import Dict, List, Any, Optional, Tuple
from issue_analyzer import IssueAnalyzer
from contextual_bias_interpreter import ContextualBiasInterpreter

class MultiLayerViewGenerator:
    """다층적 관점 생성 클래스"""
    
    def __init__(self):
        """초기화"""
        self.issue_analyzer = IssueAnalyzer()
        self.bias_interpreter = ContextualBiasInterpreter()
        
        # 성향별 기본 구조 템플릿
        self.view_structure_templates = {
            "left": {
                "basic_stance": "정부와 사회의 책임을 강조하며",
                "reasoning_approach": "구조적 문제와 사회적 불평등의 관점에서",
                "alternative_focus": "다른 성향의 입장을 인정하되 사회적 공정성 우선"
            },
            "center": {
                "basic_stance": "균형과 절충의 관점에서",
                "reasoning_approach": "양측의 장단점을 고려하여",
                "alternative_focus": "다른 성향의 입장을 모두 존중하며 실용적 해결책 모색"
            },
            "right": {
                "basic_stance": "개인 책임과 시장 원리를 중시하며",
                "reasoning_approach": "효율성과 자율성의 관점에서",
                "alternative_focus": "다른 성향의 입장을 이해하되 개인 자유와 시장 신뢰 우선"
            }
        }
    
    def generate_multi_layer_view(self, issue_data: Dict[str, Any], articles_data: List[Dict], bias: str) -> Dict[str, str]:
        """
        다층적 관점 생성
        
        Args:
            issue_data: 이슈 데이터
            articles_data: 관련 기사 데이터
            bias: 성향
            
        Returns:
            Dict: 다층적 관점 (basic_stance, reasoning, alternative)
        """
        try:
            # 이슈 특성 분석
            issue_characteristics = self.issue_analyzer.analyze_issue_characteristics(issue_data)
            
            # 맥락 기반 성향 해석
            bias_interpretation = self.bias_interpreter.interpret_bias_in_context(bias, issue_characteristics)
            
            # 기사 분석
            article_insights = self._analyze_articles_for_insights(articles_data, bias)
            
            # 다층적 관점 생성
            multi_layer_view = {
                "basic_stance": self._generate_basic_stance(
                    issue_data, issue_characteristics, bias_interpretation, article_insights, bias
                ),
                "reasoning": self._generate_reasoning(
                    issue_data, issue_characteristics, bias_interpretation, article_insights, bias
                ),
                "alternative": self._generate_alternative(
                    issue_data, issue_characteristics, bias_interpretation, article_insights, bias
                )
            }
            
            return multi_layer_view
            
        except Exception as e:
            print(f"❌ 다층적 관점 생성 실패: {str(e)}")
            return self._get_fallback_view(bias)
    
    def _analyze_articles_for_insights(self, articles_data: List[Dict], bias: str) -> Dict[str, Any]:
        """기사 분석을 통한 인사이트 추출"""
        insights = {
            "key_arguments": [],
            "supporting_evidence": [],
            "opposing_views": [],
            "contextual_factors": []
        }
        
        for article in articles_data:
            content = article.get('content', '')
            media_name = article.get('media_name', '')
            
            # 핵심 논리 추출
            arguments = self._extract_arguments(content)
            insights["key_arguments"].extend(arguments)
            
            # 근거 추출
            evidence = self._extract_evidence(content)
            insights["supporting_evidence"].extend(evidence)
            
            # 반대 의견 추출
            opposing = self._extract_opposing_views(content)
            insights["opposing_views"].extend(opposing)
            
            # 맥락적 요소 추출
            context = self._extract_contextual_factors(content)
            insights["contextual_factors"].extend(context)
        
        # 중복 제거 및 정리
        for key in insights:
            insights[key] = list(set(insights[key]))[:5]  # 상위 5개만 유지
        
        return insights
    
    def _extract_arguments(self, content: str) -> List[str]:
        """핵심 논리 추출"""
        # 논리적 연결어구를 통한 주장 추출
        argument_patterns = [
            r'따라서\s+([^.]{10,50})',
            r'그러므로\s+([^.]{10,50})',
            r'결론적으로\s+([^.]{10,50})',
            r'이는\s+([^.]{10,50})',
            r'바로\s+([^.]{10,50})'
        ]
        
        arguments = []
        for pattern in argument_patterns:
            matches = re.findall(pattern, content)
            arguments.extend(matches)
        
        return arguments
    
    def _extract_evidence(self, content: str) -> List[str]:
        """근거 추출"""
        # 데이터, 통계, 사례 등을 통한 근거 추출
        evidence_patterns = [
            r'\d+%',
            r'\d+명',
            r'\d+억원',
            r'조사에\s+따르면\s+([^.]{10,50})',
            r'통계에\s+따르면\s+([^.]{10,50})',
            r'예를\s+들면\s+([^.]{10,50})'
        ]
        
        evidence = []
        for pattern in evidence_patterns:
            matches = re.findall(pattern, content)
            evidence.extend(matches)
        
        return evidence
    
    def _extract_opposing_views(self, content: str) -> List[str]:
        """반대 의견 추출"""
        # 반대 의견을 나타내는 패턴 추출
        opposing_patterns = [
            r'하지만\s+([^.]{10,50})',
            r'그러나\s+([^.]{10,50})',
            r'반대로\s+([^.]{10,50})',
            r'다른\s+의견으로는\s+([^.]{10,50})',
            r'비판하는\s+측에서는\s+([^.]{10,50})'
        ]
        
        opposing = []
        for pattern in opposing_patterns:
            matches = re.findall(pattern, content)
            opposing.extend(matches)
        
        return opposing
    
    def _extract_contextual_factors(self, content: str) -> List[str]:
        """맥락적 요소 추출"""
        # 시간, 장소, 상황 등의 맥락 요소 추출
        context_patterns = [
            r'현재\s+([^.]{10,30})',
            r'최근\s+([^.]{10,30})',
            r'이번\s+([^.]{10,30})',
            r'지역적으로\s+([^.]{10,30})',
            r'국제적으로\s+([^.]{10,30})'
        ]
        
        context = []
        for pattern in context_patterns:
            matches = re.findall(pattern, content)
            context.extend(matches)
        
        return context
    
    def _generate_basic_stance(self, issue_data: Dict[str, Any], issue_characteristics: Dict[str, Any], 
                              bias_interpretation: Dict[str, str], article_insights: Dict[str, Any], bias: str) -> str:
        """기본 입장 생성 (60자 내외)"""
        issue_type = issue_characteristics.get("issue_type", "정치")
        urgency = issue_characteristics.get("urgency_level", "중간")
        
        # 이슈 유형별 기본 입장 템플릿
        stance_templates = {
            "경제": {
                "left": "정부의 적극적 개입으로 경제적 불평등을 해소해야 한다",
                "center": "시장과 정부의 역할 균형을 통해 경제 문제를 해결해야 한다",
                "right": "시장 원리를 신뢰하고 정부 개입을 최소화해야 한다"
            },
            "환경": {
                "left": "환경 보호는 선택이 아닌 의무이며 적극적 정책이 필요하다",
                "center": "환경과 경제의 균형을 맞춘 지속가능한 정책이 필요하다",
                "right": "기술 혁신과 시장 메커니즘을 통한 환경 문제 해결이 필요하다"
            },
            "기술": {
                "left": "기술 발전의 사회적 책임과 시민 보호를 우선해야 한다",
                "center": "기술의 장단점을 고려한 균형적 접근이 필요하다",
                "right": "기술 혁신을 통한 경쟁력 강화와 시장 발전이 필요하다"
            }
        }
        
        # 이슈 유형별 템플릿 사용
        if issue_type in stance_templates:
            base_stance = stance_templates[issue_type].get(bias, "신중한 접근이 필요하다")
        else:
            # 일반적 템플릿 사용
            template = self.view_structure_templates.get(bias, {})
            base_stance = f"{template.get('basic_stance', '균형잡힌 관점에서')} 이 문제를 해결해야 한다"
        
        # 긴급성에 따른 조정
        if urgency == "높음":
            base_stance = base_stance.replace("해야 한다", "시급히 해결해야 한다")
        
        return base_stance
    
    def _generate_reasoning(self, issue_data: Dict[str, Any], issue_characteristics: Dict[str, Any], 
                           bias_interpretation: Dict[str, str], article_insights: Dict[str, Any], bias: str) -> str:
        """근거 설명 생성 (100자 내외)"""
        issue_type = issue_characteristics.get("issue_type", "정치")
        complexity = issue_characteristics.get("complexity_level", "중간")
        
        # 핵심 가치와 접근 방식 기반 근거 구성
        core_values = bias_interpretation.get("core_values", "")
        approach_style = bias_interpretation.get("approach_style", "")
        
        # 기사 인사이트 활용
        key_arguments = article_insights.get("key_arguments", [])
        supporting_evidence = article_insights.get("supporting_evidence", [])
        
        # 근거 구성
        reasoning_parts = []
        
        # 1. 가치 기반 근거
        if core_values:
            reasoning_parts.append(f"{core_values}의 관점에서")
        
        # 2. 접근 방식 근거
        if approach_style:
            reasoning_parts.append(f"{approach_style} 접근이 필요하다")
        
        # 3. 구체적 근거 (기사에서 추출)
        if supporting_evidence:
            evidence_text = supporting_evidence[0][:20] if supporting_evidence else ""
            if evidence_text:
                reasoning_parts.append(f"실제로 {evidence_text} 등의 근거가 있다")
        
        # 4. 복잡도 고려
        if complexity == "복합":
            reasoning_parts.append("이 문제는 여러 측면을 고려해야 하는 복합적 이슈다")
        
        # 근거 조합
        if len(reasoning_parts) >= 2:
            reasoning = f"{reasoning_parts[0]}, {reasoning_parts[1]}"
            if len(reasoning_parts) > 2:
                reasoning += f" 또한 {reasoning_parts[2]}"
        else:
            reasoning = reasoning_parts[0] if reasoning_parts else "신중한 검토가 필요하다"
        
        return reasoning
    
    def _generate_alternative(self, issue_data: Dict[str, Any], issue_characteristics: Dict[str, Any], 
                             bias_interpretation: Dict[str, str], article_insights: Dict[str, Any], bias: str) -> str:
        """대안 관점 생성 (60자 내외)"""
        issue_type = issue_characteristics.get("issue_type", "정치")
        
        # 반대 의견 인정과 대안 제시
        opposing_views = article_insights.get("opposing_views", [])
        
        # 성향별 대안 접근
        alternative_templates = {
            "left": "다른 성향의 우려도 이해하되 사회적 공정성과 약자 보호가 우선이다",
            "center": "다른 성향의 입장도 존중하며 실용적이고 균형잡힌 해결책을 모색한다",
            "right": "다른 성향의 입장도 고려하되 개인 자유와 시장 신뢰가 우선이다"
        }
        
        base_alternative = alternative_templates.get(bias, "다른 관점도 고려하여")
        
        # 이슈 유형별 구체적 대안
        if issue_type == "경제":
            if bias == "left":
                base_alternative += " 시장 효율성도 인정하되 공정한 분배가 더 중요하다"
            elif bias == "right":
                base_alternative += " 사회적 안전망도 필요하되 시장 원리를 우선한다"
            else:
                base_alternative += " 성장과 분배의 균형점을 찾아야 한다"
        
        elif issue_type == "환경":
            if bias == "left":
                base_alternative += " 경제적 부담도 고려하되 환경 보호가 우선이다"
            elif bias == "right":
                base_alternative += " 환경 보호도 중요하되 경제적 실현가능성을 고려한다"
            else:
                base_alternative += " 환경과 경제의 조화를 추구한다"
        
        return base_alternative
    
    def _get_fallback_view(self, bias: str) -> Dict[str, str]:
        """기본 관점 (오류 시)"""
        return {
            "basic_stance": f"{bias} 성향의 기본 입장을 제시한다",
            "reasoning": "신중한 검토와 균형잡힌 접근이 필요하다",
            "alternative": "다른 관점도 고려하여 종합적 해결책을 모색한다"
        }
    
    def combine_multi_layer_view(self, multi_layer_view: Dict[str, str], char_limit: int = 100) -> str:
        """
        다층적 관점을 하나의 텍스트로 결합
        
        Args:
            multi_layer_view: 다층적 관점 딕셔너리
            char_limit: 최대 문자 수
            
        Returns:
            str: 결합된 관점 텍스트
        """
        basic_stance = multi_layer_view.get("basic_stance", "")
        reasoning = multi_layer_view.get("reasoning", "")
        alternative = multi_layer_view.get("alternative", "")
        
        # 기본 구조: [기본입장] [근거] [대안]
        combined = f"{basic_stance} {reasoning} {alternative}"
        
        # 문자 수 제한
        if len(combined) > char_limit:
            # 우선순위: 기본입장 > 근거 > 대안
            if len(basic_stance) <= char_limit:
                combined = basic_stance
            elif len(f"{basic_stance} {reasoning}") <= char_limit:
                combined = f"{basic_stance} {reasoning}"
            else:
                # 기본입장만으로도 초과하면 자르기
                combined = basic_stance[:char_limit-3] + "..."
        
        return combined.strip()
    
    def generate_structured_prompt(self, issue_data: Dict[str, Any], articles_data: List[Dict], 
                                  bias: str, issue_characteristics: Dict[str, Any]) -> str:
        """
        구조화된 프롬프트 생성
        
        Args:
            issue_data: 이슈 데이터
            articles_data: 기사 데이터
            bias: 성향
            issue_characteristics: 이슈 특성
            
        Returns:
            str: 구조화된 프롬프트
        """
        title = issue_data.get('title', '')
        subtitle = issue_data.get('subtitle', '')
        
        # 맥락 기반 성향 해석 가이드라인
        bias_guidance = self.bias_interpreter.generate_contextual_prompt_guidance(bias, issue_characteristics)
        
        # 기사 분석 결과
        article_insights = self._analyze_articles_for_insights(articles_data, bias)
        
        # 기사 내용 요약
        articles_summary = ""
        for i, article in enumerate(articles_data[:3], 1):  # 상위 3개 기사만
            content = article.get('content', '')[:200]  # 200자로 제한
            media_name = article.get('media_name', '')
            articles_summary += f"\n[기사 {i}] ({media_name}): {content}\n"
        
        prompt = f"""
다음 이슈에 대한 {bias} 성향의 다층적 관점을 생성해주세요.

이슈: {title}
부제목: {subtitle}

이슈 특성:
- 유형: {issue_characteristics.get('issue_type', '정치')}
- 복잡도: {issue_characteristics.get('complexity_level', '중간')}
- 긴급성: {issue_characteristics.get('urgency_level', '중간')}
- 이해관계자: {', '.join(issue_characteristics.get('stakeholders', []))}

관련 기사들:
{articles_summary}

{bias_guidance}

요구사항:
1. [기본입장] (60자 내외): 이슈에 대한 명확한 입장 제시
2. [근거설명] (100자 내외): 왜 그런 입장인지 구체적 근거 제시
3. [대안관점] (60자 내외): 다른 성향의 입장 인정과 반박

전체 200자 이내로 작성하되, 위 3단계 구조를 명확히 구분하여 작성해주세요.

{bias} 관점:
"""
        
        return prompt.strip()
