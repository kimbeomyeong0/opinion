#!/usr/bin/env python3
"""
지능형 프롬프트 생성 모듈
이슈 특성과 맥락에 맞는 적응형 프롬프트를 동적으로 생성
"""

import re
from typing import Dict, List, Any, Optional
from issue_analyzer import IssueAnalyzer
from contextual_bias_interpreter import ContextualBiasInterpreter
from multi_layer_view_generator import MultiLayerViewGenerator

class IntelligentPromptGenerator:
    """지능형 프롬프트 생성 클래스"""
    
    def __init__(self):
        """초기화"""
        self.issue_analyzer = IssueAnalyzer()
        self.bias_interpreter = ContextualBiasInterpreter()
        self.multi_layer_generator = MultiLayerViewGenerator()
        
        # 프롬프트 템플릿 구성 요소
        self.prompt_components = {
            "system_persona": {
                "left": "진보적 가치를 중시하는 정치 분석가",
                "center": "균형잡힌 시각의 정치 분석가", 
                "right": "보수적 가치를 중시하는 정치 분석가"
            },
            "tone_guidance": {
                "경제": "구체적 데이터와 현실적 근거를 바탕으로",
                "환경": "미래 세대와 지속가능성을 고려하여",
                "안보": "국가 안전과 국민 보호를 우선하여",
                "기술": "혁신과 안전의 균형을 고려하여",
                "사회": "사회적 약자와 공정성을 고려하여",
                "정치": "민주주의와 시민 참여를 중시하여"
            },
            "language_style": {
                "높음": "명확하고 직접적인 언어로",
                "중간": "이해하기 쉽고 친근한 언어로",
                "낮음": "신중하고 전문적인 언어로"
            }
        }
    
    def generate_adaptive_prompt(self, issue_data: Dict[str, Any], articles_data: List[Dict], 
                                bias: str) -> str:
        """
        적응형 프롬프트 생성
        
        Args:
            issue_data: 이슈 데이터
            articles_data: 기사 데이터
            bias: 성향
            
        Returns:
            str: 생성된 프롬프트
        """
        try:
            # 이슈 특성 분석
            issue_characteristics = self.issue_analyzer.analyze_issue_characteristics(issue_data)
            
            # 맥락 기반 성향 해석
            bias_interpretation = self.bias_interpreter.interpret_bias_in_context(bias, issue_characteristics)
            
            # 기사 분석
            article_analysis = self._analyze_articles_for_prompt(articles_data, bias)
            
            # 프롬프트 구성 요소 생성
            system_persona = self._generate_system_persona(bias, issue_characteristics)
            context_guidance = self._generate_context_guidance(issue_characteristics, bias_interpretation)
            article_context = self._generate_article_context(article_analysis)
            requirements = self._generate_requirements(issue_characteristics, bias)
            
            # 최종 프롬프트 조합
            prompt = self._combine_prompt_components(
                issue_data, system_persona, context_guidance, 
                article_context, requirements, bias
            )
            
            return prompt
            
        except Exception as e:
            print(f"❌ 프롬프트 생성 실패: {str(e)}")
            return self._get_fallback_prompt(issue_data, bias)
    
    def _analyze_articles_for_prompt(self, articles_data: List[Dict], bias: str) -> Dict[str, Any]:
        """프롬프트용 기사 분석"""
        analysis = {
            "key_insights": [],
            "supporting_data": [],
            "opposing_arguments": [],
            "contextual_factors": [],
            "media_diversity": set(),
            "temporal_span": []
        }
        
        for article in articles_data:
            content = article.get('content', '')
            media_name = article.get('media_name', '')
            published_at = article.get('published_at', '')
            
            # 미디어 다양성 추적
            analysis["media_diversity"].add(media_name)
            
            # 시간적 범위 추적
            if published_at:
                analysis["temporal_span"].append(published_at)
            
            # 핵심 인사이트 추출
            insights = self._extract_key_insights(content)
            analysis["key_insights"].extend(insights)
            
            # 지원 데이터 추출
            data = self._extract_supporting_data(content)
            analysis["supporting_data"].extend(data)
            
            # 반대 논리 추출
            opposing = self._extract_opposing_arguments(content)
            analysis["opposing_arguments"].extend(opposing)
            
            # 맥락적 요소 추출
            context = self._extract_contextual_factors(content)
            analysis["contextual_factors"].extend(context)
        
        # 중복 제거 및 정리
        for key in ["key_insights", "supporting_data", "opposing_arguments", "contextual_factors"]:
            analysis[key] = list(set(analysis[key]))[:3]  # 상위 3개만 유지
        
        analysis["media_diversity"] = list(analysis["media_diversity"])
        
        return analysis
    
    def _extract_key_insights(self, content: str) -> List[str]:
        """핵심 인사이트 추출"""
        insight_patterns = [
            r'핵심은\s+([^.]{10,40})',
            r'중요한\s+점은\s+([^.]{10,40})',
            r'문제는\s+([^.]{10,40})',
            r'해결책은\s+([^.]{10,40})',
            r'핵심\s+문제는\s+([^.]{10,40})'
        ]
        
        insights = []
        for pattern in insight_patterns:
            matches = re.findall(pattern, content)
            insights.extend(matches)
        
        return insights
    
    def _extract_supporting_data(self, content: str) -> List[str]:
        """지원 데이터 추출"""
        data_patterns = [
            r'\d+%',
            r'\d+명',
            r'\d+억원',
            r'\d+조원',
            r'증가했다',
            r'감소했다',
            r'상승했다',
            r'하락했다'
        ]
        
        data = []
        for pattern in data_patterns:
            matches = re.findall(pattern, content)
            data.extend(matches)
        
        return data
    
    def _extract_opposing_arguments(self, content: str) -> List[str]:
        """반대 논리 추출"""
        opposing_patterns = [
            r'하지만\s+([^.]{10,40})',
            r'그러나\s+([^.]{10,40})',
            r'반대로\s+([^.]{10,40})',
            r'비판하는\s+측에서는\s+([^.]{10,40})',
            r'우려하는\s+목소리는\s+([^.]{10,40})'
        ]
        
        opposing = []
        for pattern in opposing_patterns:
            matches = re.findall(pattern, content)
            opposing.extend(matches)
        
        return opposing
    
    def _extract_contextual_factors(self, content: str) -> List[str]:
        """맥락적 요소 추출"""
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
    
    def _generate_system_persona(self, bias: str, issue_characteristics: Dict[str, Any]) -> str:
        """시스템 페르소나 생성"""
        issue_type = issue_characteristics.get("issue_type", "정치")
        complexity = issue_characteristics.get("complexity_level", "중간")
        
        # 기본 페르소나
        base_persona = self.prompt_components["system_persona"].get(bias, "균형잡힌 정치 분석가")
        
        # 이슈 유형별 특화
        issue_specialization = {
            "경제": "경제 정책 전문가",
            "환경": "환경 정책 전문가", 
            "안보": "국방 안보 전문가",
            "기술": "기술 정책 전문가",
            "사회": "사회 정책 전문가"
        }
        
        specialization = issue_specialization.get(issue_type, "정치 분석가")
        
        # 복잡도별 접근 방식
        complexity_approach = {
            "단순": "명확하고 직접적으로",
            "중간": "균형잡히고 신중하게",
            "복합": "다각적이고 종합적으로"
        }
        
        approach = complexity_approach.get(complexity, "균형잡히게")
        
        return f"{base_persona}이자 {specialization}로서 {approach} 분석합니다"
    
    def _generate_context_guidance(self, issue_characteristics: Dict[str, Any], 
                                  bias_interpretation: Dict[str, str]) -> str:
        """맥락 가이드라인 생성"""
        issue_type = issue_characteristics.get("issue_type", "정치")
        urgency = issue_characteristics.get("urgency_level", "중간")
        stakeholders = issue_characteristics.get("stakeholders", [])
        value_conflicts = issue_characteristics.get("core_values_in_conflict", [])
        
        guidance_parts = []
        
        # 이슈 유형별 톤 가이드
        tone_guidance = self.prompt_components["tone_guidance"].get(issue_type, "균형잡힌 관점에서")
        guidance_parts.append(f"이슈 유형: {issue_type} - {tone_guidance}")
        
        # 긴급성별 언어 스타일
        language_style = self.prompt_components["language_style"].get(urgency, "이해하기 쉽게")
        guidance_parts.append(f"긴급성: {urgency} - {language_style} 설명")
        
        # 이해관계자 고려
        if stakeholders:
            guidance_parts.append(f"주요 이해관계자: {', '.join(stakeholders)} 모두의 입장을 고려")
        
        # 가치 갈등 고려
        if value_conflicts:
            guidance_parts.append(f"핵심 갈등: {', '.join(value_conflicts)}의 균형점 모색")
        
        # 성향별 맥락적 해석
        core_values = bias_interpretation.get("core_values", "")
        approach_style = bias_interpretation.get("approach_style", "")
        
        if core_values and approach_style:
            guidance_parts.append(f"성향적 맥락: {core_values}을 중시하며 {approach_style}")
        
        return " | ".join(guidance_parts)
    
    def _generate_article_context(self, article_analysis: Dict[str, Any]) -> str:
        """기사 맥락 생성"""
        context_parts = []
        
        # 핵심 인사이트
        if article_analysis["key_insights"]:
            insights_text = ", ".join(article_analysis["key_insights"][:2])
            context_parts.append(f"핵심 인사이트: {insights_text}")
        
        # 지원 데이터
        if article_analysis["supporting_data"]:
            data_text = ", ".join(article_analysis["supporting_data"][:2])
            context_parts.append(f"주요 데이터: {data_text}")
        
        # 반대 논리
        if article_analysis["opposing_arguments"]:
            opposing_text = ", ".join(article_analysis["opposing_arguments"][:2])
            context_parts.append(f"반대 의견: {opposing_text}")
        
        # 미디어 다양성
        media_count = len(article_analysis["media_diversity"])
        if media_count > 1:
            context_parts.append(f"다양한 미디어({media_count}개)의 관점 참고")
        
        return " | ".join(context_parts) if context_parts else "기사 분석 결과 없음"
    
    def _generate_requirements(self, issue_characteristics: Dict[str, Any], bias: str) -> List[str]:
        """요구사항 생성"""
        requirements = []
        
        issue_type = issue_characteristics.get("issue_type", "정치")
        complexity = issue_characteristics.get("complexity_level", "중간")
        urgency = issue_characteristics.get("urgency_level", "중간")
        
        # 기본 요구사항
        requirements.extend([
            "정확히 200자 이내로 작성 (절대 초과 금지)",
            "맥락과 근거를 충분히 포함하여 표현",
            "20-30대가 이해하기 쉬운 언어 사용",
            "정치용어는 괄호로 설명 추가"
        ])
        
        # 이슈 유형별 요구사항
        if issue_type == "경제":
            requirements.append("구체적 데이터와 현실적 근거 제시")
        elif issue_type == "환경":
            requirements.append("장기적 영향과 미래 세대 고려")
        elif issue_type == "기술":
            requirements.append("기술의 장단점과 사회적 영향 고려")
        elif issue_type == "안보":
            requirements.append("국가 안전과 국민 보호 우선")
        
        # 복잡도별 요구사항
        if complexity == "복합":
            requirements.append("다각적 관점과 종합적 접근")
        elif complexity == "단순":
            requirements.append("핵심에 집중한 명확한 입장")
        
        # 긴급성별 요구사항
        if urgency == "높음":
            requirements.append("시급성과 효과성 강조")
        elif urgency == "낮음":
            requirements.append("신중한 검토와 계획적 접근")
        
        # 성향별 요구사항
        if bias == "left":
            requirements.append("사회적 공정성과 약자 보호 우선")
        elif bias == "right":
            requirements.append("개인 자유와 시장 신뢰 우선")
        else:  # center
            requirements.append("균형과 절충, 실용적 해결책")
        
        return requirements
    
    def _combine_prompt_components(self, issue_data: Dict[str, Any], system_persona: str,
                                  context_guidance: str, article_context: str, 
                                  requirements: List[str], bias: str) -> str:
        """프롬프트 구성 요소 결합"""
        title = issue_data.get('title', '')
        subtitle = issue_data.get('subtitle', '')
        
        # 요구사항을 번호가 있는 목록으로 변환
        requirements_text = "\n".join([f"{i+1}. {req}" for i, req in enumerate(requirements)])
        
        prompt = f"""당신은 {system_persona}입니다.

이슈: {title}
부제목: {subtitle}

맥락 가이드라인:
{context_guidance}

기사 분석 결과:
{article_context}

요구사항:
{requirements_text}

위의 맥락과 가이드라인을 바탕으로 {bias} 성향의 관점을 생성해주세요.
다른 성향의 입장도 이해하고 있음을 보여주되, {bias} 관점에서 명확한 입장을 제시하세요.

⚠️ 중요: 반드시 200자 이내로 작성하고, 초과 시 다시 작성하세요.

{bias} 관점:"""
        
        return prompt
    
    def _get_fallback_prompt(self, issue_data: Dict[str, Any], bias: str) -> str:
        """기본 프롬프트 (오류 시)"""
        title = issue_data.get('title', '')
        subtitle = issue_data.get('subtitle', '')
        
        return f"""다음 이슈에 대한 {bias} 성향의 관점을 200자 이내로 작성해주세요.

이슈: {title}
부제목: {subtitle}

요구사항:
1. 정확히 200자 이내로 작성
2. 맥락과 근거를 충분히 포함하여 표현
3. 20-30대가 이해하기 쉬운 언어 사용
4. 정치용어는 괄호로 설명 추가
5. {bias} 성향의 핵심 가치 반영

{bias} 관점:"""
    
    def generate_follow_up_prompts(self, original_view: str, bias: str, 
                                  issue_characteristics: Dict[str, Any]) -> List[str]:
        """
        후속 질문 프롬프트 생성
        
        Args:
            original_view: 원본 관점
            bias: 성향
            issue_characteristics: 이슈 특성
            
        Returns:
            List[str]: 후속 질문 프롬프트들
        """
        follow_up_prompts = []
        
        issue_type = issue_characteristics.get("issue_type", "정치")
        
        # 일반적 후속 질문
        general_questions = [
            "이 관점의 구체적 근거는 무엇인가요?",
            "다른 성향에서는 어떻게 볼까요?",
            "실제로 실행 가능한 방안인가요?",
            "예상되는 부작용은 없을까요?"
        ]
        
        # 이슈 유형별 특화 질문
        issue_specific_questions = {
            "경제": [
                "경제적 파급효과는 어떻게 될까요?",
                "국제 경쟁력에 미치는 영향은?",
                "예산 부담은 얼마나 될까요?"
            ],
            "환경": [
                "장기적 환경 영향은 어떻게 될까요?",
                "국제적 환경 기준과 비교하면?",
                "기술적 실현가능성은 어느 정도인가요?"
            ],
            "기술": [
                "기술 발전 속도는 고려했나요?",
                "디지털 격차 문제는 어떻게 해결할까요?",
                "개인정보 보호는 어떻게 보장하나요?"
            ],
            "안보": [
                "국제법과의 일치성은?",
                "동맹국과의 협력은 어떻게 될까요?",
                "예상되는 위협 수준은 어느 정도인가요?"
            ]
        }
        
        # 질문 조합
        follow_up_prompts.extend(general_questions)
        
        if issue_type in issue_specific_questions:
            follow_up_prompts.extend(issue_specific_questions[issue_type])
        
        return follow_up_prompts[:6]  # 최대 6개 질문
