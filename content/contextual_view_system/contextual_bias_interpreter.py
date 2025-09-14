#!/usr/bin/env python3
"""
맥락 기반 성향 해석 모듈
이슈 특성에 맞게 성향을 해석하여 고정된 스테레오타입을 피하고 맥락에 맞는 관점 생성
"""

from typing import Dict, List, Any, Optional
from issue_analyzer import IssueAnalyzer

class ContextualBiasInterpreter:
    """맥락 기반 성향 해석 클래스"""
    
    def __init__(self):
        """초기화"""
        self.issue_analyzer = IssueAnalyzer()
        
        # 이슈 유형별 성향 해석 가이드라인
        self.issue_specific_interpretations = {
            "경제": {
                "left": {
                    "core_values": "경제적 평등, 사회적 안전망, 노동자 권리 보호",
                    "approach_style": "정부 주도의 공정한 경제 질서 구축",
                    "issue_specific_considerations": "시장의 한계 인정, 공공성 강조, 분배 정의 실현",
                    "nuanced_stance": "시장 원리는 인정하되 공정한 경쟁 환경과 사회적 안전망 확보 필요"
                },
                "center": {
                    "core_values": "균형과 절충, 실용적 접근, 시장과 정부의 역할 조화",
                    "approach_style": "시장 효율성과 사회적 안전망의 균형점 모색",
                    "issue_specific_considerations": "경제 성장과 분배의 조화, 단계적 개선",
                    "nuanced_stance": "시장과 정부의 역할을 상황에 맞게 조절하며 지속가능한 성장 추구"
                },
                "right": {
                    "core_values": "시장 자유, 개인 책임, 경쟁과 혁신",
                    "approach_style": "시장 메커니즘을 통한 효율적 자원 배분",
                    "issue_specific_considerations": "정부 개입 최소화, 기업 환경 개선, 성장 동력 확보",
                    "nuanced_stance": "시장 신뢰를 바탕으로 한 성장 우선, 필요시 최소한의 정부 개입"
                }
            },
            "환경": {
                "left": {
                    "core_values": "환경 정의, 미래 세대 보호, 기업 책임 강화",
                    "approach_style": "적극적 환경 정책과 기업 규제를 통한 환경 보호",
                    "issue_specific_considerations": "기후 정의, 환경 불평등 해결, 공공성 강조",
                    "nuanced_stance": "환경 보호는 선택이 아닌 의무이며, 기업과 정부 모두 책임져야 함"
                },
                "center": {
                    "core_values": "지속가능성, 균형적 접근, 과학적 근거",
                    "approach_style": "환경과 경제의 조화를 통한 지속가능한 발전",
                    "issue_specific_considerations": "단계적 전환, 기술 혁신 활용, 국제 협력",
                    "nuanced_stance": "환경 보호와 경제 발전을 동시에 추구하며 실현 가능한 방안 모색"
                },
                "right": {
                    "core_values": "기술 혁신, 시장 메커니즘, 경제적 실현가능성",
                    "approach_style": "기술과 시장을 통한 환경 문제 해결",
                    "issue_specific_considerations": "기업 자율성 보장, 기술 개발 지원, 국제 경쟁력 유지",
                    "nuanced_stance": "환경 보호도 시장 원리와 기술 혁신을 통해 효율적으로 해결 가능"
                }
            },
            "기술": {
                "left": {
                    "core_values": "디지털 권리, 공정한 접근, 시민 보호",
                    "approach_style": "기술 발전의 사회적 책임과 공공성 강조",
                    "issue_specific_considerations": "디지털 격차 해소, 개인정보 보호, 알고리즘 공정성",
                    "nuanced_stance": "기술 혁신은 환영하되 시민의 권리와 사회적 공정성 보장이 우선"
                },
                "center": {
                    "core_values": "균형적 발전, 신중한 접근, 이해관계자 조율",
                    "approach_style": "기술의 장단점을 고려한 균형적 정책",
                    "issue_specific_considerations": "혁신과 안전의 균형, 단계적 도입, 지속적 모니터링",
                    "nuanced_stance": "기술 발전을 지지하되 충분한 검토와 안전장치 마련 필요"
                },
                "right": {
                    "core_values": "혁신 우선, 시장 자율, 경쟁력 강화",
                    "approach_style": "기술 혁신을 통한 경제 성장과 경쟁력 확보",
                    "issue_specific_considerations": "규제 완화, 기업 지원, 글로벌 경쟁력",
                    "nuanced_stance": "과도한 규제는 혁신을 저해하므로 기업의 자율적 개발 지원 필요"
                }
            },
            "안보": {
                "left": {
                    "core_values": "평화 우선, 대화와 협력, 국제법 준수",
                    "approach_style": "외교적 해결과 국제 협력을 통한 안보 확보",
                    "issue_specific_considerations": "군사력 증강보다는 대화, 국제 사회와의 협력",
                    "nuanced_stance": "강력한 국방력은 필요하되 평화적 해결을 우선적으로 추구"
                },
                "center": {
                    "core_values": "균형적 안보, 신중한 판단, 다각적 접근",
                    "approach_style": "군사력과 외교를 조화시킨 종합적 안보 전략",
                    "issue_specific_considerations": "위협 수준에 따른 차별적 대응, 동맹국과의 협력",
                    "nuanced_stance": "평화를 추구하되 현실적 위협에 대한 대비도 필요"
                },
                "right": {
                    "core_values": "강력한 국방, 동맹 강화, 현실적 대응",
                    "approach_style": "강력한 군사력을 바탕으로 한 안보 확보",
                    "issue_specific_considerations": "국방비 증액, 동맹국과의 협력 강화, 위협에 대한 단호한 대응",
                    "nuanced_stance": "평화는 힘의 균형을 통해서만 가능하며 강력한 국방력이 필수"
                }
            },
            "사회": {
                "left": {
                    "core_values": "사회적 약자 보호, 복지 확대, 포용성",
                    "approach_style": "정부 주도의 사회 안전망 구축과 불평등 해소",
                    "issue_specific_considerations": "소수자 권리, 사회적 통합, 공정한 기회 제공",
                    "nuanced_stance": "모든 시민이 인간다운 삶을 살 수 있도록 사회적 책임 강화"
                },
                "center": {
                    "core_values": "사회 통합, 균형적 접근, 단계적 개선",
                    "approach_style": "정부와 민간의 협력을 통한 사회 문제 해결",
                    "issue_specific_considerations": "효율성과 공정성의 조화, 실현 가능한 정책",
                    "nuanced_stance": "사회적 약자 보호와 개인 책임의 균형점에서 해결책 모색"
                },
                "right": {
                    "core_values": "개인 책임, 자율성, 효율성",
                    "approach_style": "개인과 가족의 자율성을 통한 사회 문제 해결",
                    "issue_specific_considerations": "정부 의존도 감소, 민간 자율성 확대, 효율적 자원 배분",
                    "nuanced_stance": "사회적 약자도 개인적 노력과 책임을 바탕으로 자립할 수 있도록 지원"
                }
            },
            "법률": {
                "left": {
                    "core_values": "법적 정의, 사회적 공정성, 약자 보호",
                    "approach_style": "강력한 법 집행을 통한 사회 정의 실현",
                    "issue_specific_considerations": "권력형 비리 엄정 대응, 법적 평등 보장, 투명한 수사",
                    "nuanced_stance": "법의 정당한 집행을 통해 사회적 불평등 해소와 정의 실현"
                },
                "center": {
                    "core_values": "법치주의, 공정성, 신중한 판단",
                    "approach_style": "법적 절차 준수와 신중한 수사를 통한 정의 실현",
                    "issue_specific_considerations": "사실관계 명확화, 절차적 공정성, 정치적 중립성",
                    "nuanced_stance": "법적 절차를 준수하면서도 정치적 편향 없이 공정한 수사 진행"
                },
                "right": {
                    "core_values": "법치주의, 개인 자유, 시장 신뢰",
                    "approach_style": "법적 절차 준수와 개인 권리 보호를 통한 정의 실현",
                    "issue_specific_considerations": "개인 자유 보장, 시장 신뢰 유지, 정치적 중립성, 신중한 수사",
                    "nuanced_stance": "법적 절차를 준수하되 개인의 자유와 시장 신뢰를 해치지 않는 선에서 수사"
                }
            }
        }
    
    def interpret_bias_in_context(self, bias: str, issue_characteristics: Dict[str, Any]) -> Dict[str, str]:
        """
        이슈 맥락에 맞게 성향을 해석
        
        Args:
            bias: 성향 (left, center, right)
            issue_characteristics: 이슈 특성 분석 결과
            
        Returns:
            Dict: 맥락에 맞는 성향 해석
        """
        try:
            issue_type = issue_characteristics.get("issue_type", "정치")
            complexity = issue_characteristics.get("complexity_level", "중간")
            urgency = issue_characteristics.get("urgency_level", "중간")
            stakeholders = issue_characteristics.get("stakeholders", [])
            
            # 기본 해석 가져오기
            base_interpretation = self._get_base_interpretation(bias, issue_type)
            
            # 맥락별 조정
            contextual_interpretation = self._adjust_for_context(
                base_interpretation, issue_characteristics
            )
            
            # 복잡도별 조정
            complexity_adjusted = self._adjust_for_complexity(
                contextual_interpretation, complexity
            )
            
            # 긴급성별 조정
            urgency_adjusted = self._adjust_for_urgency(
                complexity_adjusted, urgency
            )
            
            # 이해관계자별 조정
            final_interpretation = self._adjust_for_stakeholders(
                urgency_adjusted, stakeholders
            )
            
            return final_interpretation
            
        except Exception as e:
            print(f"❌ 성향 해석 실패: {str(e)}")
            return self._get_fallback_interpretation(bias)
    
    def _get_base_interpretation(self, bias: str, issue_type: str) -> Dict[str, str]:
        """기본 성향 해석 가져오기"""
        if issue_type in self.issue_specific_interpretations:
            return self.issue_specific_interpretations[issue_type].get(bias, {})
        
        # 기본 정치 이슈 해석
        return self._get_default_political_interpretation(bias)
    
    def _get_default_political_interpretation(self, bias: str) -> Dict[str, str]:
        """기본 정치 이슈 성향 해석"""
        default_interpretations = {
            "left": {
                "core_values": "진보적 가치, 사회적 약자 보호, 평등과 정의",
                "approach_style": "정부 주도의 사회 개혁과 불평등 해소",
                "issue_specific_considerations": "사회적 공정성, 약자 권리, 구조적 문제 해결",
                "nuanced_stance": "기존 질서의 문제점을 인식하고 더 공정한 사회를 추구"
            },
            "center": {
                "core_values": "균형과 절충, 실용주의, 합리적 접근",
                "approach_style": "양측 입장을 고려한 중도적 해결책 모색",
                "issue_specific_considerations": "실현 가능성, 사회적 합의, 단계적 개선",
                "nuanced_stance": "극단을 피하고 현실적이고 실용적인 해결책 추구"
            },
            "right": {
                "core_values": "자유시장, 개인 책임, 효율성, 혁신, 경쟁력",
                "approach_style": "시장 원리와 개인 자율성을 통한 효율적 문제 해결",
                "issue_specific_considerations": "정부 개입 최소화, 기업 환경 개선, 시장 신뢰 강화, 개인 자율성 보장",
                "nuanced_stance": "기존 제도의 장점을 인정하되 시장 원리를 통한 지속적 혁신과 발전 추구"
            }
        }
        return default_interpretations.get(bias, {})
    
    def _adjust_for_context(self, interpretation: Dict[str, str], issue_characteristics: Dict[str, Any]) -> Dict[str, str]:
        """맥락에 따른 조정"""
        adjusted = interpretation.copy()
        
        # 이슈 특성에 따른 추가 고려사항
        issue_type = issue_characteristics.get("issue_type", "정치")
        value_conflicts = issue_characteristics.get("core_values_in_conflict", [])
        
        if value_conflicts:
            conflict_context = f" 특히 {', '.join(value_conflicts)}의 균형을 고려하여"
            adjusted["issue_specific_considerations"] += conflict_context
        
        return adjusted
    
    def _adjust_for_complexity(self, interpretation: Dict[str, str], complexity: str) -> Dict[str, str]:
        """복잡도에 따른 조정"""
        adjusted = interpretation.copy()
        
        if complexity == "복합":
            adjusted["approach_style"] += " 다각적이고 종합적인"
            adjusted["nuanced_stance"] += " 이슈의 복잡성을 인정하며"
        elif complexity == "단순":
            adjusted["approach_style"] += " 명확하고 직접적인"
            adjusted["nuanced_stance"] += " 핵심에 집중하여"
        
        return adjusted
    
    def _adjust_for_urgency(self, interpretation: Dict[str, str], urgency: str) -> Dict[str, str]:
        """긴급성에 따른 조정"""
        adjusted = interpretation.copy()
        
        if urgency == "높음":
            adjusted["approach_style"] += " 신속하고 효과적인"
            adjusted["nuanced_stance"] += " 시급한 상황을 고려하여"
        elif urgency == "낮음":
            adjusted["approach_style"] += " 신중하고 계획적인"
            adjusted["nuanced_stance"] += " 장기적 관점에서"
        
        return adjusted
    
    def _adjust_for_stakeholders(self, interpretation: Dict[str, str], stakeholders: List[str]) -> Dict[str, str]:
        """이해관계자에 따른 조정"""
        adjusted = interpretation.copy()
        
        if "기업" in stakeholders and "시민" in stakeholders:
            adjusted["issue_specific_considerations"] += " 기업과 시민의 이익 조화를 고려하여"
        elif "국제기구" in stakeholders:
            adjusted["issue_specific_considerations"] += " 국제적 기준과 협력을 고려하여"
        
        return adjusted
    
    def _get_fallback_interpretation(self, bias: str) -> Dict[str, str]:
        """기본 성향 해석 (오류 시)"""
        return {
            "core_values": f"{bias} 성향의 기본 가치",
            "approach_style": "균형잡힌 접근",
            "issue_specific_considerations": "이슈의 특성을 고려한 접근",
            "nuanced_stance": "맥락에 맞는 입장 제시"
        }
    
    def generate_contextual_prompt_guidance(self, bias: str, issue_characteristics: Dict[str, Any]) -> str:
        """
        맥락 기반 프롬프트 가이드라인 생성
        
        Args:
            bias: 성향
            issue_characteristics: 이슈 특성
            
        Returns:
            str: 프롬프트용 가이드라인 텍스트
        """
        interpretation = self.interpret_bias_in_context(bias, issue_characteristics)
        
        guidance = f"""
{bias.upper()} 성향의 맥락적 해석:

핵심 가치: {interpretation.get('core_values', '')}
접근 방식: {interpretation.get('approach_style', '')}
이슈별 고려사항: {interpretation.get('issue_specific_considerations', '')}
세밀한 입장: {interpretation.get('nuanced_stance', '')}

주의사항:
- 고정된 스테레오타입에 빠지지 말고 이슈의 맥락을 고려하세요
- 다른 성향의 입장도 이해하고 있음을 보여주세요
- 구체적이고 실행 가능한 대안을 제시하세요
- 정치적 용어는 쉽게 풀어서 설명하세요
"""
        
        return guidance.strip()
