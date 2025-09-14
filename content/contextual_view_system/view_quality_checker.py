#!/usr/bin/env python3
"""
관점 품질 검증 모듈
생성된 관점의 품질을 다각도로 검증하고 개선점 제시
"""

import re
from typing import Dict, List, Any, Optional, Tuple
from issue_analyzer import IssueAnalyzer
from contextual_bias_interpreter import ContextualBiasInterpreter

class ViewQualityChecker:
    """관점 품질 검증 클래스"""
    
    def __init__(self):
        """초기화"""
        self.issue_analyzer = IssueAnalyzer()
        self.bias_interpreter = ContextualBiasInterpreter()
        
        # 품질 검증 기준 (완화된 기준)
        self.quality_criteria = {
            "bias_consistency": {
                "weight": 0.20,
                "description": "성향 일관성"
            },
            "issue_relevance": {
                "weight": 0.25,
                "description": "이슈 관련성"
            },
            "nuance_presence": {
                "weight": 0.15,
                "description": "뉘앙스 존재"
            },
            "stereotype_avoidance": {
                "weight": 0.10,
                "description": "스테레오타입 회피"
            },
            "constructive_tone": {
                "weight": 0.15,
                "description": "건설적 톤"
            },
            "clarity": {
                "weight": 0.10,
                "description": "명확성"
            },
            "length_appropriateness": {
                "weight": 0.05,
                "description": "길이 적절성"
            }
        }
        
        # 성향별 키워드 패턴
        self.bias_keywords = {
            "left": {
                "positive": ["공정", "평등", "약자", "사회적", "정부", "책임", "보호", "복지", "정의"],
                "negative": ["불평등", "차별", "억압", "착취", "기득권", "특권"]
            },
            "center": {
                "positive": ["균형", "절충", "신중", "실용", "합리", "조화", "중도", "현실"],
                "negative": ["극단", "편향", "일방", "성급", "무책임"]
            },
            "right": {
                "positive": ["자유", "개인", "시장", "경쟁", "효율", "자율", "책임", "전통", "안정", "혁신", "성장", "투자", "기업", "경영", "신뢰", "원리", "자율성", "경쟁력", "실용", "현실", "개선", "발전"],
                "negative": ["규제", "개입", "통제", "의존", "무책임", "무질서", "억압", "강제", "제한", "억제"]
            }
        }
        
        # 스테레오타입 패턴
        self.stereotype_patterns = [
            r"항상\s+",
            r"모든\s+",
            r"절대\s+",
            r"반드시\s+",
            r"당연히\s+",
            r"당연하다",
            r"틀림없이",
            r"확실히"
        ]
        
        # 건설적 톤 키워드
        self.constructive_keywords = [
            "해결", "개선", "발전", "협력", "대화", "이해", "고려", "모색", "추구", "실현"
        ]
        
        # 비건설적 톤 키워드
        self.destructive_keywords = [
            "반대", "거부", "배척", "무시", "부정", "비판", "공격", "적대", "배타"
        ]
    
    def validate_view_quality(self, view: str, bias: str, issue_characteristics: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        관점 품질 검증
        
        Args:
            view: 검증할 관점 텍스트
            bias: 성향
            issue_characteristics: 이슈 특성
            
        Returns:
            Tuple[bool, Dict]: (품질 통과 여부, 상세 검증 결과)
        """
        try:
            validation_results = {}
            total_score = 0.0
            
            # 각 품질 기준별 검증
            for criterion, config in self.quality_criteria.items():
                score, details = self._check_criterion(view, bias, issue_characteristics, criterion)
                validation_results[criterion] = {
                    "score": score,
                    "details": details,
                    "weight": config["weight"],
                    "description": config["description"]
                }
                total_score += score * config["weight"]
            
            # 전체 품질 평가 (완화된 기준)
            quality_passed = total_score >= 0.5  # 50% 이상이면 통과
            
            validation_results["overall"] = {
                "total_score": total_score,
                "quality_passed": quality_passed,
                "grade": self._get_quality_grade(total_score)
            }
            
            return quality_passed, validation_results
            
        except Exception as e:
            print(f"❌ 품질 검증 실패: {str(e)}")
            return False, {"error": str(e)}
    
    def _check_criterion(self, view: str, bias: str, issue_characteristics: Dict[str, Any], criterion: str) -> Tuple[float, str]:
        """개별 품질 기준 검증"""
        if criterion == "bias_consistency":
            return self._check_bias_consistency(view, bias)
        elif criterion == "issue_relevance":
            return self._check_issue_relevance(view, issue_characteristics)
        elif criterion == "nuance_presence":
            return self._check_nuance_presence(view)
        elif criterion == "stereotype_avoidance":
            return self._check_stereotype_avoidance(view)
        elif criterion == "constructive_tone":
            return self._check_constructive_tone(view)
        elif criterion == "clarity":
            return self._check_clarity(view)
        elif criterion == "length_appropriateness":
            return self._check_length_appropriateness(view)
        else:
            return 0.0, "알 수 없는 기준"
    
    def _check_bias_consistency(self, view: str, bias: str) -> Tuple[float, str]:
        """성향 일관성 검증 (완화된 기준)"""
        if bias not in self.bias_keywords:
            return 0.6, "성향 키워드 없음"
        
        positive_keywords = self.bias_keywords[bias]["positive"]
        negative_keywords = self.bias_keywords[bias]["negative"]
        
        # 긍정적 키워드 매칭
        positive_matches = sum(1 for keyword in positive_keywords if keyword in view)
        negative_matches = sum(1 for keyword in negative_keywords if keyword in view)
        
        # 성향 일관성 점수 계산 (완화된 기준)
        if positive_keywords:
            consistency_score = min(1.0, positive_matches / max(1, len(positive_keywords) // 2))  # 절반만 매칭되어도 만점
        else:
            consistency_score = 0.5
        
        # 부정 키워드에 대한 페널티 완화
        if negative_keywords:
            inconsistency_penalty = min(0.3, negative_matches / max(1, len(negative_keywords) // 2)) * 0.3
        else:
            inconsistency_penalty = 0
        
        final_score = max(0.3, min(1, consistency_score - inconsistency_penalty))  # 최소 0.3 보장
        
        details = f"긍정 키워드: {positive_matches}/{len(positive_keywords)}, 부정 키워드: {negative_matches}/{len(negative_keywords)}"
        
        return final_score, details
    
    def _check_issue_relevance(self, view: str, issue_characteristics: Dict[str, Any]) -> Tuple[float, str]:
        """이슈 관련성 검증"""
        issue_type = issue_characteristics.get("issue_type", "정치")
        stakeholders = issue_characteristics.get("stakeholders", [])
        value_conflicts = issue_characteristics.get("core_values_in_conflict", [])
        
        relevance_score = 0.0
        details_parts = []
        
        # 이슈 유형 관련성
        issue_type_keywords = {
            "경제": ["경제", "성장", "투자", "고용", "시장", "경기"],
            "환경": ["환경", "기후", "탄소", "에너지", "생태", "지속가능"],
            "안보": ["안보", "국방", "안전", "보호", "위협", "방어"],
            "기술": ["기술", "AI", "디지털", "혁신", "데이터", "알고리즘"],
            "사회": ["사회", "복지", "시민", "공정", "포용", "통합"]
        }
        
        if issue_type in issue_type_keywords:
            type_keywords = issue_type_keywords[issue_type]
            type_matches = sum(1 for keyword in type_keywords if keyword in view)
            type_score = min(1.0, type_matches / 2)  # 최대 2개 키워드 매칭
            relevance_score += type_score * 0.4
            details_parts.append(f"이슈 유형 키워드: {type_matches}개")
        
        # 이해관계자 관련성
        if stakeholders:
            stakeholder_matches = sum(1 for stakeholder in stakeholders if stakeholder in view)
            stakeholder_score = min(1.0, stakeholder_matches / len(stakeholders))
            relevance_score += stakeholder_score * 0.3
            details_parts.append(f"이해관계자 언급: {stakeholder_matches}개")
        
        # 가치 갈등 관련성
        if value_conflicts:
            conflict_matches = sum(1 for conflict in value_conflicts for keyword in conflict.split(" vs ") if keyword in view)
            conflict_score = min(1.0, conflict_matches / len(value_conflicts))
            relevance_score += conflict_score * 0.3
            details_parts.append(f"가치 갈등 언급: {conflict_matches}개")
        
        details = " | ".join(details_parts) if details_parts else "관련성 지표 없음"
        
        return min(1.0, relevance_score), details
    
    def _check_nuance_presence(self, view: str) -> Tuple[float, str]:
        """뉘앙스 존재 검증"""
        nuance_indicators = [
            "하지만", "그러나", "다만", "그런데", "반면", "한편",
            "다만", "그러나", "하지만", "그런데", "반면", "한편",
            "일부", "어느 정도", "상당히", "비교적", "상대적으로",
            "고려해야", "생각해볼", "검토해야", "신중해야"
        ]
        
        nuance_matches = sum(1 for indicator in nuance_indicators if indicator in view)
        nuance_score = min(1.0, nuance_matches / 3)  # 최대 3개 매칭
        
        details = f"뉘앙스 지표: {nuance_matches}개"
        
        return nuance_score, details
    
    def _check_stereotype_avoidance(self, view: str) -> Tuple[float, str]:
        """스테레오타입 회피 검증"""
        stereotype_count = sum(1 for pattern in self.stereotype_patterns if re.search(pattern, view))
        stereotype_score = max(0, 1 - stereotype_count * 0.3)  # 스테레오타입 패턴당 0.3 감점
        
        details = f"스테레오타입 패턴: {stereotype_count}개"
        
        return stereotype_score, details
    
    def _check_constructive_tone(self, view: str) -> Tuple[float, str]:
        """건설적 톤 검증"""
        constructive_matches = sum(1 for keyword in self.constructive_keywords if keyword in view)
        destructive_matches = sum(1 for keyword in self.destructive_keywords if keyword in view)
        
        if constructive_matches + destructive_matches == 0:
            return 0.5, "톤 지표 없음"
        
        tone_score = constructive_matches / (constructive_matches + destructive_matches)
        
        details = f"건설적: {constructive_matches}개, 비건설적: {destructive_matches}개"
        
        return tone_score, details
    
    def _check_clarity(self, view: str) -> Tuple[float, str]:
        """명확성 검증"""
        # 문장 길이 분석
        sentences = re.split(r'[.!?]', view)
        sentence_lengths = [len(s.strip()) for s in sentences if s.strip()]
        
        if not sentence_lengths:
            return 0.0, "문장 없음"
        
        avg_sentence_length = sum(sentence_lengths) / len(sentence_lengths)
        
        # 적절한 문장 길이 (10-50자)
        if 10 <= avg_sentence_length <= 50:
            length_score = 1.0
        elif avg_sentence_length < 10:
            length_score = avg_sentence_length / 10
        else:
            length_score = max(0, 1 - (avg_sentence_length - 50) / 50)
        
        # 복잡한 문장 패턴 검사
        complex_patterns = [
            r'[가-힣]+[가-힣]+[가-힣]+[가-힣]+[가-힣]+',  # 5개 이상 한글 연속
            r'[가-힣]+[가-힣]+[가-힣]+[가-힣]+[가-힣]+[가-힣]+'  # 6개 이상 한글 연속
        ]
        
        complex_count = sum(1 for pattern in complex_patterns if re.search(pattern, view))
        complexity_penalty = min(0.3, complex_count * 0.1)
        
        final_score = max(0, length_score - complexity_penalty)
        
        details = f"평균 문장 길이: {avg_sentence_length:.1f}자, 복잡 패턴: {complex_count}개"
        
        return final_score, details
    
    def _check_length_appropriateness(self, view: str) -> Tuple[float, str]:
        """길이 적절성 검증"""
        length = len(view)
        
        # 200자 기준으로 점수 계산
        if 160 <= length <= 200:
            score = 1.0
        elif 140 <= length < 160:
            score = 0.8
        elif 200 < length <= 240:
            score = 0.8
        elif 120 <= length < 140:
            score = 0.6
        elif 240 < length <= 300:
            score = 0.6
        else:
            score = 0.3
        
        details = f"길이: {length}자"
        
        return score, details
    
    def _get_quality_grade(self, score: float) -> str:
        """품질 등급 반환"""
        if score >= 0.9:
            return "A+"
        elif score >= 0.8:
            return "A"
        elif score >= 0.7:
            return "B+"
        elif score >= 0.6:
            return "B"
        elif score >= 0.5:
            return "C+"
        elif score >= 0.4:
            return "C"
        else:
            return "D"
    
    def generate_improvement_suggestions(self, validation_results: Dict[str, Any]) -> List[str]:
        """개선 제안 생성"""
        suggestions = []
        
        for criterion, result in validation_results.items():
            if criterion == "overall":
                continue
                
            score = result["score"]
            description = result["description"]
            
            if score < 0.6:  # 60% 미만이면 개선 제안
                if criterion == "bias_consistency":
                    suggestions.append(f"{description} 개선: 성향에 맞는 키워드를 더 많이 사용하고 반대 키워드는 피하세요")
                elif criterion == "issue_relevance":
                    suggestions.append(f"{description} 개선: 이슈와 관련된 구체적 내용을 더 많이 포함하세요")
                elif criterion == "nuance_presence":
                    suggestions.append(f"{description} 개선: '하지만', '다만', '그러나' 등의 뉘앙스 표현을 사용하세요")
                elif criterion == "stereotype_avoidance":
                    suggestions.append(f"{description} 개선: '항상', '모든', '절대' 등의 극단적 표현을 피하세요")
                elif criterion == "constructive_tone":
                    suggestions.append(f"{description} 개선: '해결', '개선', '협력' 등의 건설적 표현을 사용하세요")
                elif criterion == "clarity":
                    suggestions.append(f"{description} 개선: 문장을 더 짧고 명확하게 작성하세요")
                elif criterion == "length_appropriateness":
                    suggestions.append(f"{description} 개선: 160-200자 내외로 길이를 조정하세요")
        
        return suggestions
    
    def get_quality_report(self, validation_results: Dict[str, Any]) -> str:
        """품질 보고서 생성"""
        overall = validation_results.get("overall", {})
        total_score = overall.get("total_score", 0)
        grade = overall.get("grade", "D")
        quality_passed = overall.get("quality_passed", False)
        
        report = f"""
=== 관점 품질 보고서 ===
전체 점수: {total_score:.2f} ({grade})
품질 통과: {'✅ 통과' if quality_passed else '❌ 미통과'}

=== 세부 평가 ===
"""
        
        for criterion, result in validation_results.items():
            if criterion == "overall":
                continue
                
            score = result["score"]
            description = result["description"]
            details = result["details"]
            
            status = "✅" if score >= 0.7 else "⚠️" if score >= 0.5 else "❌"
            
            report += f"{status} {description}: {score:.2f} - {details}\n"
        
        # 개선 제안
        suggestions = self.generate_improvement_suggestions(validation_results)
        if suggestions:
            report += "\n=== 개선 제안 ===\n"
            for i, suggestion in enumerate(suggestions, 1):
                report += f"{i}. {suggestion}\n"
        
        return report.strip()
