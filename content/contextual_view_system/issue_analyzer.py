#!/usr/bin/env python3
"""
이슈 특성 분석 모듈
이슈의 본질적 특성을 파악하여 맥락 기반 관점 생성을 위한 기초 데이터 제공
"""

import re
from typing import Dict, List, Any, Optional
from datetime import datetime

class IssueAnalyzer:
    """이슈 특성 분석 클래스"""
    
    def __init__(self):
        """초기화"""
        # 이슈 유형별 키워드 매핑 (우선순위 순)
        self.issue_type_keywords = {
            "법률": ["법률", "법", "입법", "사법", "재판", "검찰", "경찰", "형사", "민사", "헌법", "인권", "자유", "구속", "영장", "수사", "특검", "의혹", "비리", "부패", "사건", "피의자", "혐의"],
            "정치": ["정치", "선거", "정당", "국회", "정부", "대통령", "총리", "장관", "의원", "개헌", "정치개혁", "공천", "여야", "여당", "야당"],
            "경제": ["경제", "경기", "성장", "투자", "고용", "임금", "물가", "금리", "세금", "예산", "부동산", "주식", "시장", "자본", "금융", "기업", "경영"],
            "사회": ["사회", "복지", "보험", "의료", "교육", "노인", "아동", "장애인", "다문화", "이민", "주거", "교통", "교사", "학생", "학교"],
            "환경": ["환경", "기후", "탄소", "에너지", "재생", "오염", "대기", "수질", "생태", "녹색", "친환경", "지구온난화"],
            "안보": ["안보", "국방", "군사", "북한", "미사일", "핵", "군사동맹", "방위", "보안", "테러", "사이버", "정보보호"],
            "기술": ["기술", "AI", "인공지능", "디지털", "4차산업", "빅데이터", "클라우드", "블록체인", "메타버스", "규제"],
            "외교": ["외교", "국제", "외국", "협정", "FTA", "통상", "수출", "수입", "글로벌", "국제기구", "UN", "WTO"]
        }
        
        # 이해관계자 키워드 매핑
        self.stakeholder_keywords = {
            "정부": ["정부", "국가", "공공", "공무원", "관료", "정책", "예산", "세금"],
            "기업": ["기업", "회사", "사업자", "경영", "투자", "시장", "경쟁", "수익", "고용"],
            "시민": ["시민", "국민", "주민", "사람", "개인", "가족", "소비자", "이용자"],
            "국제기구": ["UN", "WTO", "OECD", "IMF", "세계은행", "국제기구", "글로벌", "국제사회"],
            "지역": ["지역", "지방", "시도", "광역", "기초", "자치", "주민", "지역경제"],
            "전문가": ["전문가", "학자", "연구자", "교수", "박사", "연구소", "학회", "기관"]
        }
        
        # 핵심 가치 갈등 키워드
        self.value_conflict_keywords = {
            "자유 vs 평등": ["자유", "평등", "기회", "공정", "차별", "권리", "의무"],
            "효율 vs 공정": ["효율", "공정", "성과", "공평", "경쟁", "협력", "분배"],
            "혁신 vs 안전": ["혁신", "안전", "변화", "보수", "진보", "전통", "새로움"],
            "개인 vs 집단": ["개인", "집단", "공동체", "사회", "이익", "공익", "사적", "공적"],
            "시장 vs 정부": ["시장", "정부", "자율", "규제", "개입", "통제", "자유화", "공영"]
        }
    
    def analyze_issue_characteristics(self, issue_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        이슈의 본질적 특성을 분석
        
        Args:
            issue_data: 이슈 데이터 (title, subtitle, content 등)
            
        Returns:
            Dict: 이슈 특성 분석 결과
        """
        try:
            # 기본 정보 추출
            title = issue_data.get('title', '')
            subtitle = issue_data.get('subtitle', '')
            content = issue_data.get('content', '')
            
            # 전체 텍스트 결합
            full_text = f"{title} {subtitle} {content}".lower()
            
            # 이슈 유형 분석
            issue_type = self._analyze_issue_type(full_text)
            
            # 이해관계자 분석
            stakeholders = self._analyze_stakeholders(full_text)
            
            # 핵심 가치 갈등 분석
            value_conflicts = self._analyze_value_conflicts(full_text)
            
            # 복잡도 분석
            complexity_level = self._analyze_complexity(full_text, issue_type)
            
            # 시간적 맥락 분석
            temporal_context = self._analyze_temporal_context(full_text)
            
            # 긴급성 분석
            urgency_level = self._analyze_urgency(full_text)
            
            return {
                "issue_type": issue_type,
                "stakeholders": stakeholders,
                "core_values_in_conflict": value_conflicts,
                "complexity_level": complexity_level,
                "temporal_context": temporal_context,
                "urgency_level": urgency_level,
                "analysis_confidence": self._calculate_confidence(issue_type, stakeholders, value_conflicts)
            }
            
        except Exception as e:
            print(f"❌ 이슈 특성 분석 실패: {str(e)}")
            return self._get_default_characteristics()
    
    def _analyze_issue_type(self, text: str) -> str:
        """이슈 유형 분석 (개선된 로직)"""
        type_scores = {}
        
        # 각 이슈 유형별로 가중치 적용
        for issue_type, keywords in self.issue_type_keywords.items():
            score = 0
            for keyword in keywords:
                if keyword in text:
                    # 핵심 키워드는 더 높은 가중치
                    if keyword in ["구속", "영장", "수사", "특검", "의혹", "비리", "부패", "사건"]:
                        score += 3
                    elif keyword in ["검찰", "경찰", "재판", "법률", "법"]:
                        score += 2
                    else:
                        score += 1
            type_scores[issue_type] = score
        
        # 가장 높은 점수의 이슈 유형 반환
        if type_scores and max(type_scores.values()) > 0:
            return max(type_scores, key=type_scores.get)
        return "정치"  # 기본값
    
    def _analyze_stakeholders(self, text: str) -> List[str]:
        """이해관계자 분석"""
        stakeholder_scores = {}
        
        for stakeholder, keywords in self.stakeholder_keywords.items():
            score = sum(1 for keyword in keywords if keyword in text)
            if score > 0:
                stakeholder_scores[stakeholder] = score
        
        # 점수가 높은 상위 3개 이해관계자 반환
        sorted_stakeholders = sorted(stakeholder_scores.items(), key=lambda x: x[1], reverse=True)
        return [stakeholder for stakeholder, score in sorted_stakeholders[:3]]
    
    def _analyze_value_conflicts(self, text: str) -> List[str]:
        """핵심 가치 갈등 분석"""
        conflict_scores = {}
        
        for conflict, keywords in self.value_conflict_keywords.items():
            score = sum(1 for keyword in keywords if keyword in text)
            if score > 0:
                conflict_scores[conflict] = score
        
        # 점수가 높은 상위 2개 갈등 반환
        sorted_conflicts = sorted(conflict_scores.items(), key=lambda x: x[1], reverse=True)
        return [conflict for conflict, score in sorted_conflicts[:2]]
    
    def _analyze_complexity(self, text: str, issue_type: str) -> str:
        """복잡도 분석"""
        complexity_indicators = {
            "단순": ["단순", "명확", "간단", "직접", "단일"],
            "중간": ["복합", "다양", "여러", "다각", "종합"],
            "복합": ["복잡", "다층", "상호", "연관", "시스템", "구조적", "체계적"]
        }
        
        # 복잡도 지표 키워드 개수 계산
        simple_count = sum(1 for keyword in complexity_indicators["단순"] if keyword in text)
        medium_count = sum(1 for keyword in complexity_indicators["중간"] if keyword in text)
        complex_count = sum(1 for keyword in complexity_indicators["복합"] if keyword in text)
        
        # 문장 길이와 단어 수 고려
        sentence_count = len(re.split(r'[.!?]', text))
        word_count = len(text.split())
        
        if word_count > 200 or complex_count > medium_count + simple_count:
            return "복합"
        elif word_count > 100 or medium_count > simple_count:
            return "중간"
        else:
            return "단순"
    
    def _analyze_temporal_context(self, text: str) -> str:
        """시간적 맥락 분석"""
        urgent_keywords = ["긴급", "즉시", "당장", "시급", "응급", "위기", "위험"]
        long_term_keywords = ["장기", "지속", "미래", "계획", "전략", "비전", "발전"]
        transitional_keywords = ["과도", "전환", "변화", "개혁", "혁신", "새로운"]
        
        urgent_count = sum(1 for keyword in urgent_keywords if keyword in text)
        long_term_count = sum(1 for keyword in long_term_keywords if keyword in text)
        transitional_count = sum(1 for keyword in transitional_keywords if keyword in text)
        
        if urgent_count > long_term_count and urgent_count > transitional_count:
            return "긴급성"
        elif long_term_count > urgent_count and long_term_count > transitional_count:
            return "장기성"
        else:
            return "과도기"
    
    def _analyze_urgency(self, text: str) -> str:
        """긴급성 분석"""
        high_urgency_keywords = ["긴급", "즉시", "당장", "시급", "위기", "위험", "심각"]
        medium_urgency_keywords = ["중요", "필요", "요구", "제안", "권고"]
        low_urgency_keywords = ["검토", "논의", "연구", "분석", "고려"]
        
        high_count = sum(1 for keyword in high_urgency_keywords if keyword in text)
        medium_count = sum(1 for keyword in medium_urgency_keywords if keyword in text)
        low_count = sum(1 for keyword in low_urgency_keywords if keyword in text)
        
        if high_count > medium_count and high_count > low_count:
            return "높음"
        elif medium_count > high_count and medium_count > low_count:
            return "중간"
        else:
            return "낮음"
    
    def _calculate_confidence(self, issue_type: str, stakeholders: List[str], value_conflicts: List[str]) -> float:
        """분석 신뢰도 계산"""
        confidence = 0.0
        
        # 이슈 유형이 명확할수록 신뢰도 증가
        if issue_type != "정치":  # 기본값이 아닌 경우
            confidence += 0.3
        
        # 이해관계자가 많을수록 신뢰도 증가
        confidence += min(len(stakeholders) * 0.1, 0.3)
        
        # 가치 갈등이 명확할수록 신뢰도 증가
        confidence += min(len(value_conflicts) * 0.1, 0.2)
        
        # 기본 신뢰도
        confidence += 0.2
        
        return min(confidence, 1.0)
    
    def _get_default_characteristics(self) -> Dict[str, Any]:
        """기본 이슈 특성 반환"""
        return {
            "issue_type": "정치",
            "stakeholders": ["정부", "시민"],
            "core_values_in_conflict": ["자유 vs 평등"],
            "complexity_level": "중간",
            "temporal_context": "과도기",
            "urgency_level": "중간",
            "analysis_confidence": 0.5
        }
    
    def get_issue_specific_guidance(self, issue_characteristics: Dict[str, Any]) -> Dict[str, Any]:
        """
        이슈 특성에 따른 맞춤형 가이드라인 생성
        
        Args:
            issue_characteristics: 이슈 특성 분석 결과
            
        Returns:
            Dict: 이슈별 맞춤형 가이드라인
        """
        issue_type = issue_characteristics.get("issue_type", "정치")
        complexity = issue_characteristics.get("complexity_level", "중간")
        urgency = issue_characteristics.get("urgency_level", "중간")
        
        guidance = {
            "focus_areas": self._get_focus_areas(issue_type),
            "considerations": self._get_considerations(issue_type, complexity, urgency),
            "language_style": self._get_language_style(issue_type, urgency),
            "perspective_approach": self._get_perspective_approach(issue_type, complexity)
        }
        
        return guidance
    
    def _get_focus_areas(self, issue_type: str) -> List[str]:
        """이슈 유형별 집중 영역"""
        focus_areas = {
            "경제": ["성장과 분배의 균형", "시장 효율성", "고용 안정성", "소득 불평등"],
            "환경": ["지속가능성", "미래 세대", "생태계 보호", "기후 변화 대응"],
            "안보": ["국가 안전", "국민 보호", "국제 협력", "위협 대응"],
            "사회": ["사회 통합", "복지 확대", "공정성", "포용성"],
            "기술": ["혁신과 안전", "디지털 격차", "윤리적 사용", "경쟁력"],
            "법률": ["공정성", "인권 보호", "법치주의", "사회 정의"],
            "외교": ["국가 이익", "국제 협력", "상호 존중", "평화 유지"]
        }
        return focus_areas.get(issue_type, ["공정성", "효율성", "민주성"])
    
    def _get_considerations(self, issue_type: str, complexity: str, urgency: str) -> List[str]:
        """고려사항"""
        considerations = []
        
        # 이슈 유형별 고려사항
        if issue_type == "경제":
            considerations.extend(["경제적 파급효과", "국제 경쟁력", "시장 신뢰도"])
        elif issue_type == "환경":
            considerations.extend(["장기적 영향", "국제적 책임", "기술적 실현가능성"])
        elif issue_type == "안보":
            considerations.extend(["국가 안전", "국제법 준수", "동맹국 관계"])
        
        # 복잡도별 고려사항
        if complexity == "복합":
            considerations.extend(["다각적 접근", "이해관계자 조율", "단계적 실행"])
        
        # 긴급성별 고려사항
        if urgency == "높음":
            considerations.extend(["신속한 대응", "위험 관리", "효과적 해결"])
        
        return considerations
    
    def _get_language_style(self, issue_type: str, urgency: str) -> str:
        """언어 스타일"""
        if urgency == "높음":
            return "명확하고 직접적"
        elif issue_type in ["기술", "법률"]:
            return "정확하고 전문적"
        else:
            return "이해하기 쉽고 친근한"
    
    def _get_perspective_approach(self, issue_type: str, complexity: str) -> str:
        """관점 접근 방식"""
        if complexity == "복합":
            return "다층적이고 종합적"
        elif issue_type in ["경제", "기술"]:
            return "데이터 기반이고 실용적"
        else:
            return "가치 중심이고 원칙적"
