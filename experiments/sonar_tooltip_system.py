#!/usr/bin/env python3
"""
Sonar API를 활용한 정치 용어/한자 툴팁 시스템
나무위키 스타일의 클릭 가능한 용어 설명 기능
"""

import os
import re
import json
import requests
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from rich.console import Console

console = Console()

@dataclass
class TermInfo:
    """용어 정보 클래스"""
    term: str
    explanation: str
    category: str  # 'political', 'hanja', 'general'
    confidence: float

class SonarTooltipSystem:
    """Sonar API 기반 툴팁 시스템"""
    
    def __init__(self):
        self.api_key = os.getenv('SONAR_API_KEY')
        self.base_url = "https://api.sonar.com/v1"
        self.term_cache = {}  # 용어 캐시
        self.political_terms = self._load_political_terms()
        self.hanja_patterns = self._load_hanja_patterns()
        
    def _load_political_terms(self) -> Dict[str, str]:
        """정치 용어 사전 로드"""
        return {
            # 정부/정치 기관
            "국회": "국민의 대표기관",
            "대통령": "국가 원수",
            "국무총리": "정부 수반",
            "국무회의": "정부 최고 정책심의기구",
            "국정감사": "국회의 감시기능",
            "대정부질문": "국회가 정부에 질문하는 제도",
            "정기국회": "매년 정기적으로 열리는 국회",
            "임시국회": "특별한 안건으로 소집되는 국회",
            
            # 정당/선거
            "여당": "집권 정당",
            "야당": "비집권 정당",
            "여소야대": "여당이 소수인 국회",
            "여대야소": "여당이 다수인 국회",
            "연정": "여러 정당이 연합한 정부",
            "총선": "국회의원 선거",
            "대선": "대통령 선거",
            "지방선거": "지방자치단체 선거",
            
            # 사법/검찰
            "검찰": "수사기관",
            "법원": "사법기관",
            "대법원": "최고 사법기관",
            "헌법재판소": "헌법 위반 심판기관",
            "사법개혁": "사법제도 개선",
            "검찰개혁": "검찰제도 개선",
            "독립수사": "검찰 독립성 강화",
            
            # 외교/안보
            "외교부": "외교 업무 담당 부처",
            "국방부": "국방 업무 담당 부처",
            "통일부": "남북통일 업무 담당 부처",
            "한미동맹": "한국-미국 동맹관계",
            "북핵": "북한의 핵무기",
            "사드": "미사일 방어체계",
            
            # 경제
            "기획재정부": "경제정책 기획 부처",
            "한은": "한국은행",
            "금리": "이자율",
            "인플레이션": "물가상승",
            "GDP": "국내총생산",
            "세금": "국가 재정 수입",
            
            # 사회
            "복지": "국민 생활 안정",
            "교육부": "교육 업무 담당 부처",
            "보건복지부": "보건복지 업무 담당 부처",
            "환경부": "환경 보호 업무 담당 부처",
        }
    
    def _load_hanja_patterns(self) -> Dict[str, str]:
        """한자 패턴 로드"""
        return {
            # 정치 관련 한자
            "정치": "政治 - 나라를 다스리는 일",
            "정부": "政府 - 국가 행정기관",
            "국가": "國家 - 주권을 가진 정치체",
            "국민": "國民 - 국가 구성원",
            "민주": "民主 - 국민이 주인인 정치",
            "공화": "共和 - 여러 계급이 협력하는 정치",
            "헌법": "憲法 - 국가 기본법",
            "법률": "法律 - 국가가 제정한 규칙",
            "정책": "政策 - 정부의 계획과 방침",
            "제도": "制度 - 사회적 규범과 체계",
            "개혁": "改革 - 기존 제도를 바꿈",
            "혁신": "革新 - 새로운 방법으로 개선",
            "발전": "發展 - 나아가고 성장함",
            "성장": "成長 - 크게 자라남",
            "안정": "安定 - 흔들리지 않고 견고함",
            "평화": "平和 - 전쟁이 없는 상태",
            "통일": "統一 - 나뉜 것을 하나로 합침",
            "독립": "獨立 - 다른 것에 의존하지 않음",
            "자유": "自由 - 구속받지 않는 상태",
            "평등": "平等 - 차별이 없는 상태",
            "정의": "正義 - 바르고 옳은 도리",
            "윤리": "倫理 - 도덕적 규범",
            "도덕": "道德 - 선악을 구분하는 기준",
            "책임": "責任 - 맡은 바 의무",
            "의무": "義務 - 반드시 해야 할 일",
            "권리": "權利 - 누릴 수 있는 자격",
            "의견": "意見 - 개인의 생각",
            "논의": "論議 - 의견을 나누어 토론",
            "토론": "討論 - 의견을 주고받으며 논의",
            "협의": "協議 - 서로 의논하여 결정",
            "합의": "合意 - 서로 같은 의견",
            "대화": "對話 - 서로 이야기함",
            "소통": "疏通 - 서로 이해하고 통함",
            "협력": "協力 - 함께 힘을 합침",
            "연합": "聯合 - 여러 세력이 결합",
            "동맹": "同盟 - 서로 도움을 약속",
            "협정": "協定 - 국제적 약속",
            "조약": "條約 - 국가간 공식 약속",
            "외교": "外交 - 국가간 관계",
            "안보": "安保 - 국가 안전 보장",
            "국방": "國防 - 국가를 지킴",
            "군사": "軍事 - 군대와 관련된 일",
            "전쟁": "戰爭 - 무력 충돌",
            "평화": "平和 - 전쟁이 없는 상태",
            "분쟁": "紛爭 - 의견 차이로 인한 갈등",
            "갈등": "葛藤 - 서로 다른 이해관계",
            "대립": "對立 - 서로 반대 입장",
            "경쟁": "競爭 - 서로 앞서려고 함",
            "협상": "協商 - 서로 타협점 찾기",
            "타협": "妥協 - 서로 양보하여 합의",
            "양보": "讓步 - 자신의 주장을 굽힘",
            "절충": "折衷 - 양쪽 의견을 절반씩 수용",
            "조정": "調整 - 갈등을 해결하려 함",
            "중재": "仲裁 - 제3자가 갈등 해결",
            "조율": "調律 - 여러 의견을 맞춤",
        }
    
    def detect_terms_in_text(self, text: str) -> List[Tuple[str, int, int, str]]:
        """텍스트에서 용어 감지"""
        detected_terms = []
        
        # 정치 용어 감지
        for term, explanation in self.political_terms.items():
            if term in text:
                start_pos = text.find(term)
                while start_pos != -1:
                    detected_terms.append((term, start_pos, start_pos + len(term), explanation))
                    start_pos = text.find(term, start_pos + 1)
        
        # 한자 패턴 감지
        for hanja_term, explanation in self.hanja_patterns.items():
            if hanja_term in text:
                start_pos = text.find(hanja_term)
                while start_pos != -1:
                    detected_terms.append((hanja_term, start_pos, start_pos + len(hanja_term), explanation))
                    start_pos = text.find(hanja_term, start_pos + 1)
        
        # 중복 제거 및 정렬
        unique_terms = {}
        for term, start, end, explanation in detected_terms:
            key = (start, end)
            if key not in unique_terms:
                unique_terms[key] = (term, start, end, explanation)
        
        return sorted(unique_terms.values(), key=lambda x: x[1])
    
    def get_term_explanation(self, term: str) -> Optional[str]:
        """용어 설명 조회"""
        # 캐시 확인
        if term in self.term_cache:
            return self.term_cache[term]
        
        # 정치 용어 사전 확인
        if term in self.political_terms:
            explanation = self.political_terms[term]
            self.term_cache[term] = explanation
            return explanation
        
        # 한자 패턴 확인
        if term in self.hanja_patterns:
            explanation = self.hanja_patterns[term]
            self.term_cache[term] = explanation
            return explanation
        
        # Sonar API 호출 (향후 확장)
        # explanation = self._call_sonar_api(term)
        
        return None
    
    def _call_sonar_api(self, term: str) -> Optional[str]:
        """Sonar API 호출 (향후 구현)"""
        # TODO: Sonar API 연동 구현
        return None
    
    def generate_tooltip_html(self, text: str) -> str:
        """텍스트를 툴팁이 포함된 HTML로 변환"""
        detected_terms = self.detect_terms_in_text(text)
        
        if not detected_terms:
            return text
        
        # 텍스트를 HTML로 변환하면서 툴팁 추가
        html_parts = []
        last_pos = 0
        
        for term, start, end, explanation in detected_terms:
            # 툴팁이 없는 부분 추가
            if start > last_pos:
                html_parts.append(text[last_pos:start])
            
            # 툴팁이 있는 용어 추가
            tooltip_id = f"tooltip_{hash(term) % 10000}"
            html_parts.append(f'<span class="tooltip-trigger" data-tooltip-id="{tooltip_id}">{term}</span>')
            
            last_pos = end
        
        # 마지막 부분 추가
        if last_pos < len(text):
            html_parts.append(text[last_pos:])
        
        return ''.join(html_parts)
    
    def generate_tooltip_definitions(self, text: str) -> str:
        """툴팁 정의 HTML 생성"""
        detected_terms = self.detect_terms_in_text(text)
        
        if not detected_terms:
            return ""
        
        definitions_html = []
        for term, start, end, explanation in detected_terms:
            tooltip_id = f"tooltip_{hash(term) % 10000}"
            definitions_html.append(f'''
                <div class="tooltip" id="{tooltip_id}">
                    <div class="tooltip-content">
                        <div class="tooltip-term">{term}</div>
                        <div class="tooltip-explanation">{explanation}</div>
                    </div>
                </div>
            ''')
        
        return ''.join(definitions_html)

def test_tooltip_system():
    """툴팁 시스템 테스트"""
    console.print("🧪 Sonar 툴팁 시스템 테스트 시작", style="bold blue")
    
    system = SonarTooltipSystem()
    
    # 테스트 텍스트
    test_text = """
    이재명 대통령은 국회에서 대정부질문에 답변하며 사법개혁과 검찰개혁의 필요성을 강조했습니다. 
    여당인 더불어민주당과 야당인 국민의힘 사이에 정치적 갈등이 있었지만, 
    국민의 복지와 국가 발전을 위한 정책 협의가 필요하다고 밝혔습니다.
    """
    
    console.print(f"📝 테스트 텍스트:", style="bold")
    console.print(test_text.strip())
    
    # 용어 감지
    detected_terms = system.detect_terms_in_text(test_text)
    console.print(f"\n🔍 감지된 용어: {len(detected_terms)}개", style="bold")
    
    for term, start, end, explanation in detected_terms:
        console.print(f"  • {term}: {explanation}")
    
    # HTML 생성 테스트
    html_with_tooltips = system.generate_tooltip_html(test_text)
    console.print(f"\n🌐 생성된 HTML:", style="bold")
    console.print(html_with_tooltips)
    
    # 툴팁 정의 생성
    tooltip_definitions = system.generate_tooltip_definitions(test_text)
    console.print(f"\n📋 툴팁 정의 HTML:", style="bold")
    console.print(tooltip_definitions[:500] + "..." if len(tooltip_definitions) > 500 else tooltip_definitions)

if __name__ == "__main__":
    test_tooltip_system()
