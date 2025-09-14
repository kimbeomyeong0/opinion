#!/usr/bin/env python3
"""
LLM 기반 품질 검증 모듈
생성된 관점의 품질을 다각도로 검증
"""

from typing import Dict, List, Any, Optional, Tuple

class LLMBasedQualityChecker:
    """LLM 기반 품질 검증 클래스"""
    
    def __init__(self, llm_client):
        """초기화"""
        self.llm_client = llm_client
        self.MODEL_NAME = "gpt-4o-mini"
        self.TEMPERATURE = 0.3
    
    def validate_view_quality(self, title: str, content: str, bias: str, 
                            issue_characteristics: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        관점 품질 검증
        
        Args:
            title: 생성된 제목
            content: 생성된 내용
            bias: 성향
            issue_characteristics: 이슈 특성
            
        Returns:
            Tuple[bool, Dict]: (품질 통과 여부, 상세 검증 결과)
        """
        try:
            # LLM 기반 품질 검증
            quality_prompt = f"""다음 관점의 품질을 검증해주세요.

[생성된 관점]
제목: {title}
내용: {content}
성향: {bias}

[이슈 특성]
- 유형: {issue_characteristics['issue_type']}
- 이해관계자: {', '.join(issue_characteristics['stakeholders'])}
- 갈등: {', '.join(issue_characteristics['core_conflicts'])}
- 복잡도: {issue_characteristics['complexity']}

다음 기준으로 검증해주세요 (각 1-10점):

1. 성향 일관성: {bias} 성향의 특징이 잘 드러나는가?
2. 이슈 관련성: 이슈와 관련된 구체적 내용인가?
3. 객관성: 편향되지 않고 균형잡힌 관점인가?
4. 명확성: 이해하기 쉽고 명확한 표현인가?
5. 자연스러움: 딱딱하지 않고 자연스러운 문체인가?
6. 길이 적절성: 제목 20-30자, 내용 300자 내외인가?

다음 형식으로만 응답해주세요:
성향 일관성: [점수]/10
이슈 관련성: [점수]/10
객관성: [점수]/10
명확성: [점수]/10
자연스러움: [점수]/10
길이 적절성: [점수]/10
총점: [총점]/60
등급: [A/B/C/D/F]"""
            
            response = self._call_llm(quality_prompt)
            quality_scores = self._parse_quality_scores(response)
            
            # 통과 기준: 총점 36점 이상 (60점 만점의 60%)
            passed = quality_scores['total_score'] >= 36
            
            return passed, quality_scores
            
        except Exception as e:
            print(f"❌ 품질 검증 실패: {str(e)}")
            return False, self._get_default_quality_result()
    
    def _call_llm(self, prompt: str) -> str:
        """LLM 호출"""
        
        response = self.llm_client.chat.completions.create(
            model=self.MODEL_NAME,
            messages=[
                {"role": "system", "content": "당신은 콘텐츠 품질 평가 전문가입니다. 객관적이고 정확한 평가를 제공합니다."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=300,
            temperature=self.TEMPERATURE
        )
        
        return response.choices[0].message.content.strip()
    
    def _parse_quality_scores(self, response: str) -> Dict[str, Any]:
        """품질 점수 파싱"""
        
        try:
            scores = {}
            total_score = 0
            
            lines = response.split('\n')
            for line in lines:
                line = line.strip()
                if ':' in line and '/' in line:
                    key = line.split(':')[0].strip()
                    value = line.split(':')[1].strip()
                    
                    if '/' in value:
                        score = int(value.split('/')[0])
                        scores[key] = score
                        if key != '총점':
                            total_score += score
                    elif key == '등급':
                        scores['grade'] = value
            
            scores['total_score'] = total_score
            scores['overall'] = {
                'total_score': total_score,
                'grade': scores.get('grade', 'C'),
                'passed': total_score >= 36
            }
            
            return scores
            
        except Exception as e:
            print(f"❌ 점수 파싱 실패: {str(e)}")
            return self._get_default_quality_result()
    
    def _get_default_quality_result(self) -> Dict[str, Any]:
        """기본 품질 결과 반환"""
        return {
            'total_score': 30,
            'grade': 'C',
            'overall': {
                'total_score': 30,
                'grade': 'C',
                'passed': False
            }
        }
    
    def check_basic_quality(self, title: str, content: str) -> Dict[str, bool]:
        """기본 품질 체크 (LLM 없이)"""
        
        return {
            "title_length_ok": 15 <= len(title) <= 35,
            "content_length_ok": 200 <= len(content) <= 400,
            "has_title": bool(title.strip()),
            "has_content": bool(content.strip()),
            "no_emotional_words": self._check_emotional_words(title + content),
            "no_bias_keywords": self._check_bias_keywords(title + content)
        }
    
    def _check_emotional_words(self, text: str) -> bool:
        """감정적 표현 체크"""
        
        emotional_words = [
            "충격", "폭발적", "격렬", "심각", "위험", "위기",
            "대폭", "급격", "급증", "급감", "급상승", "급하락"
        ]
        
        return not any(word in text for word in emotional_words)
    
    def _check_bias_keywords(self, text: str) -> bool:
        """편향성 키워드 체크"""
        
        bias_keywords = [
            "반드시", "당연히", "틀림없이", "확실히", "무조건",
            "절대", "완전히", "모든", "항상"
        ]
        
        return not any(word in text for word in bias_keywords)
