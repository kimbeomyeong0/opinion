#!/usr/bin/env python3
"""
View Generator 설정 파일
"""

import os
from typing import Dict, Any

def get_config() -> Dict[str, Any]:
    """View Generator 설정 반환"""
    
    return {
        # LLM 설정
        "llm_model": "gpt-4o-mini",  # 비용 효율적인 모델 사용
        "max_tokens": 1000,
        "temperature": 0.7,
        
        # 성향별 프롬프트 설정
        "bias_prompts": {
            "left": {
                "name": "진보적 관점",
                "description": "인권·사회정의 관점에서 구체적 사례와 개혁 방안을 제시",
                "tone": "인권과 사회정의 중심의 개혁적 톤",
                "keywords": ["인권", "사회정의", "개혁", "약자보호", "민주주의", "진보"],
                "role": "인권·사회정의 관점 + 구체적 사례 + 개혁 방안"
            },
            "center": {
                "name": "중도적 관점", 
                "description": "사실 정리와 쟁점 분석에 집중하는 중립적 관점",
                "tone": "객관적 사실 정리 + 쟁점 분석 톤",
                "keywords": ["사실", "정리", "쟁점", "변수", "분석", "근거"],
                "role": "사실 정리 + 쟁점 분석 + 변수 제시"
            },
            "right": {
                "name": "보수적 관점",
                "description": "법질서·안정성 관점에서 제도적 해결책을 제시", 
                "tone": "법질서와 안정성 중심의 보수적 톤", 
                "keywords": ["법질서", "안정성", "제도", "보수", "자유시장", "국가안보"],
                "role": "법질서·안정성 관점 + 제도적 해결책 + 질서 유지"
            }
        },
        
        # 명칭 일관성 가이드라인
        "naming_guidelines": {
            "government": "정부",
            "ruling_party": "여당", 
            "opposition_party": "야당",
            "former_president": "전 대통령",
            "current_president": "현 대통령",
            "assembly": "국회",
            "police": "경찰",
            "court": "법원",
            "prosecution": "검찰"
        },
        
        # 출력 설정
        "view_format": {
            "title_max_length": 50,
            "content_max_length": 300,
            "include_source_count": False
        }
    }
