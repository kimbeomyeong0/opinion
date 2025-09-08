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
                "description": "사회적 약자와 소수자 권리, 평등, 사회정의를 중시하는 관점",
                "tone": "진보적이고 사회정의를 중시하는 톤",
                "keywords": ["사회정의", "평등", "인권", "민주주의", "진보", "개혁"]
            },
            "center": {
                "name": "중도적 관점", 
                "description": "균형잡힌 시각으로 객관적이고 중립적인 분석을 제공하는 관점",
                "tone": "객관적이고 균형잡힌 톤",
                "keywords": ["객관적", "균형", "중립", "분석", "사실", "근거"]
            },
            "right": {
                "name": "보수적 관점",
                "description": "전통적 가치와 질서, 안정성을 중시하는 관점",
                "tone": "보수적이고 안정성을 중시하는 톤", 
                "keywords": ["전통", "질서", "안정", "보수", "자유시장", "국가안보"]
            }
        },
        
        # 출력 설정
        "view_format": {
            "title_max_length": 50,
            "content_max_length": 300,
            "include_source_count": False
        }
    }
