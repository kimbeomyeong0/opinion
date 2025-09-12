#!/usr/bin/env python3
"""
임베딩 설정 모듈
"""

def get_config():
    """임베딩 설정 반환"""
    return {
        "embedding_model": "text-embedding-3-small",
        "max_tokens": 8191,
        "batch_size": 100,
        "max_text_length": 4000
    }
