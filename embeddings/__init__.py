#!/usr/bin/env python3
"""
임베딩 모듈 초기화
"""

from .config import get_config
from .run_embeddings import EmbeddingProcessor

__all__ = [
    'get_config',
    'EmbeddingProcessor'
]
