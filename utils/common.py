#!/usr/bin/env python3
"""
공통 유틸리티 함수들
"""

import httpx
from typing import Optional, Dict, Any
from rich.console import Console

console = Console()

async def make_request(
    url: str,
    method: str = "GET",
    params: Optional[Dict] = None,
    headers: Optional[Dict] = None,
    timeout: int = 10
) -> Optional[str]:
    """
    httpx.AsyncClient를 사용한 HTTP 요청 함수
    
    Args:
        url: 요청할 URL
        method: HTTP 메서드 (GET, POST)
        params: 쿼리 파라미터
        headers: HTTP 헤더
        timeout: 타임아웃 시간 (초)
    
    Returns:
        응답 텍스트 또는 None (실패 시)
    """
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            if method.upper() == "GET":
                response = await client.get(url, params=params, headers=headers)
            elif method.upper() == "POST":
                response = await client.post(url, params=params, headers=headers)
            else:
                raise ValueError(f"지원하지 않는 HTTP 메서드: {method}")
            
            response.raise_for_status()
            return response.text
            
    except Exception as e:
        console.print(f"❌ HTTP 요청 실패 ({url}): {str(e)}")
        return None

def log_info(message: str):
    """정보 로그 출력"""
    console.print(f"ℹ️ {message}")

def log_success(message: str):
    """성공 로그 출력"""
    console.print(f"✅ {message}")

def log_error(message: str):
    """에러 로그 출력"""
    console.print(f"❌ {message}")
