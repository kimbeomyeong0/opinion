#!/bin/bash

echo "=== 패키지 환경 정리 스크립트 ==="
echo ""

# 현재 패키지 수 확인
CURRENT_COUNT=$(python3 -m pip list | wc -l | tr -d ' ')
echo "현재 설치된 패키지 수: $CURRENT_COUNT"
echo ""

# 현재 환경 백업
echo "1. 현재 환경 백업 중..."
python3 -m pip freeze > current_environment_backup.txt
echo "   백업 완료: current_environment_backup.txt"
echo ""

# 불필요한 패키지들 목록
PACKAGES_TO_REMOVE=(
    "selenium"
    "aiohttp" 
    "openai"
    "gemini-cli"
    "feedparser"
    "keybert"
    "altgraph"
    "macholib"
    "future"
    "six"
    "jupyterlab"
    "notebook"
    "jupyter-server-terminals"
    "jupyterlab-pygments"
    "jupyterlab-server"
    "jupyterlab-widgets"
    "nbclient"
    "nbconvert"
    "nbformat"
    "notebook-shim"
    "mypy"
    "pytest-html"
    "pytest-metadata"
    "pytest-mock"
    "debugpy"
)

echo "2. 불필요한 패키지 제거 중..."
for package in "${PACKAGES_TO_REMOVE[@]}"; do
    echo "   제거 중: $package"
    python3 -m pip uninstall -y "$package" 2>/dev/null || echo "   (이미 제거됨 또는 없음)"
done
echo ""

# 정리 후 패키지 수 확인
AFTER_COUNT=$(python3 -m pip list | wc -l | tr -d ' ')
echo "정리 후 패키지 수: $AFTER_COUNT"
echo "제거된 패키지 수: $((CURRENT_COUNT - AFTER_COUNT))"
echo ""

echo "3. requirements.txt 기반으로 필요한 패키지 재설치..."
python3 -m pip install -r requirements.txt
echo ""

echo "=== 정리 완료 ==="
echo "새로운 패키지 수: $(python3 -m pip list | wc -l | tr -d ' ')"
echo ""
echo "개발 환경이 필요한 경우:"
echo "python3 -m pip install -r requirements-dev.txt"
