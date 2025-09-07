#!/usr/bin/env python3
"""
작업 종료 시 임시 파일 및 테스트 파일 정리 스크립트
"""

import os
import glob
from pathlib import Path
from rich.console import Console
from rich.prompt import Confirm

console = Console()

def find_temp_files():
    """정리할 수 있는 임시 파일들을 찾습니다."""
    
    # 정리 대상 패턴들
    patterns = [
        "**/test_*.py",           # test_로 시작하는 파일들
        "**/temp_*.py",           # temp_로 시작하는 파일들
        "**/tmp_*.py",            # tmp_로 시작하는 파일들
        "**/*_temp.py",           # _temp로 끝나는 파일들
        "**/*_test.py",           # _test로 끝나는 파일들
        "**/debug_*.py",          # debug_로 시작하는 파일들
        "**/check_*.py",          # check_로 시작하는 파일들
        "**/*.log",               # 로그 파일들
        "**/temp_*",              # temp_로 시작하는 디렉토리들
        "**/tmp_*",               # tmp_로 시작하는 디렉토리들
    ]
    
    temp_files = []
    
    for pattern in patterns:
        files = glob.glob(pattern, recursive=True)
        # 프로젝트 루트 기준으로 필터링
        project_root = Path.cwd()
        for file in files:
            file_path = Path(file)
            if file_path.is_file() and not file_path.name.startswith('.'):
                # .git, .cursor 등 숨김 디렉토리는 제외
                if not any(part.startswith('.') for part in file_path.parts):
                    temp_files.append(str(file_path))
    
    return temp_files

def cleanup_files(files_to_delete):
    """파일들을 삭제합니다."""
    deleted_count = 0
    
    for file_path in files_to_delete:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                console.print(f"✅ 삭제됨: {file_path}")
                deleted_count += 1
            else:
                console.print(f"⚠️  파일 없음: {file_path}")
        except Exception as e:
            console.print(f"❌ 삭제 실패: {file_path} - {str(e)}")
    
    return deleted_count

def main():
    """메인 함수"""
    console.print("[bold blue]🧹 작업 부산물 정리 스크립트[/bold blue]\n")
    
    # 임시 파일들 찾기
    temp_files = find_temp_files()
    
    if not temp_files:
        console.print("✅ 정리할 임시 파일이 없습니다.")
        return
    
    # 찾은 파일들 표시
    console.print(f"📋 발견된 임시 파일들 ({len(temp_files)}개):")
    for file_path in temp_files:
        console.print(f"  • {file_path}")
    
    console.print()
    
    # 사용자 확인
    if Confirm.ask("이 파일들을 삭제하시겠습니까?"):
        deleted_count = cleanup_files(temp_files)
        console.print(f"\n🎉 정리 완료! {deleted_count}개 파일이 삭제되었습니다.")
    else:
        console.print("❌ 정리가 취소되었습니다.")

if __name__ == "__main__":
    main()
