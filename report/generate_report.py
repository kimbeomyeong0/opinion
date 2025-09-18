#!/usr/bin/env python3
"""
정치 이슈 HTML 보고서 생성기 (리팩토링 버전)
Substack 스타일의 미니멀 디자인으로 모바일 최적화된 보고서 생성
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(str(Path(__file__).parent.parent))

from rich.console import Console
from report.data_processor import DataProcessor
from report.html_generator import HTMLGenerator

console = Console()

class ReportGenerator:
    """HTML 보고서 생성기 (리팩토링 버전)"""
    
    def __init__(self):
        """초기화"""
        self.reports_dir = Path(__file__).parent / "reports"
        self.reports_dir.mkdir(exist_ok=True)
        
        # 의존성 주입
        self.data_processor = DataProcessor()
        self.html_generator = HTMLGenerator()
        
    def generate_filename(self, date: datetime = None) -> str:
        """날짜 기반 파일명 생성"""
        if date is None:
            date = datetime.now()
        
        base_name = f"{date.strftime('%m%d')}_이슈정리"
        counter = 1
        
        while True:
            if counter == 1:
                filename = f"{base_name}.html"
            else:
                filename = f"{base_name}({counter}).html"
            
            if not (self.reports_dir / filename).exists():
                return filename
            counter += 1
    
    
    
    

    
    

    

    
    def save_report(self, html: str, filename: str = None) -> Optional[str]:
        """HTML 파일 저장"""
        if filename is None:
            filename = self.generate_filename()
        
        file_path = self.reports_dir / filename
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(html)
            return str(file_path)
        except Exception as e:
            console.print(f"❌ 파일 저장 실패: {str(e)}")
            return None
    
    def generate_html(self) -> Optional[str]:
        """전체 HTML 생성 (리팩토링 버전)"""
        try:
            # 데이터 처리
            issues, all_stats = self.data_processor.process_all_data()
            if not issues:
                return None
            
            # HTML 생성
            html = self.html_generator.generate_full_html(issues, all_stats)
            return html
            
        except Exception as e:
            console.print(f"❌ HTML 생성 실패: {str(e)}")
            return None
    
    def generate_report(self) -> bool:
        """보고서 생성 메인 함수"""
        try:
            console.print("🚀 정치 이슈 HTML 보고서 생성기 시작")
            
            # HTML 생성
            html = self.generate_html()
            if not html:
                return False
            
            # 파일 저장
            file_path = self.save_report(html)
            if not file_path:
                return False
            
            console.print(f"✅ 보고서 생성 완료: {Path(file_path).name}")
            console.print(f"📁 저장 위치: {file_path}")
            console.print("🎉 보고서 생성 완료!")
            console.print(f"📱 모바일에서 확인해보세요: {file_path}")
            
            return True
            
        except Exception as e:
            console.print(f"❌ 보고서 생성 실패: {str(e)}")
            return False


def main():
    """메인 함수"""
    try:
        generator = ReportGenerator()
        success = generator.generate_report()
        
        if not success:
            sys.exit(1)
            
    except KeyboardInterrupt:
        console.print("\n👋 사용자에 의해 중단되었습니다.")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n❌ 오류 발생: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()