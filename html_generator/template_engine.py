#!/usr/bin/env python3
"""
HTML 템플릿 엔진
Jinja2를 사용하여 HTML 템플릿을 렌더링
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jinja2 import Environment, FileSystemLoader, select_autoescape
from rich.console import Console

from html_generator.config import get_config

console = Console()

class HTMLTemplateEngine:
    """HTML 템플릿 엔진 클래스"""
    
    def __init__(self):
        """초기화"""
        self.config = get_config()
        self.template_dir = os.path.join(os.path.dirname(__file__), self.config['template_dir'])
        
        # Jinja2 환경 설정
        self.jinja_env = Environment(
            loader=FileSystemLoader(self.template_dir),
            autoescape=select_autoescape(['html', 'xml']),
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        # 커스텀 필터 추가
        self._add_custom_filters()
    
    def _add_custom_filters(self):
        """커스텀 필터 추가"""
        
        def format_date(date_str):
            """날짜 포맷팅"""
            if not date_str:
                return 'N/A'
            try:
                from datetime import datetime
                date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                return date_obj.strftime('%Y년 %m월 %d일')
            except:
                return date_str
        
        def truncate_text(text, length=100):
            """텍스트 자르기"""
            if not text:
                return ''
            if len(text) <= length:
                return text
            return text[:length] + '...'
        
        def safe_html(text):
            """HTML 안전하게 렌더링"""
            if not text:
                return ''
            return text.replace('\n', '<br>')
        
        self.jinja_env.filters['format_date'] = format_date
        self.jinja_env.filters['truncate_text'] = truncate_text
        self.jinja_env.filters['safe_html'] = safe_html
    
    def render_issues_report(self, issues_data: list, summary_data: dict) -> str:
        """이슈 레포트 HTML 렌더링"""
        try:
            console.print("🎨 HTML 템플릿 렌더링 중...")
            
            # 템플릿 가져오기
            template = self.jinja_env.get_template('base.html')
            
            # 렌더링 데이터 준비
            render_data = {
                'title': self.config['html_title'],
                'description': self.config['html_description'],
                'author': self.config['html_author'],
                'issues': issues_data,
                'summary': summary_data,
                'theme_colors': self.config['theme_colors']
            }
            
            # HTML 렌더링
            html_content = template.render(**render_data)
            
            console.print("✅ HTML 템플릿 렌더링 완료")
            return html_content
            
        except Exception as e:
            console.print(f"❌ HTML 템플릿 렌더링 실패: {e}")
            return ""
    
    def save_html_report(self, html_content: str, output_path: str) -> bool:
        """HTML 레포트 파일 저장"""
        try:
            console.print(f"💾 HTML 레포트 저장 중: {output_path}")
            
            # 출력 디렉토리 생성
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # HTML 파일 저장
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            console.print(f"✅ HTML 레포트 저장 완료: {output_path}")
            return True
            
        except Exception as e:
            console.print(f"❌ HTML 레포트 저장 실패: {e}")
            return False
    
    def generate_report(self, issues_data: list, summary_data: dict, output_path: str) -> bool:
        """전체 레포트 생성 프로세스"""
        try:
            # HTML 렌더링
            html_content = self.render_issues_report(issues_data, summary_data)
            if not html_content:
                return False
            
            # 파일 저장
            if not self.save_html_report(html_content, output_path):
                return False
            
            return True
            
        except Exception as e:
            console.print(f"❌ 레포트 생성 실패: {e}")
            return False
