#!/usr/bin/env python3
"""
툴팁 기능이 통합된 향상된 레포트 생성기
기존 레포트에 Sonar 툴팁 시스템 통합
"""

import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utils.supabase_manager import SupabaseManager
from sonar_tooltip_system import SonarTooltipSystem
from tooltip_ui import TooltipUIController
from rich.console import Console
from datetime import datetime
from typing import Dict, List, Any

console = Console()

class EnhancedReportGenerator:
    """툴팁 기능이 통합된 레포트 생성기"""
    
    def __init__(self):
        self.supabase_manager = SupabaseManager()
        self.tooltip_system = SonarTooltipSystem()
        self.ui_controller = TooltipUIController()
    
    def generate_enhanced_html(self) -> str:
        """툴팁이 통합된 HTML 레포트 생성"""
        try:
            console.print("🚀 툴팁 통합 레포트 생성 시작", style="bold blue")
            
            # 이슈 데이터 조회
            result = self.supabase_manager.client.table('issues').select(
                'id, title, subtitle, background, left_view, center_view, right_view, created_at'
            ).order('created_at', desc=True).execute()
            
            if not result.data:
                console.print("⚠️ 이슈 데이터가 없습니다.", style="yellow")
                return ""
            
            console.print(f"✅ {len(result.data)}개 이슈 데이터 조회 완료")
            
            # 툴팁이 적용된 HTML 생성
            html_content, tooltip_definitions = self._generate_tooltip_html(result.data)
            
            # 완전한 HTML 문서 생성
            full_html = self._create_complete_html(html_content, tooltip_definitions)
            
            # 파일 저장
            timestamp = datetime.now().strftime("%m%d")
            filename = f"{timestamp}_이슈정리_툴팁.html"
            filepath = Path("report/reports") / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(full_html)
            
            console.print(f"✅ 툴팁 통합 레포트 생성 완료: {filename}")
            console.print(f"📁 저장 위치: {filepath}")
            
            return str(filepath)
            
        except Exception as e:
            console.print(f"❌ 레포트 생성 중 오류: {e}", style="red")
            return ""
    
    def _generate_tooltip_html(self, issues: List[Dict[str, Any]]) -> str:
        """이슈 데이터를 툴팁이 적용된 HTML로 변환"""
        html_parts = []
        all_tooltip_definitions = []
        
        for issue in issues:
            # 이슈 카드 HTML 생성
            issue_html = self._generate_issue_card_with_tooltips(issue)
            html_parts.append(issue_html)
            
            # 툴팁 정의 수집
            tooltip_definitions = self._collect_tooltip_definitions(issue)
            all_tooltip_definitions.extend(tooltip_definitions)
        
        # 모든 툴팁 정의를 하나로 합치기
        combined_tooltip_html = ''.join(set(all_tooltip_definitions))
        
        return ''.join(html_parts), combined_tooltip_html
    
    def _generate_issue_card_with_tooltips(self, issue: Dict[str, Any]) -> str:
        """툴팁이 적용된 이슈 카드 생성"""
        # 제목과 부제목에 툴팁 적용
        title_with_tooltips = self.tooltip_system.generate_tooltip_html(issue.get('title', ''))
        subtitle_with_tooltips = self.tooltip_system.generate_tooltip_html(issue.get('subtitle', ''))
        
        # 배경 정보에 툴팁 적용
        background_with_tooltips = self.tooltip_system.generate_tooltip_html(issue.get('background', ''))
        
        # 관점에 툴팁 적용
        left_view_html = self._generate_view_with_tooltips(issue.get('left_view', ''), 'left', issue['id'])
        right_view_html = self._generate_view_with_tooltips(issue.get('right_view', ''), 'right', issue['id'])
        center_view_html = self._generate_view_with_tooltips(issue.get('center_view', ''), 'center', issue['id'])
        
        return f"""
    <div class="issue-card">
        <div class="issue-header">
            <h2 class="issue-title">{title_with_tooltips}</h2>
            <p class="issue-subtitle">{subtitle_with_tooltips}</p>
        </div>
        
        <div class="section">
            <div class="section-label">배경 정보</div>
            <div class="section-content">{self._format_background(background_with_tooltips)}</div>
        </div>
        
        <div class="side-views-container">
            {left_view_html}
            {right_view_html}
        </div>
        
        {center_view_html}
    </div>
"""
    
    def _generate_view_with_tooltips(self, view_data: str, bias: str, issue_id: str) -> str:
        """툴팁이 적용된 관점 HTML 생성"""
        if not view_data or not view_data.strip():
            return ""
        
        # view 데이터 파싱
        title, content = self._parse_view_data(view_data)
        
        # 툴팁 적용
        title_with_tooltips = self.tooltip_system.generate_tooltip_html(title)
        content_with_tooltips = self.tooltip_system.generate_tooltip_html(content)
        
        if bias in ['left', 'right']:
            bias_name = '좌파 관점' if bias == 'left' else '우파 관점'
            return f"""
            <div class="side-view {bias}">
                <div class="view-header" onclick="toggleView('{bias}-{issue_id[:8]}')">
                    <div class="view-title {bias}">{bias_name}</div>
                    <span class="toggle-icon" id="toggle-{bias}-{issue_id[:8]}">▼</span>
                </div>
                <div class="view-summary" onclick="toggleView('{bias}-{issue_id[:8]}')">
                    {title_with_tooltips}
                </div>
                <div class="view-content collapsible" id="content-{bias}-{issue_id[:8]}" style="display: none;">
                    {self._format_content(content_with_tooltips)}
                </div>
            </div>
            """
        else:  # center
            return f"""
            <div class="collapsible-section">
                <div class="section-label collapsible-header" onclick="toggleView('center-{issue_id[:8]}')">
                    <span>중립 관점</span>
                    <span class="toggle-icon" id="toggle-center-{issue_id[:8]}">▼</span>
                </div>
                <div class="section-content">
                    <div class="view-summary" onclick="toggleView('center-{issue_id[:8]}')">
                        {title_with_tooltips}
                    </div>
                    <div class="view-content collapsible" id="content-center-{issue_id[:8]}" style="display: none;">
                        {self._format_content(content_with_tooltips)}
                    </div>
                </div>
            </div>
            """
    
    def _parse_view_data(self, view_data: str) -> tuple:
        """view 데이터를 제목과 내용으로 파싱"""
        if not view_data or not view_data.strip():
            return '', ''
        
        # "제목|||내용" 형태로 저장된 데이터 파싱
        if '|||' in view_data:
            parts = view_data.split('|||', 1)
            title = parts[0].strip()
            content = parts[1].strip() if len(parts) > 1 else ''
            return title, content
        else:
            # 기존 형태의 데이터 (전체 내용만 있는 경우)
            return '', view_data.strip()
    
    def _format_background(self, text: str) -> str:
        """Background 텍스트 포맷팅"""
        if not text or not text.strip():
            return text
        
        # "관련 기사 내용:" 라벨 제거
        formatted_text = text.replace('관련 기사 내용:', '').strip()
        
        # <br> 태그를 실제 줄바꿈으로 변환
        formatted_text = formatted_text.replace('<br>', '\n')
        
        # 각 불렛 포인트를 별도 줄로 분리
        lines = [line.strip() for line in formatted_text.split('\n') if line.strip()]
        
        # 불렛 포인트들을 HTML로 포맷팅
        bullet_html = '<div class="background-bullets">'
        for line in lines:
            if line.startswith('•'):
                # • 기호 제거
                content = line[1:].strip()
            else:
                content = line
            
            # HTML 생성
            bullet_html += '<div class="background-bullet-container">'
            bullet_html += f'<div class="background-bullet">{content}</div>'
            bullet_html += '</div>'
        
        bullet_html += '</div>'
        return bullet_html
    
    def _format_content(self, content: str) -> str:
        """내용을 HTML 형식으로 포맷팅"""
        if not content:
            return ""
        
        # 줄바꿈을 <br>로 변환
        formatted = content.replace('\n', '<br>')
        
        # 문단 구분을 위해 이중 줄바꿈을 <p> 태그로 변환
        paragraphs = formatted.split('<br><br>')
        formatted = '</p><p>'.join(paragraphs)
        
        if formatted:
            formatted = f'<p>{formatted}</p>'
        
        return formatted
    
    def _collect_tooltip_definitions(self, issue: Dict[str, Any]) -> List[str]:
        """이슈에서 사용된 모든 툴팁 정의 수집"""
        definitions = []
        
        # 제목, 부제목, 배경에서 툴팁 정의 수집
        texts = [
            issue.get('title', ''),
            issue.get('subtitle', ''),
            issue.get('background', '')
        ]
        
        # 관점에서 툴팁 정의 수집
        for view_key in ['left_view', 'center_view', 'right_view']:
            view_data = issue.get(view_key, '')
            if view_data:
                title, content = self._parse_view_data(view_data)
                texts.extend([title, content])
        
        # 각 텍스트에서 툴팁 정의 생성
        for text in texts:
            if text:
                tooltip_definitions = self.tooltip_system.generate_tooltip_definitions(text)
                if tooltip_definitions:
                    definitions.append(tooltip_definitions)
        
        return definitions
    
    def _create_complete_html(self, html_content: str, tooltip_definitions: str) -> str:
        """완전한 HTML 문서 생성"""
        # 기존 레포트의 CSS와 JavaScript를 가져와서 툴팁 기능 추가
        css = self._get_enhanced_css()
        js = self._get_enhanced_javascript()
        
        return f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>정치 이슈 정리 - 툴팁 기능 포함</title>
    <style>
        {css}
    </style>
</head>
<body>
    <div class="container">
        <header class="header">
            <h1>📰 정치 이슈 정리</h1>
            <p class="subtitle">각 용어에 마우스를 올리거나 클릭하면 간단한 설명을 볼 수 있습니다</p>
        </header>
        
        <main class="main-content">
            {html_content}
        </main>
    </div>
    
    <!-- 툴팁 정의들 -->
    {tooltip_definitions}
    
    <script>
        {js}
    </script>
</body>
</html>"""
    
    def _get_enhanced_css(self) -> str:
        """향상된 CSS (기존 + 툴팁)"""
        # 기존 CSS + 툴팁 CSS
        return """
        /* 기존 레포트 CSS */
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            color: #333;
            background-color: #f8fafc;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        
        .header {
            text-align: center;
            margin-bottom: 40px;
            padding: 30px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 15px;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            font-weight: 700;
        }
        
        .subtitle {
            font-size: 1.1em;
            opacity: 0.9;
            margin-top: 10px;
        }
        
        .issue-card {
            background: white;
            border-radius: 12px;
            padding: 25px;
            margin-bottom: 25px;
            box-shadow: 0 2px 15px rgba(0, 0, 0, 0.08);
            border: 1px solid #e2e8f0;
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        
        .issue-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 25px rgba(0, 0, 0, 0.12);
        }
        
        .issue-header {
            margin-bottom: 20px;
            padding-bottom: 15px;
            border-bottom: 2px solid #e2e8f0;
        }
        
        .issue-title {
            font-size: 1.4em;
            color: #2d3748;
            margin-bottom: 8px;
            font-weight: 600;
        }
        
        .issue-subtitle {
            font-size: 1.1em;
            color: #4a5568;
            font-weight: 400;
        }
        
        .section {
            margin-bottom: 20px;
        }
        
        .section-label {
            font-size: 1.1em;
            font-weight: 600;
            color: #2d3748;
            margin-bottom: 10px;
            padding: 8px 12px;
            background: #f7fafc;
            border-radius: 6px;
            border-left: 4px solid #4299e1;
        }
        
        .section-content {
            font-size: 15px;
            line-height: 1.6;
            color: #4a5568;
        }
        
        .side-views-container {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin: 25px 0;
        }
        
        .side-view {
            background: #f8fafc;
            border-radius: 8px;
            padding: 15px;
            border: 1px solid #e2e8f0;
        }
        
        .side-view.left {
            border-left: 4px solid #e53e3e;
        }
        
        .side-view.right {
            border-left: 4px solid #3182ce;
        }
        
        .view-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            cursor: pointer;
            margin-bottom: 8px;
        }
        
        .view-title {
            margin-bottom: 0;
            font-size: 14px;
            font-weight: 600;
            background: none;
            border: none;
            padding: 0;
            border-radius: 0;
        }
        
        .view-title.left {
            color: #e53e3e;
        }
        
        .view-title.right {
            color: #3182ce;
        }
        
        .view-summary {
            font-size: 14px;
            line-height: 1.5;
            color: #333333;
            cursor: pointer;
            margin-bottom: 8px;
            font-weight: 500;
        }
        
        .view-content {
            font-size: 14px;
            line-height: 1.5;
            color: #666666;
        }
        
        .toggle-icon {
            font-size: 12px;
            transition: transform 0.2s ease;
        }
        
        .collapsible-section {
            margin-top: 20px;
            background: #f7fafc;
            border-radius: 8px;
            border: 1px solid #e2e8f0;
        }
        
        .collapsible-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            cursor: pointer;
            padding: 12px 15px;
            background: #edf2f7;
            border-radius: 8px 8px 0 0;
        }
        
        .collapsible-section .view-summary {
            font-size: 14px;
            line-height: 1.5;
            color: #333333;
            cursor: pointer;
            font-weight: 500;
            padding: 8px 0;
        }
        
        .background-bullets {
            display: flex;
            flex-direction: column;
            gap: 12px;
        }
        
        .background-bullet-container {
            display: flex;
            align-items: flex-start;
        }
        
        .background-bullet {
            padding: 10px 14px;
            font-size: 15px;
            line-height: 1.5;
            background: #f1f5f9;
            border-radius: 8px;
            border-left: 4px solid #4299e1;
            flex: 1;
        }
        
        /* 툴팁 CSS */
        .tooltip-trigger {
            color: #2563eb;
            text-decoration: underline;
            text-decoration-style: dotted;
            cursor: pointer;
            position: relative;
            border-bottom: 1px dotted #2563eb;
            transition: all 0.2s ease;
        }
        
        .tooltip-trigger:hover {
            color: #1d4ed8;
            text-decoration-color: #1d4ed8;
            background-color: rgba(37, 99, 235, 0.1);
            border-radius: 3px;
            padding: 1px 2px;
        }
        
        .tooltip {
            position: absolute;
            z-index: 1000;
            background-color: #2d3748;
            color: #ffffff;
            border: 1px solid #4a5568;
            border-radius: 8px;
            font-size: 14px;
            max-width: 300px;
            padding: 12px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
            opacity: 0;
            visibility: hidden;
            transition: all 0.3s ease;
            transform: translateY(-10px);
            pointer-events: none;
        }
        
        .tooltip.show {
            opacity: 1;
            visibility: visible;
            transform: translateY(0);
            pointer-events: auto;
        }
        
        .tooltip-content {
            position: relative;
        }
        
        .tooltip-term {
            font-weight: bold;
            font-size: 15px;
            margin-bottom: 6px;
            color: #fbbf24;
            border-bottom: 1px solid #4a5568;
            padding-bottom: 4px;
        }
        
        .tooltip-explanation {
            line-height: 1.4;
            color: #e2e8f0;
        }
        
        /* 반응형 디자인 */
        @media (max-width: 768px) {
            .container {
                padding: 10px;
            }
            
            .header h1 {
                font-size: 2em;
            }
            
            .side-views-container {
                grid-template-columns: 1fr;
                gap: 15px;
            }
            
            .issue-card {
                padding: 20px;
            }
            
            .tooltip {
                max-width: 250px;
                font-size: 13px;
                padding: 10px;
            }
        }
        """
    
    def _get_enhanced_javascript(self) -> str:
        """향상된 JavaScript (기존 + 툴팁)"""
        return """
        // 기존 토글 기능
        function toggleView(viewId) {
            const content = document.getElementById('content-' + viewId);
            const toggleIcon = document.getElementById('toggle-' + viewId);

            if (content.style.display === 'none') {
                content.style.display = 'block';
                toggleIcon.textContent = '▲';
                toggleIcon.classList.add('rotated');
            } else {
                content.style.display = 'none';
                toggleIcon.textContent = '▼';
                toggleIcon.classList.remove('rotated');
            }
        }

        // 툴팁 시스템 JavaScript
        class TooltipManager {
            constructor() {
                this.activeTooltip = null;
                this.tooltipElements = new Map();
                this.init();
            }
            
            init() {
                document.querySelectorAll('.tooltip-trigger').forEach(trigger => {
                    const tooltipId = trigger.getAttribute('data-tooltip-id');
                    const tooltip = document.getElementById(tooltipId);
                    
                    if (tooltip) {
                        this.tooltipElements.set(tooltipId, tooltip);
                        
                        trigger.addEventListener('mouseenter', (e) => this.showTooltip(e, tooltipId));
                        trigger.addEventListener('mouseleave', () => this.hideTooltip());
                        trigger.addEventListener('click', (e) => this.toggleTooltip(e, tooltipId));
                        trigger.addEventListener('keydown', (e) => {
                            if (e.key === 'Enter' || e.key === ' ') {
                                e.preventDefault();
                                this.toggleTooltip(e, tooltipId);
                            }
                        });
                        trigger.addEventListener('focus', (e) => this.showTooltip(e, tooltipId));
                        trigger.addEventListener('blur', () => this.hideTooltip());
                    }
                });
                
                document.addEventListener('click', (e) => {
                    if (!e.target.closest('.tooltip-trigger') && !e.target.closest('.tooltip')) {
                        this.hideTooltip();
                    }
                });
                
                document.addEventListener('keydown', (e) => {
                    if (e.key === 'Escape') {
                        this.hideTooltip();
                    }
                });
                
                window.addEventListener('scroll', () => this.updateTooltipPosition());
                window.addEventListener('resize', () => this.updateTooltipPosition());
            }
            
            showTooltip(event, tooltipId) {
                this.hideTooltip();
                
                const tooltip = this.tooltipElements.get(tooltipId);
                if (!tooltip) return;
                
                this.activeTooltip = tooltipId;
                tooltip.classList.add('show');
                
                this.positionTooltip(event, tooltip);
                
                if (!tooltip.parentNode) {
                    document.body.appendChild(tooltip);
                }
            }
            
            hideTooltip() {
                if (this.activeTooltip) {
                    const tooltip = this.tooltipElements.get(this.activeTooltip);
                    if (tooltip) {
                        tooltip.classList.remove('show');
                    }
                    this.activeTooltip = null;
                }
            }
            
            toggleTooltip(event, tooltipId) {
                event.preventDefault();
                event.stopPropagation();
                
                if (this.activeTooltip === tooltipId) {
                    this.hideTooltip();
                } else {
                    this.showTooltip(event, tooltipId);
                }
            }
            
            positionTooltip(event, tooltip) {
                const rect = event.target.getBoundingClientRect();
                const tooltipRect = tooltip.getBoundingClientRect();
                const viewportWidth = window.innerWidth;
                const viewportHeight = window.innerHeight;
                
                let left = rect.left + (rect.width / 2) - (tooltipRect.width / 2);
                let top = rect.top - tooltipRect.height - 10;
                
                if (left < 10) {
                    left = 10;
                } else if (left + tooltipRect.width > viewportWidth - 10) {
                    left = viewportWidth - tooltipRect.width - 10;
                }
                
                if (top < 10) {
                    top = rect.bottom + 10;
                }
                
                tooltip.style.left = `${left}px`;
                tooltip.style.top = `${top}px`;
                tooltip.style.position = 'fixed';
            }
            
            updateTooltipPosition() {
                if (this.activeTooltip) {
                    const tooltip = this.tooltipElements.get(this.activeTooltip);
                    const trigger = document.querySelector(`[data-tooltip-id="${this.activeTooltip}"]`);
                    
                    if (tooltip && trigger) {
                        const rect = trigger.getBoundingClientRect();
                        const tooltipRect = tooltip.getBoundingClientRect();
                        
                        let left = rect.left + (rect.width / 2) - (tooltipRect.width / 2);
                        let top = rect.top - tooltipRect.height - 10;
                        
                        tooltip.style.left = `${left}px`;
                        tooltip.style.top = `${top}px`;
                    }
                }
            }
        }

        // 초기화
        document.addEventListener('DOMContentLoaded', () => {
            window.tooltipManager = new TooltipManager();
        });
        """

def main():
    """메인 실행 함수"""
    generator = EnhancedReportGenerator()
    filepath = generator.generate_enhanced_html()
    
    if filepath:
        console.print(f"🎉 툴팁 통합 레포트 생성 완료!", style="bold green")
        console.print(f"🌐 브라우저에서 확인해보세요: {filepath}")
        
        # 브라우저에서 열기
        import subprocess
        subprocess.run(['open', filepath])
    else:
        console.print("❌ 레포트 생성 실패", style="bold red")

if __name__ == "__main__":
    main()
