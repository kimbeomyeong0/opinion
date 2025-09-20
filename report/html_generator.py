#!/usr/bin/env python3
"""
HTML 생성 전용 클래스
보고서의 HTML 구조와 스타일링을 담당
"""

from typing import Dict, Any, List
from pathlib import Path


class HTMLGenerator:
    """HTML 생성 전용 클래스"""
    
    def __init__(self):
        """초기화"""
        self.styles_path = Path(__file__).parent / "styles.css"
    
    def load_css(self) -> str:
        """CSS 파일 로드"""
        try:
            if self.styles_path.exists():
                with open(self.styles_path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                return self._get_fallback_css()
        except Exception:
            return self._get_fallback_css()
    
    def _get_fallback_css(self) -> str:
        """CSS 파일 로드 실패 시 기본 스타일"""
        return "/* CSS 파일을 찾을 수 없습니다 */"
    
    def generate_gauge_bar(self, stats: Dict[str, int]) -> str:
        """게이지바 HTML 생성"""
        total = stats.get('total', 0)
        if total == 0:
            return '<div class="gauge-bar"><div class="gauge-fill"></div></div>'
        
        left_pct = (stats.get('left', 0) / total) * 100
        center_pct = (stats.get('center', 0) / total) * 100
        right_pct = (stats.get('right', 0) / total) * 100
        
        gauge_html = '<div class="gauge-bar"><div class="gauge-fill">'
        
        if left_pct > 0:
            gauge_html += f'<div class="gauge-left" style="width: {left_pct}%">'
            if left_pct > 15:
                gauge_html += f'<div class="gauge-percentage">{left_pct:.0f}%</div>'
            gauge_html += '</div>'
        
        if center_pct > 0:
            gauge_html += f'<div class="gauge-center" style="width: {center_pct}%">'
            if center_pct > 15:
                gauge_html += f'<div class="gauge-percentage">{center_pct:.0f}%</div>'
            gauge_html += '</div>'
        
        if right_pct > 0:
            gauge_html += f'<div class="gauge-right" style="width: {right_pct}%">'
            if right_pct > 15:
                gauge_html += f'<div class="gauge-percentage">{right_pct:.0f}%</div>'
            gauge_html += '</div>'
        
        gauge_html += '</div></div>'
        return gauge_html
    
    
    def format_date(self, date_str: str) -> str:
        """날짜 포맷팅"""
        if not date_str:
            return ""
        
        created_date = date_str[:10]
        try:
            from datetime import datetime
            date_obj = datetime.strptime(created_date, '%Y-%m-%d')
            return date_obj.strftime('%Y년 %m월 %d일')
        except:
            return created_date
    
    def clean_text(self, text: str) -> str:
        """텍스트에서 따옴표 제거"""
        if not text:
            return ""
        return str(text).strip('"').strip("'")
    
    def generate_issue_card(self, issue: Dict[str, Any], stats: Dict[str, int]) -> str:
        """단일 이슈 카드 HTML 생성"""
        gauge_bar = self.generate_gauge_bar(stats)
        formatted_date = self.format_date(issue.get('created_at', ''))
        clean_title = self.clean_text(issue.get('title', ''))
        clean_summary = self.clean_text(issue.get('issue_summary', ''))
        
        # 성향별 관점 처리
        left_perspective = self.clean_text(issue.get('left_perspective', ''))
        right_perspective = self.clean_text(issue.get('right_perspective', ''))
        
        # 관점 섹션 HTML 생성
        perspectives_html = ""
        if left_perspective or right_perspective:
            perspectives_html = '<div class="perspectives-container">'
            perspectives_html += '<div class="perspectives-title">성향별 관점</div>'
            
            if left_perspective:
                perspectives_html += f'''
                <div class="perspective-item">
                    <div class="perspective-label left">진보 관점</div>
                    <div class="perspective-content">{left_perspective}</div>
                </div>'''
            
            if right_perspective:
                perspectives_html += f'''
                <div class="perspective-item">
                    <div class="perspective-label right">보수 관점</div>
                    <div class="perspective-content">{right_perspective}</div>
                </div>'''
            
            perspectives_html += '</div>'
        
        return f"""
    <div class="issue-card">
        <div class="issue-header">
            <div class="meta-info">{stats['total']}개 기사 ∙ {formatted_date}</div>
            <div class="title">{clean_title}</div>
            <div class="subtitle">
                <div class="summary-content">{clean_summary}</div>
            </div>
        </div>
        
        <div class="gauge-container">
            <div class="gauge-title">언론사 성향별 보도 비율</div>
            {gauge_bar}
        </div>
        
        {perspectives_html}
    </div>"""
    
    def generate_full_html(self, issues: List[Dict[str, Any]], all_stats: List[Dict[str, int]]) -> str:
        """전체 HTML 문서 생성"""
        css_content = self.load_css()
        
        # 이슈 카드들 생성
        issue_cards = []
        for issue, stats in zip(issues, all_stats):
            card_html = self.generate_issue_card(issue, stats)
            issue_cards.append(card_html)
        
        return f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>정치 이슈 보고서</title>
    <style>
        {css_content}
    </style>
</head>
<body>
    <div class="header">
        <h1>정치 이슈 보고서</h1>
        <div class="subtitle">언론사 성향별 분석</div>
    </div>
    
    {''.join(issue_cards)}
</body>
</html>"""
