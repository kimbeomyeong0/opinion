#!/usr/bin/env python3
"""
정치 이슈 HTML 보고서 생성기
Substack 스타일의 미니멀 디자인으로 모바일 최적화된 보고서 생성
"""

import os
import sys
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(str(Path(__file__).parent.parent))

from utils.supabase_manager import SupabaseManager
from rich.console import Console

console = Console()

class ReportGenerator:
    """HTML 보고서 생성기"""
    
    def __init__(self):
        """초기화"""
        self.supabase_manager = SupabaseManager()
        self.reports_dir = Path(__file__).parent / "reports"
        self.reports_dir.mkdir(exist_ok=True)
        
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
    
    def _get_article_stats(self, issue_id: str) -> Dict[str, int]:
        """이슈별 기사 통계 조회"""
        try:
            result = self.supabase_manager.client.table('issue_articles').select(
                'article_id, articles!inner(media_id, media_outlets!inner(name, bias))'
            ).eq('issue_id', issue_id).execute()
            
            if not result.data:
                return {"total": 0, "left": 0, "center": 0, "right": 0}
            
            stats = {"total": len(result.data), "left": 0, "center": 0, "right": 0}
            
            for item in result.data:
                bias = item['articles']['media_outlets']['bias']
                if bias in stats:
                    stats[bias] += 1
            
            return stats
            
        except Exception as e:
            console.print(f"❌ 기사 통계 조회 실패: {str(e)}")
            return {"total": 0, "left": 0, "center": 0, "right": 0}
    
    def _get_default_css(self) -> str:
        """기본 CSS 스타일 반환"""
        return """
/* 전체 레이아웃 */
* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    line-height: 1.6;
    color: #1a1a1a;
    background: #ffffff;
    padding: 20px;
    max-width: 800px;
    margin: 0 auto;
}

/* 헤더 */
.header {
    text-align: center;
    margin-bottom: 40px;
    padding-bottom: 20px;
    border-bottom: 2px solid #e9ecef;
}

.header h1 {
    font-size: 28px;
    font-weight: 700;
    color: #1a1a1a;
    margin-bottom: 8px;
}

.header .subtitle {
    font-size: 16px;
    color: #666666;
    font-weight: 400;
}

/* 이슈 카드 */
.issue-card {
    background: #ffffff;
    border: 1px solid #e9ecef;
    border-radius: 12px;
    padding: 24px;
    margin-bottom: 32px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}

.issue-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15);
}

/* 이슈 헤더 */
.issue-header {
    margin-bottom: 20px;
}

.meta-info {
    font-size: 12px;
    color: #999999;
    margin-bottom: 8px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.title {
    font-size: 20px;
    font-weight: 600;
    color: #1a1a1a;
    margin-bottom: 8px;
    line-height: 1.4;
}

.subtitle {
    font-size: 16px;
    color: #666666;
    font-weight: 400;
    line-height: 1.5;
}

/* 좌우 관점 나란히 배치 */
.side-views-container {
    display: flex;
    gap: 20px;
    margin-bottom: 24px;
}

.side-view {
    flex: 1;
    padding: 16px;
    border-radius: 12px;
    border: 1px solid #e9ecef;
}

.side-view.left {
    background-color: rgba(25, 118, 210, 0.05);
    border-color: rgba(25, 118, 210, 0.2);
}

.side-view.right {
    background-color: rgba(220, 53, 69, 0.05);
    border-color: rgba(220, 53, 69, 0.2);
}

.side-view .view-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    cursor: pointer;
    margin-bottom: 8px;
}

.side-view .view-title {
    margin-bottom: 0;
    font-size: 14px;
    font-weight: 600;
    background: none;
    border: none;
    padding: 0;
    border-radius: 0;
}

.side-view .view-summary {
    font-size: 14px;
    line-height: 1.5;
    color: #333333;
    cursor: pointer;
    margin-bottom: 8px;
    font-weight: 500;
}

.side-view .view-content {
    font-size: 14px;
    line-height: 1.5;
    color: #666666;
}

.side-view .toggle-icon {
    font-size: 12px;
    transition: transform 0.2s ease;
}

.side-view .toggle-icon.rotated {
    transform: rotate(180deg);
}

/* 접을 수 있는 섹션 */
.collapsible-section {
    margin-bottom: 24px;
}

.collapsible-header {
    cursor: pointer;
    user-select: none;
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 8px 0;
    border-bottom: 1px solid #e9ecef;
    transition: all 0.2s ease;
}

.collapsible-header:hover {
    background-color: #f8f9fa;
    border-radius: 4px;
    padding: 8px 12px;
}

.collapsible-section .view-summary {
    font-size: 14px;
    line-height: 1.5;
    color: #333333;
    cursor: pointer;
    margin-bottom: 8px;
    font-weight: 500;
    padding: 8px 0;
}

.toggle-icon {
    font-size: 12px;
    color: #666666;
    transition: transform 0.2s ease;
}

.toggle-icon.rotated {
    transform: rotate(180deg);
}

/* 섹션 */
.section {
    margin-bottom: 24px;
}

.section-label {
    font-size: 14px;
    font-weight: 600;
    color: #1a1a1a;
    margin-bottom: 8px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.section-content {
    font-size: 14px;
    color: #888888;
    line-height: 1.6;
}

/* 게이지바 */
.gauge-container {
    margin-bottom: 24px;
    padding: 16px;
    background: #fafbfc;
    border-radius: 8px;
    border: 1px solid #e1e5e9;
}

.gauge-title {
    font-size: 16px;
    font-weight: 600;
    color: #1a1a1a;
    margin-bottom: 16px;
    text-align: center;
}

.gauge-bar {
    height: 24px;
    background: #f1f3f4;
    border-radius: 4px;
    overflow: hidden;
    position: relative;
    margin-bottom: 12px;
    box-shadow: inset 0 1px 2px rgba(0, 0, 0, 0.05);
}

.gauge-fill {
    height: 100%;
    display: flex;
    border-radius: 4px;
    overflow: hidden;
}

.gauge-left {
    background: #1976d2;
    position: relative;
    box-shadow: 0 1px 2px rgba(25, 118, 210, 0.2);
}

.gauge-center {
    background: #9ca3af;
    position: relative;
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.1);
}

.gauge-right {
    background: #dc3545;
    position: relative;
    box-shadow: 0 1px 2px rgba(220, 53, 69, 0.2);
}

.gauge-percentage {
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    color: white;
    font-weight: 600;
    font-size: 12px;
    text-shadow: 0 1px 2px rgba(0, 0, 0, 0.5);
    z-index: 2;
    text-align: center;
    width: 100%;
}

.gauge-center .gauge-percentage {
    color: #ffffff;
    font-weight: 700;
    text-shadow: 0 1px 2px rgba(0, 0, 0, 0.5);
}

/* Background 불렛 포인트 스타일 */
.background-bullets {
    margin: 16px 0;
}

.background-bullet-container {
    margin-bottom: 16px;
}

.background-bullet {
    padding: 12px 16px;
    background-color: #f8f9fa;
    border-radius: 8px;
    font-size: 14px;
    line-height: 1.7;
    color: #2c3e50;
    font-weight: 400;
    transition: all 0.2s ease;
    position: relative;
    word-wrap: break-word;
    overflow-wrap: break-word;
    white-space: normal;
    max-width: 100%;
}

.background-bullet:hover {
    background-color: #e3f2fd;
    transform: translateX(2px);
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

.background-bullet:last-child {
    margin-bottom: 0;
}

/* 기타 유틸리티 */
.no-content {
    color: #999999;
    font-style: italic;
    font-size: 14px;
}

/* 반응형 디자인 */
@media (max-width: 768px) {
    body {
        padding: 16px;
    }
    
    .issue-card {
        padding: 20px;
        margin-bottom: 24px;
    }
    
    .title {
        font-size: 18px;
    }
    
    .subtitle {
        font-size: 15px;
    }
    
    .side-views-container {
        flex-direction: column;
        gap: 12px;
    }
    
    .side-view {
        padding: 12px;
    }
    
    .background-bullet {
        padding: 10px 14px;
        font-size: 15px;
        margin-bottom: 12px;
    }
}
"""
    
    def _format_background(self, text: str) -> str:
        """Background 텍스트 포맷팅 (불렛 포인트 스타일)"""
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
            
            # 너무 긴 텍스트는 적절히 줄임 (200자 제한)
            if len(content) > 200:
                content = content[:200] + "..."
            
            # HTML 생성
            bullet_html += '<div class="background-bullet-container">'
            bullet_html += f'<div class="background-bullet">{content}</div>'
            bullet_html += '</div>'
        
        bullet_html += '</div>'
        return bullet_html
    
    def _generate_gauge_bar(self, stats: Dict[str, int]) -> str:
        """게이지바 HTML 생성"""
        total = stats.get('total', 0)
        if total == 0:
            return '<div class="gauge-bar"><div class="gauge-fill"></div></div>'
        
        left_pct = (stats.get('left', 0) / total) * 100
        center_pct = (stats.get('center', 0) / total) * 100
        right_pct = (stats.get('right', 0) / total) * 100
        
        gauge_html = '<div class="gauge-bar">'
        gauge_html += '<div class="gauge-fill">'
        
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

    def _generate_side_views(self, issue: Dict[str, Any]) -> str:
        """좌우 관점을 나란히 배치"""
        left_title, left_content = self._parse_view_data(issue.get('left_view', ''))
        right_title, right_content = self._parse_view_data(issue.get('right_view', ''))
        
        left_html = ""
        right_html = ""
        
        if left_title:
            left_html = f"""
        <div class="side-view left">
            <div class="view-header" onclick="toggleView('left-{issue['id'][:8]}')">
                <div class="view-title left">좌파 관점</div>
                <span class="toggle-icon" id="toggle-left-{issue['id'][:8]}">▼</span>
            </div>
            <div class="view-summary" onclick="toggleView('left-{issue['id'][:8]}')">
                {left_title}
            </div>
            <div class="view-content collapsible" id="content-left-{issue['id'][:8]}" style="display: none;">
                {self._format_content(left_content)}
            </div>
        </div>
"""
        
        if right_title:
            right_html = f"""
        <div class="side-view right">
            <div class="view-header" onclick="toggleView('right-{issue['id'][:8]}')">
                <div class="view-title right">우파 관점</div>
                <span class="toggle-icon" id="toggle-right-{issue['id'][:8]}">▼</span>
            </div>
            <div class="view-summary" onclick="toggleView('right-{issue['id'][:8]}')">
                {right_title}
            </div>
            <div class="view-content collapsible" id="content-right-{issue['id'][:8]}" style="display: none;">
                {self._format_content(right_content)}
            </div>
        </div>
"""
        
        return f"""
        <div class="side-views-container">
            {left_html}
            {right_html}
        </div>
"""
    
    def _generate_center_view_section(self, issue: Dict[str, Any]) -> str:
        """게이지바 아래 중립 관점 섹션 생성"""
        center_title, center_content = self._parse_view_data(issue.get('center_view', ''))
        
        if not center_title:
            return ""
        
        return f"""
        <div class="collapsible-section">
            <div class="section-label collapsible-header" onclick="toggleCollapse('center-{issue['id'][:8]}')">
                <span>중립 관점</span>
                <span class="toggle-icon" id="toggle-center-{issue['id'][:8]}">▼</span>
            </div>
            <div class="section-content" id="center-{issue['id'][:8]}" style="display: none;">
                <div class="view-summary" style="font-weight: 600; margin-bottom: 12px; color: #28a745;">
                    {center_title}
                </div>
                <div class="view-content" style="color: #666666; line-height: 1.6;">
                    {self._format_content(center_content)}
                </div>
            </div>
        </div>
"""
    
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

    def _generate_issue_card(self, issue: Dict[str, Any]) -> str:
        """이슈 카드 HTML 생성"""
        stats = self._get_article_stats(issue['id'])
        gauge_bar = self._generate_gauge_bar(stats)
        
        # 날짜 포맷팅
        created_date = issue['created_at'][:10] if issue['created_at'] else ""
        if created_date:
            try:
                from datetime import datetime
                date_obj = datetime.strptime(created_date, '%Y-%m-%d')
                formatted_date = date_obj.strftime('%Y년 %m월 %d일')
            except:
                formatted_date = created_date
        else:
            formatted_date = ""
        
        # 제목과 부제목에서 따옴표 제거
        clean_title = str(issue['title']).strip('"').strip("'") if issue['title'] else ""
        clean_subtitle = str(issue['issue_summary']).strip('"').strip("'") if issue['issue_summary'] else ""
        
        return f"""
    <div class="issue-card">
        <div class="issue-header">
            <div class="meta-info">{stats['total']}개 기사 ∙ {formatted_date}</div>
            <div class="title">{clean_title}</div>
            <div class="subtitle">{clean_subtitle}</div>
        </div>
        
        {self._generate_side_views(issue)}
        
        <div class="gauge-container">
            <div class="gauge-title">언론사 성향별 보도 비율</div>
            {gauge_bar}
        </div>
        
        {self._generate_center_view_section(issue)}
        
        <div class="collapsible-section">
            <div class="section-label collapsible-header" onclick="toggleCollapse('background-{issue['id'][:8]}')">
                <span>배경 정보</span>
                <span class="toggle-icon" id="toggle-background-{issue['id'][:8]}">▼</span>
            </div>
            <div class="section-content" id="background-{issue['id'][:8]}" style="display: none;">
                {self._format_background(issue['issue_timeline'])}
            </div>
        </div>
    </div>
"""
    
    def save_report(self, html: str, filename: str = None) -> str:
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
    
    def generate_html(self) -> str:
        """전체 HTML 생성"""
        try:
            # 이슈 데이터 조회
            result = self.supabase_manager.client.table('issues').select(
                'id, title, issue_summary, issue_timeline, left_view, center_view, right_view, created_at'
            ).order('created_at', desc=True).execute()
            
            if not result.data:
                console.print("❌ 이슈 데이터가 없습니다.")
                return None
            
            console.print(f"✅ {len(result.data)}개 이슈 데이터 조회 완료")
            
            # 각 이슈에 통계 정보 추가
            for issue in result.data:
                stats = self._get_article_stats(issue['id'])
                issue['total_articles'] = stats['total']
            
            # source가 10개 이상인 이슈만 필터링
            filtered_issues = [issue for issue in result.data if issue.get('total_articles', 0) >= 10]
            console.print(f"✅ source 10개 이상 이슈 필터링: {len(result.data)}개 → {len(filtered_issues)}개")
            
            # source 순으로 정렬
            filtered_issues.sort(key=lambda x: x.get('total_articles', 0), reverse=True)
            console.print("✅ source 순으로 정렬 완료")
            
            # HTML 생성
            html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>정치 이슈 보고서</title>
    <style>
        {self._get_default_css()}
    </style>
</head>
<body>
    <div class="header">
        <h1>정치 이슈 보고서</h1>
        <div class="subtitle">언론사 성향별 분석</div>
    </div>
    
    {''.join([self._generate_issue_card(issue) for issue in filtered_issues])}
    
    <script>
        function toggleCollapse(elementId) {{
            const content = document.getElementById(elementId);
            const toggleIcon = document.getElementById('toggle-' + elementId);
            
            if (content.style.display === 'none') {{
                content.style.display = 'block';
                toggleIcon.textContent = '▲';
                toggleIcon.classList.add('rotated');
            }} else {{
                content.style.display = 'none';
                toggleIcon.textContent = '▼';
                toggleIcon.classList.remove('rotated');
            }}
        }}
        
        function toggleView(viewId) {{
            const content = document.getElementById('content-' + viewId);
            const toggleIcon = document.getElementById('toggle-' + viewId);
            
            if (content.style.display === 'none') {{
                content.style.display = 'block';
                toggleIcon.textContent = '▲';
                toggleIcon.classList.add('rotated');
            }} else {{
                content.style.display = 'none';
                toggleIcon.textContent = '▼';
                toggleIcon.classList.remove('rotated');
            }}
        }}
    </script>
</body>
</html>"""
            
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