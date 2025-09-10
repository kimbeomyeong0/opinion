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
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()

class ReportGenerator:
    """HTML 보고서 생성기"""
    
    def __init__(self):
        self.supabase_manager = SupabaseManager()
        self.reports_dir = Path(__file__).parent / "reports"
        self.reports_dir.mkdir(exist_ok=True)
        
    def generate_filename(self, date: datetime = None) -> str:
        """날짜 기반 파일명 생성 (MMDD, MMDD(1) 형식)"""
        if date is None:
            date = datetime.now()
        
        base_name = date.strftime("%m%d")
        counter = 1
        
        while True:
            if counter == 1:
                filename = f"{base_name}.html"
            else:
                filename = f"{base_name}({counter}).html"
            
            filepath = self.reports_dir / filename
            if not filepath.exists():
                return filename
            
            counter += 1
    
    def get_real_issues(self, count: int = 2) -> List[Dict[str, Any]]:
        """실제 데이터베이스에서 이슈 데이터 조회"""
        if not self.supabase_manager.client:
            console.print("❌ 데이터베이스 연결 실패")
            return []
        
        try:
            # issues 테이블에서 이슈 조회
            result = self.supabase_manager.client.table('issues').select(
                'id, title, subtitle, background, summary, left_view, center_view, right_view, created_at'
            ).limit(count).execute()
            
            if not result.data:
                console.print("❌ 이슈 데이터가 없습니다.")
                return []
            
            issues = []
            for issue in result.data:
                # 각 이슈별로 관련 기사 수 조회
                article_stats = self._get_article_stats(issue['id'])
                
                issue_data = {
                    "created_at": issue.get('created_at', '')[:16] if issue.get('created_at') else '',
                    "title": issue.get('title', ''),
                    "subtitle": issue.get('subtitle', ''),
                    "background": self._highlight_background_last_sentence(issue.get('background', '')),
                    "summary": issue.get('summary', ''),
                    "left_view": self._highlight_stance(issue.get('left_view', ''), 'left'),
                    "center_view": self._highlight_stance(issue.get('center_view', ''), 'center'),
                    "right_view": self._highlight_stance(issue.get('right_view', ''), 'right'),
                    **article_stats
                }
                issues.append(issue_data)
            
            return issues
            
        except Exception as e:
            console.print(f"❌ 이슈 데이터 조회 실패: {str(e)}")
            return []
    
    def _highlight_stance(self, text: str, bias: str = '') -> str:
        """스탠스 부분에 하이라이트 처리"""
        if not text or text.strip() == '' or text.lower() == 'none':
            return '<span class="no-content">해당 성향의 언론사에서 이 이슈를 보도하지 않았습니다.</span>'
        
        # 스탠스 패턴들
        stance_patterns = [
            r'(지지한다)',
            r'(반대한다)',
            r'(비판한다)',
            r'(중립적 입장에서)',
            r'(옹호한다)',
            r'(지원한다)',
            r'(경계한다)',
            r'(신중한 입장에서)',
            r'(단호한 입장에서)',
            r'(강력히 비판한다)',
            r'(철저히 조사한다)',
            r'(적극 지지한다)',
            r'(강력히 반대한다)',
            r'(지지한다\.)',
            r'(반대한다\.)',
            r'(비판한다\.)'
        ]
        
        highlighted_text = text
        
        for pattern in stance_patterns:
            highlighted_text = re.sub(
                pattern, 
                r'<span class="stance-highlight">\1</span>', 
                highlighted_text
            )
        
        return highlighted_text
    
    def _highlight_background_last_sentence(self, text: str) -> str:
        """배경 정보의 마지막 문장에 하이라이트 처리"""
        if not text or text.strip() == '':
            return text
        
        # 문장을 구분 (마침표, 느낌표, 물음표로 구분)
        sentences = re.split(r'[.!?]+', text.strip())
        sentences = [s.strip() for s in sentences if s.strip()]
        
        if len(sentences) <= 1:
            return text
        
        # 마지막 문장에 하이라이트 적용
        last_sentence = sentences[-1]
        highlighted_last = f'<span class="background-highlight">{last_sentence}</span>'
        
        # 나머지 문장들과 합치기
        other_sentences = sentences[:-1]
        result = '. '.join(other_sentences) + '. ' + highlighted_last
        
        return result
    
    def _get_article_stats(self, issue_id: str) -> Dict[str, int]:
        """이슈별 기사 통계 조회"""
        try:
            # issue_articles 테이블에서 관련 기사들 조회
            result = self.supabase_manager.client.table('issue_articles').select(
                'articles!inner(media_id, media_outlets!inner(bias))'
            ).eq('issue_id', issue_id).execute()
            
            if not result.data:
                return {"total_articles": 0, "left_articles": 0, "center_articles": 0, "right_articles": 0}
            
            # 성향별 기사 수 계산
            left_count = 0
            center_count = 0
            right_count = 0
            
            for item in result.data:
                if 'articles' in item and 'media_outlets' in item['articles']:
                    bias = item['articles']['media_outlets'].get('bias', 'center')
                    if bias == 'left':
                        left_count += 1
                    elif bias == 'right':
                        right_count += 1
                    else:
                        center_count += 1
            
            total = left_count + center_count + right_count
            
            return {
                "total_articles": total,
                "left_articles": left_count,
                "center_articles": center_count,
                "right_articles": right_count
            }
            
        except Exception as e:
            console.print(f"❌ 기사 통계 조회 실패: {str(e)}")
            return {"total_articles": 0, "left_articles": 0, "center_articles": 0, "right_articles": 0}
    
    def generate_html(self, issues: List[Dict[str, Any]]) -> str:
        """HTML 보고서 생성"""
        current_time = datetime.now().strftime("%Y.%m.%d %H:%M")
        
        html = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>정치 이슈 분석 보고서</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background-color: #ffffff;
            color: #1a1a1a;
            line-height: 1.6;
            max-width: 600px;
            margin: 0 auto;
            padding: 24px;
        }}
        
        .header {{
            text-align: center;
            margin-bottom: 32px;
            padding-bottom: 24px;
            border-bottom: 1px solid #e5e5e5;
        }}
        
        .header h1 {{
            font-size: 24px;
            font-weight: 700;
            margin-bottom: 8px;
            color: #1a1a1a;
        }}
        
        .header .subtitle {{
            font-size: 16px;
            color: #666666;
            margin-bottom: 16px;
        }}
        
        .header .meta {{
            font-size: 14px;
            color: #999999;
        }}
        
        .issue-card {{
            background: #ffffff;
            border: 1px solid #e5e5e5;
            border-radius: 8px;
            padding: 24px;
            margin-bottom: 32px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        
        .created-at {{
            font-size: 14px;
            color: #999999;
            margin-bottom: 16px;
        }}
        
        .title {{
            font-size: 24px;
            font-weight: 700;
            margin-bottom: 8px;
            color: #1a1a1a;
            line-height: 1.3;
        }}
        
        .subtitle {{
            font-size: 18px;
            font-weight: 500;
            color: #333333;
            margin-bottom: 24px;
            line-height: 1.4;
        }}
        
        .section {{
            margin-bottom: 24px;
        }}
        
        .section-label {{
            font-size: 14px;
            font-weight: 600;
            color: #666666;
            margin-bottom: 8px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        .section-content {{
            font-size: 16px;
            color: #1a1a1a;
            line-height: 1.6;
        }}
        
        .source-stats {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
            padding: 16px;
            background: #f8f9fa;
            border-radius: 6px;
        }}
        
        .source-item {{
            text-align: center;
        }}
        
        .source-number {{
            font-size: 20px;
            font-weight: 700;
            color: #1a1a1a;
        }}
        
        .source-label {{
            font-size: 12px;
            color: #666666;
            margin-top: 4px;
        }}
        
        .gauge-container {{
            margin-bottom: 24px;
        }}
        
        .gauge-bar {{
            height: 12px;
            background: #e5e5e5;
            border-radius: 6px;
            overflow: hidden;
            margin-bottom: 8px;
        }}
        
        .gauge-fill {{
            height: 100%;
            display: flex;
        }}
        
        .gauge-left {{
            background: #0066cc;
        }}
        
        .gauge-center {{
            background: #999999;
        }}
        
        .gauge-right {{
            background: #dc3545;
        }}
        
        .gauge-labels {{
            display: flex;
            justify-content: space-between;
            font-size: 12px;
            color: #666666;
        }}
        
        .view-section {{
            margin-bottom: 20px;
        }}
        
        .view-title {{
            font-size: 14px;
            font-weight: 600;
            color: #666666;
            margin-bottom: 8px;
            padding: 8px 12px;
            background: #f5f5f5;
            border-radius: 16px;
            display: inline-block;
        }}
        
        .view-title.left {{
            background: #e3f2fd;
            color: #1976d2;
        }}
        
        .view-title.right {{
            background: #ffebee;
            color: #d32f2f;
        }}
        
        .view-title.center {{
            background: #f3e5f5;
            color: #7b1fa2;
        }}
        
        .view-content {{
            font-size: 15px;
            color: #1a1a1a;
            line-height: 1.6;
            padding-left: 8px;
        }}
        
        .stance-highlight {{
            background: linear-gradient(120deg, #c8e6c9 0%, #c8e6c9 100%);
            background-size: 100% 0.4em;
            background-repeat: no-repeat;
            background-position: 0 85%;
            padding: 0 3px;
            font-weight: 500;
        }}
        
        .background-highlight {{
            background: linear-gradient(120deg, #c8e6c9 0%, #c8e6c9 100%);
            background-size: 100% 0.4em;
            background-repeat: no-repeat;
            background-position: 0 85%;
            padding: 0 3px;
            font-weight: 500;
        }}
        
        .no-content {{
            color: #999999;
            font-style: italic;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>정치 이슈 분석 보고서</h1>
        <div class="subtitle">Political Insights Report</div>
        <div class="meta">생성일시: {current_time}</div>
    </div>
"""
        
        for issue in issues:
            # 게이지바 계산
            total = issue['total_articles']
            left_pct = (issue['left_articles'] / total * 100) if total > 0 else 0
            center_pct = (issue['center_articles'] / total * 100) if total > 0 else 0
            right_pct = (issue['right_articles'] / total * 100) if total > 0 else 0
            
            html += f"""
    <div class="issue-card">
        <div class="created-at">{issue['created_at']}</div>
        
        <div class="title">{issue['title']}</div>
        <div class="subtitle">{issue['subtitle']}</div>
        
        <div class="section">
            <div class="section-label">배경 정보</div>
            <div class="section-content">{issue['background']}</div>
        </div>
        
        <div class="source-stats">
            <div class="source-item">
                <div class="source-number">{issue['total_articles']}</div>
                <div class="source-label">전체</div>
            </div>
            <div class="source-item">
                <div class="source-number">{issue['left_articles']}</div>
                <div class="source-label">Left</div>
            </div>
            <div class="source-item">
                <div class="source-number">{issue['center_articles']}</div>
                <div class="source-label">Center</div>
            </div>
            <div class="source-item">
                <div class="source-number">{issue['right_articles']}</div>
                <div class="source-label">Right</div>
            </div>
        </div>
        
        <div class="gauge-container">
            <div class="gauge-bar">
                <div class="gauge-fill">
                    <div class="gauge-left" style="width: {left_pct}%"></div>
                    <div class="gauge-center" style="width: {center_pct}%"></div>
                    <div class="gauge-right" style="width: {right_pct}%"></div>
                </div>
            </div>
            <div class="gauge-labels">
                <span>좌파 {left_pct:.0f}%</span>
                <span>중립 {center_pct:.0f}%</span>
                <span>우파 {right_pct:.0f}%</span>
            </div>
        </div>
        
        <div class="section">
            <div class="section-label">핵심 쟁점</div>
            <div class="section-content">{issue['summary']}</div>
        </div>
        
        <div class="view-section">
            <div class="view-title left">좌파 관점</div>
            <div class="view-content">{issue['left_view']}</div>
        </div>
        
        <div class="view-section">
            <div class="view-title right">우파 관점</div>
            <div class="view-content">{issue['right_view']}</div>
        </div>
        
        <div class="view-section">
            <div class="view-title center">중립 관점</div>
            <div class="view-content">{issue['center_view']}</div>
        </div>
    </div>
"""
        
        html += """
</body>
</html>
"""
        return html
    
    def save_report(self, html: str, filename: str) -> str:
        """보고서 파일 저장"""
        filepath = self.reports_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html)
        return str(filepath)
    
    def generate_report(self, issues: List[Dict[str, Any]] = None) -> str:
        """보고서 생성 메인 함수"""
        if issues is None:
            issues = self.get_real_issues(2)
        
        filename = self.generate_filename()
        html = self.generate_html(issues)
        filepath = self.save_report(html, filename)
        
        console.print(f"✅ 보고서 생성 완료: {filename}")
        console.print(f"📁 저장 위치: {filepath}")
        
        return filepath

def main():
    """메인 실행 함수"""
    console.print("🚀 정치 이슈 HTML 보고서 생성기 시작")
    
    generator = ReportGenerator()
    
    # 실제 데이터베이스에서 이슈 2개 조회해서 테스트 생성
    console.print("📊 실제 데이터베이스에서 이슈 2개를 조회하여 보고서 생성 중...")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("보고서 생성 중...", total=None)
        
        filepath = generator.generate_report()
        
        progress.update(task, completed=True)
    
    console.print(f"🎉 보고서 생성 완료!")
    console.print(f"📱 모바일에서 확인해보세요: {filepath}")

if __name__ == "__main__":
    main()