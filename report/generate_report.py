#!/usr/bin/env python3
"""
정치 이슈 HTML 보고서 생성기 (완전 리팩토링 버전)
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
    
    # 애니메이션 타입 상수
    ANIMATION_TYPES = {
        "wave": "gauge-wave",
        "flow": "gauge-flow", 
        "pulse": "gauge-pulse",
        "sparkle": "gauge-sparkle",
        "3d": "gauge-3d",
        "typewriter": "gauge-typewriter"
    }
    
    def __init__(self, animation_type: str = "wave"):
        self.supabase_manager = SupabaseManager()
        self.reports_dir = Path(__file__).parent / "reports"
        self.reports_dir.mkdir(exist_ok=True)
        self.animation_type = self.ANIMATION_TYPES.get(animation_type, "gauge-wave")
        
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
    
    def get_real_issues(self, count: int = None) -> List[Dict[str, Any]]:
        """실제 데이터베이스에서 이슈 데이터 조회"""
        if not self.supabase_manager.client:
            console.print("❌ 데이터베이스 연결 실패")
            return []
        
        try:
            # issues 테이블에서 이슈 조회
            query = self.supabase_manager.client.table('issues').select(
                'id, title, subtitle, background, summary, left_view, center_view, right_view, created_at'
            )
            
            if count is not None:
                query = query.limit(count)
            
            result = query.execute()
            
            if not result.data:
                console.print("❌ 이슈 데이터가 없습니다.")
                return []
            
            issues = []
            for issue in result.data:
                # 각 이슈별로 관련 기사 수 조회
                article_stats = self._get_article_stats(issue['id'])
                
                # 이슈별 주요 언론사 정보 조회
                primary_sources = self._get_primary_sources(issue['id'])
                
                issue_data = {
                    'id': issue['id'],
                    'title': issue['title'],
                    'subtitle': issue['subtitle'],
                    'background': issue['background'],
                    'summary': issue['summary'],
                    'left_view': issue['left_view'],
                    'center_view': issue['center_view'],
                    'right_view': issue['right_view'],
                    'created_at': issue['created_at'],
                    'total_articles': article_stats['total'],
                    'left_articles': article_stats['left'],
                    'center_articles': article_stats['center'],
                    'right_articles': article_stats['right'],
                    'primary_sources': primary_sources
                }
                issues.append(issue_data)
            
            console.print(f"✅ {len(issues)}개 이슈 데이터 조회 완료")
            return issues
            
        except Exception as e:
            console.print(f"❌ 이슈 데이터 조회 실패: {e}")
            return []
    
    def _get_article_stats(self, issue_id: str) -> Dict[str, int]:
        """이슈별 기사 통계 조회 (올바른 스키마 사용)"""
        try:
            # issue_articles 테이블을 통해 articles와 media_outlets 조인
            result = self.supabase_manager.client.table('issue_articles').select(
                'article_id, articles!inner(media_id, media_outlets!inner(name, bias))'
            ).eq('issue_id', issue_id).execute()
            
            if not result.data:
                return {'total': 0, 'left': 0, 'center': 0, 'right': 0}
            
            # bias별 기사 수 계산
            bias_counts = {'left': 0, 'center': 0, 'right': 0}
            for item in result.data:
                # articles 정보에서 media_outlets 추출
                article = item.get('articles', {})
                media_outlet = article.get('media_outlets', {})
                bias = media_outlet.get('bias', 'center')
                
                if bias in bias_counts:
                    bias_counts[bias] += 1
            
            return {
                'total': len(result.data),
                'left': bias_counts['left'],
                'center': bias_counts['center'],
                'right': bias_counts['right']
            }
            
        except Exception as e:
            console.print(f"❌ 기사 통계 조회 실패: {e}")
            return {'total': 0, 'left': 0, 'center': 0, 'right': 0}
    
    def _get_primary_sources(self, issue_id: str) -> List[str]:
        """이슈별 주요 언론사 조회 (올바른 스키마 사용)"""
        try:
            result = self.supabase_manager.client.table('issue_articles').select(
                'article_id, articles!inner(media_id, media_outlets!inner(name, bias))'
            ).eq('issue_id', issue_id).execute()
            
            if not result.data:
                return []
            
            # 언론사별 기사 수 계산
            outlet_counts = {}
            for item in result.data:
                article = item.get('articles', {})
                media_outlet = article.get('media_outlets', {})
                outlet_name = media_outlet.get('name', '')
                if outlet_name:
                    outlet_counts[outlet_name] = outlet_counts.get(outlet_name, 0) + 1
            
            # 기사 수가 많은 순으로 정렬하여 상위 3개 반환
            sorted_outlets = sorted(outlet_counts.items(), key=lambda x: x[1], reverse=True)
            return [outlet for outlet, count in sorted_outlets[:3]]
            
        except Exception as e:
            console.print(f"❌ 주요 언론사 조회 실패: {e}")
            return []
    
    def generate_html(self, issues: List[Dict[str, Any]]) -> str:
        """HTML 보고서 생성 (인라인 CSS 포함)"""
        current_time = datetime.now().strftime("%Y년 %m월 %d일 %H:%M")
        
        # CSS를 인라인으로 포함하여 로딩 문제 해결
        css_content = self._get_css_content()
        
        html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>정치 이슈 분석 보고서</title>
    <style>
{css_content}
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
            html += self._generate_issue_card(issue)
        
        html += """
</body>
</html>
"""
        return html
    
    def _get_css_content(self) -> str:
        """CSS 내용을 문자열로 반환"""
        return self._get_default_css()
    
    def _get_default_css(self) -> str:
        """기본 CSS 반환"""
        return """
/* 기본 스타일 */
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
    max-width: 600px;
    margin: 0 auto;
}

.header {
    text-align: center;
    margin-bottom: 40px;
    padding-bottom: 20px;
    border-bottom: 2px solid #e9ecef;
}

.title {
    font-size: 28px;
    font-weight: 700;
    color: #1a1a1a;
    line-height: 1.3;
}

.subtitle {
    font-size: 18px;
    font-weight: 500;
    color: #333333;
    margin-bottom: 24px;
    line-height: 1.4;
}

.meta {
    font-size: 14px;
    color: #666666;
    margin-top: 8px;
}

.issue-card {
    background: #ffffff;
    border: 1px solid #e9ecef;
    border-radius: 12px;
    padding: 24px;
    margin-bottom: 32px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    transition: box-shadow 0.3s ease;
}

.issue-card:hover {
    box-shadow: 0 4px 16px rgba(0,0,0,0.12);
}

.created-at {
    font-size: 12px;
    color: #999999;
    margin-bottom: 12px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.section {
    margin-bottom: 24px;
}

.section-label {
    font-size: 14px;
    font-weight: 600;
    color: #666666;
    margin-bottom: 8px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.section-content {
    font-size: 16px;
    color: #1a1a1a;
    line-height: 1.6;
}

.source-stats {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
    padding: 16px;
    background: #f8f9fa;
    border-radius: 6px;
}

.source-item {
    text-align: center;
}

.source-number {
    font-size: 20px;
    font-weight: 700;
    color: #1a1a1a;
}

.source-label {
    font-size: 12px;
    color: #666666;
    margin-top: 4px;
}

.gauge-container {
    margin-bottom: 32px;
    padding: 20px;
    background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
    border-radius: 12px;
    border: 1px solid #e9ecef;
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
}

.gauge-title {
    font-size: 16px;
    font-weight: 600;
    color: #2c3e50;
    margin-bottom: 16px;
    text-align: center;
}

.gauge-bar {
    height: 32px;
    background: linear-gradient(90deg, #f1f3f4 0%, #e8eaed 100%);
    border-radius: 16px;
    overflow: hidden;
    margin-bottom: 12px;
    position: relative;
    box-shadow: inset 0 2px 4px rgba(0,0,0,0.1);
    border: 1px solid #dadce0;
}

.gauge-fill {
    height: 100%;
    display: flex;
    transition: all 0.8s ease-in-out;
    animation: fillGauge 1.5s ease-out;
}

.gauge-left {
    background: linear-gradient(135deg, #1976d2 0%, #1565c0 100%);
    box-shadow: 0 2px 4px rgba(25, 118, 210, 0.3);
    position: relative;
}

.gauge-center {
    background: linear-gradient(135deg, #6c757d 0%, #5a6268 100%);
    box-shadow: 0 2px 4px rgba(108, 117, 125, 0.3);
    position: relative;
}

.gauge-right {
    background: linear-gradient(135deg, #dc3545 0%, #c82333 100%);
    box-shadow: 0 2px 4px rgba(220, 53, 69, 0.3);
    position: relative;
}

.gauge-percentage {
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    font-size: 12px;
    font-weight: 700;
    color: #ffffff;
    text-shadow: 0 1px 2px rgba(0,0,0,0.5);
    z-index: 10;
}

@keyframes fillGauge {
    0% { width: 0%; }
    100% { width: var(--target-width); }
}

@keyframes wave {
    0%, 100% { transform: scaleY(1); }
    50% { transform: scaleY(1.1); }
}

@keyframes waveMove {
    0% { transform: translateX(-100%); }
    100% { transform: translateX(100%); }
}

@keyframes gradientFlow {
    0% { background-position: 0% 50%; }
    50% { background-position: 100% 50%; }
    100% { background-position: 0% 50%; }
}

@keyframes pulse {
    0%, 100% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.7; transform: scale(1.05); }
}

@keyframes sparkle {
    0%, 100% { opacity: 0; transform: translateY(-50%) scale(0.5); }
    50% { opacity: 1; transform: translateY(-50%) scale(1.2); }
}

@keyframes rotate3d {
    0%, 100% { transform: rotateX(0deg) rotateY(0deg); }
    25% { transform: rotateX(5deg) rotateY(5deg); }
    50% { transform: rotateX(0deg) rotateY(10deg); }
    75% { transform: rotateX(-5deg) rotateY(5deg); }
}

@keyframes typewriter {
    0% { width: 0%; }
    100% { width: var(--target-width); }
}

/* 애니메이션 클래스들 */
.gauge-wave .gauge-left { animation: wave 4s ease-in-out infinite; }
.gauge-wave .gauge-center { animation: wave 4s ease-in-out infinite 0.5s; }
.gauge-wave .gauge-right { animation: wave 4s ease-in-out infinite 1s; }

.gauge-wave .gauge-left::after,
.gauge-wave .gauge-center::after,
.gauge-wave .gauge-right::after {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; bottom: 0;
    background: linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.4) 50%, transparent 100%);
    animation: waveMove 3s ease-in-out infinite;
}

.gauge-flow .gauge-left {
    background: linear-gradient(45deg, #1976d2, #42a5f5, #1976d2, #1565c0);
    background-size: 400% 400%;
    animation: gradientFlow 3s ease infinite;
}

.gauge-flow .gauge-center {
    background: linear-gradient(45deg, #6c757d, #adb5bd, #6c757d, #5a6268);
    background-size: 400% 400%;
    animation: gradientFlow 3s ease infinite 0.5s;
}

.gauge-flow .gauge-right {
    background: linear-gradient(45deg, #dc3545, #ff6b6b, #dc3545, #c82333);
    background-size: 400% 400%;
    animation: gradientFlow 3s ease infinite 1s;
}

.gauge-pulse .gauge-left { animation: pulse 2s ease-in-out infinite; }
.gauge-pulse .gauge-center { animation: pulse 2s ease-in-out infinite 0.3s; }
.gauge-pulse .gauge-right { animation: pulse 2s ease-in-out infinite 0.6s; }

.gauge-sparkle .gauge-left::before,
.gauge-sparkle .gauge-center::before,
.gauge-sparkle .gauge-right::before {
    content: '✨';
    position: absolute;
    top: 50%; left: 20%;
    transform: translateY(-50%);
    font-size: 12px;
    animation: sparkle 2s ease-in-out infinite;
}

.gauge-sparkle .gauge-center::before { left: 50%; animation-delay: 0.5s; }
.gauge-sparkle .gauge-right::before { left: 80%; animation-delay: 1s; }

.gauge-3d .gauge-bar {
    transform-style: preserve-3d;
    animation: rotate3d 6s ease-in-out infinite;
}

.gauge-3d .gauge-fill { transform-style: preserve-3d; }

.gauge-typewriter .gauge-left { animation: typewriter 2s steps(20) infinite; }
.gauge-typewriter .gauge-center { animation: typewriter 2s steps(20) infinite 0.5s; }
.gauge-typewriter .gauge-right { animation: typewriter 2s steps(20) infinite 1s; }

/* 호버 효과 */
.gauge-container:hover .gauge-bar {
    transform: scale(1.02);
    box-shadow: 0 6px 20px rgba(0,0,0,0.15);
}

.gauge-container:hover .gauge-fill {
    transform: scale(1.05);
}

/* 뷰 섹션 */
.view-section {
    margin-bottom: 20px;
}

.view-title {
    font-size: 16px;
    font-weight: 600;
    color: #1a1a1a;
    margin-bottom: 8px;
    display: inline-block;
    padding: 6px 12px;
    border-radius: 20px;
    font-size: 14px;
}

.view-title.left {
    background-color: rgba(25, 118, 210, 0.15);
    color: #1976d2;
}

.view-title.center {
    background-color: rgba(108, 117, 125, 0.08);
    color: #6c757d;
    padding: 6px 12px;
    border-radius: 20px;
}

.view-title.right {
    background-color: rgba(220, 53, 69, 0.15);
    color: #dc3545;
}

.view-content {
    font-size: 14px;
    color: #666666;
    line-height: 1.5;
}

/* 기타 유틸리티 */
.background-highlight {
    background: linear-gradient(120deg, #c8e6c9 0%, #c8e6c9 100%);
    background-size: 100% 0.4em;
    background-repeat: no-repeat;
    background-position: 0 85%;
    padding: 0 3px;
    font-weight: 500;
}

.no-content {
    color: #999999;
    font-style: italic;
    font-size: 14px;
}
"""
    
    def _generate_issue_card(self, issue: Dict[str, Any]) -> str:
        """개별 이슈 카드 HTML 생성"""
        # 게이지바 계산
        total = issue['total_articles']
        left_pct = (issue['left_articles'] / total * 100) if total > 0 else 0
        center_pct = (issue['center_articles'] / total * 100) if total > 0 else 0
        right_pct = (issue['right_articles'] / total * 100) if total > 0 else 0
        
        return f"""
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
            <div class="gauge-title">언론사 성향별 보도 비율</div>
            <div class="gauge-bar {self.animation_type}">
                <div class="gauge-fill" style="--target-width: 100%;">
                    <div class="gauge-left" style="width: {left_pct}%">
                        {f'<div class="gauge-percentage">{left_pct:.0f}%</div>' if left_pct > 5 else ''}
                    </div>
                    <div class="gauge-center" style="width: {center_pct}%">
                        {f'<div class="gauge-percentage">{center_pct:.0f}%</div>' if center_pct > 5 else ''}
                    </div>
                    <div class="gauge-right" style="width: {right_pct}%">
                        {f'<div class="gauge-percentage">{right_pct:.0f}%</div>' if right_pct > 5 else ''}
                    </div>
                </div>
            </div>
        </div>
        
        <div class="section">
            <div class="section-label">핵심 쟁점</div>
            <div class="section-content">{issue['summary']}</div>
        </div>
        
        {self._generate_view_sections(issue)}
    </div>
"""
    
    def _generate_view_sections(self, issue: Dict[str, Any]) -> str:
        """뷰 섹션들 생성"""
        views = [
            ("좌파 관점", issue['left_view'], "left"),
            ("우파 관점", issue['right_view'], "right"),
            ("중립 관점", issue['center_view'], "center")
        ]
        
        view_html = ""
        for title, content, bias_class in views:
            if content and content.strip():
                view_html += f"""
        <div class="view-section">
            <div class="view-title {bias_class}">{title}</div>
            <div class="view-content">{content}</div>
        </div>
"""
        return view_html
    
    def save_report(self, html: str, filename: str = None) -> str:
        """HTML 보고서를 파일로 저장"""
        if filename is None:
            filename = self.generate_filename()
        
        filepath = self.reports_dir / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html)
            
            console.print(f"✅ 보고서 생성 완료: {filename}")
            console.print(f"📁 저장 위치: {filepath}")
            return str(filepath)
            
        except Exception as e:
            console.print(f"❌ 보고서 저장 실패: {e}")
            return None
    
    def generate_report(self, count: int = None, animation_type: str = None) -> str:
        """전체 보고서 생성 프로세스"""
        if animation_type:
            self.animation_type = self.ANIMATION_TYPES.get(animation_type, "gauge-wave")
        
        console.print("🚀 정치 이슈 HTML 보고서 생성기 시작")
        
        # 데이터베이스 연결 확인
        if not self.supabase_manager.client:
            console.print("❌ Supabase 클라이언트 초기화 실패")
            return None
        
        console.print("✅ Supabase 클라이언트 초기화 완료")
        
        # 실제 데이터베이스에서 이슈 조회
        console.print("📊 실제 데이터베이스에서 모든 이슈를 조회하여 보고서 생성 중...")
        issues = self.get_real_issues(count)
        
        if not issues:
            console.print("❌ 이슈 데이터가 없어 보고서를 생성할 수 없습니다.")
            return None
        
        # HTML 생성
        html = self.generate_html(issues)
        
        # 파일 저장
        filename = self.save_report(html)
        
        if filename:
            console.print("🎉 보고서 생성 완료!")
            console.print(f"📱 모바일에서 확인해보세요: {filename}")
        
        return filename

def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="정치 이슈 HTML 보고서 생성기")
    parser.add_argument("--count", type=int, help="생성할 이슈 개수 (기본값: 모든 이슈)")
    parser.add_argument("--animation", choices=list(ReportGenerator.ANIMATION_TYPES.keys()), 
                       default="wave", help="게이지바 애니메이션 타입")
    
    args = parser.parse_args()
    
    generator = ReportGenerator(animation_type=args.animation)
    generator.generate_report(count=args.count, animation_type=args.animation)

if __name__ == "__main__":
    main()