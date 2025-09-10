#!/usr/bin/env python3
"""
정치 이슈 HTML 보고서 생성기 (리팩토링 버전)
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
        """초기화"""
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

.created-at {
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
    color: #666666;
    line-height: 1.6;
}

/* 소스 통계 */
.source-stats {
    display: flex;
    gap: 16px;
    margin-bottom: 24px;
    flex-wrap: wrap;
    padding: 16px;
    background: #f8f9fa;
    border-radius: 6px;
}

.source-item {
    text-align: center;
    flex: 1;
    min-width: 60px;
}

.source-number {
    font-size: 20px;
    font-weight: 700;
    color: #1a1a1a;
    margin-bottom: 4px;
}

.source-label {
    font-size: 12px;
    color: #666666;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

/* 게이지바 */
.gauge-container {
    margin-bottom: 24px;
    padding: 20px;
    background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
    border-radius: 12px;
    border: 1px solid #e9ecef;
}

.gauge-title {
    font-size: 16px;
    font-weight: 600;
    color: #1a1a1a;
    margin-bottom: 16px;
    text-align: center;
}

.gauge-bar {
    height: 32px;
    background: linear-gradient(90deg, #f1f3f4 0%, #e8eaed 100%);
    border-radius: 16px;
    overflow: hidden;
    position: relative;
    margin-bottom: 12px;
    box-shadow: inset 0 2px 4px rgba(0, 0, 0, 0.1);
}

.gauge-fill {
    height: 100%;
    display: flex;
    border-radius: 16px;
    overflow: hidden;
}

.gauge-left {
    background: linear-gradient(135deg, #1976d2 0%, #1565c0 100%);
    position: relative;
    box-shadow: 0 2px 4px rgba(25, 118, 210, 0.3);
}

.gauge-center {
    background: linear-gradient(135deg, #6c757d 0%, #5a6268 100%);
    position: relative;
    box-shadow: 0 2px 4px rgba(108, 117, 125, 0.3);
}

.gauge-right {
    background: linear-gradient(135deg, #dc3545 0%, #c82333 100%);
    position: relative;
    box-shadow: 0 2px 4px rgba(220, 53, 69, 0.3);
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
}

/* 애니메이션 */
@keyframes shimmer {
    0% { background-position: 0% 50%; }
    50% { background-position: 100% 50%; }
    100% { background-position: 0% 50%; }
}

@keyframes shimmerMove {
    0% { transform: translateX(-100%); }
    100% { transform: translateX(100%); }
}

@keyframes wave {
    0%, 100% { transform: scaleY(1); }
    50% { transform: scaleY(1.1); }
}

@keyframes gradientFlow {
    0% { background-position: 0% 50%; }
    50% { background-position: 100% 50%; }
    100% { background-position: 0% 50%; }
}

@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.7; }
}

@keyframes sparkle {
    0%, 100% { box-shadow: 0 0 5px rgba(255, 255, 255, 0.5); }
    50% { box-shadow: 0 0 20px rgba(255, 255, 255, 0.8), 0 0 30px rgba(255, 255, 255, 0.6); }
}

@keyframes rotate3d {
    0% { transform: rotateY(0deg); }
    50% { transform: rotateY(180deg); }
    100% { transform: rotateY(360deg); }
}

@keyframes typewriter {
    0% { width: 0; }
    100% { width: 100%; }
}

.gauge-wave .gauge-fill {
    animation: wave 2s ease-in-out infinite;
}

.gauge-flow .gauge-left {
    background: linear-gradient(45deg, #1976d2, #42a5f5, #1976d2, #1565c0);
    background-size: 400% 400%;
    animation: gradientFlow 3s ease infinite;
}

.gauge-flow .gauge-center {
    background: linear-gradient(45deg, #6c757d, #adb5bd, #6c757d, #5a6268);
    background-size: 400% 400%;
    animation: gradientFlow 3s ease infinite;
}

.gauge-flow .gauge-right {
    background: linear-gradient(45deg, #dc3545, #ff6b6b, #dc3545, #c82333);
    background-size: 400% 400%;
    animation: gradientFlow 3s ease infinite;
}

.gauge-pulse .gauge-fill {
    animation: pulse 2s ease-in-out infinite;
}

.gauge-sparkle .gauge-fill {
    animation: sparkle 2s ease-in-out infinite;
}

.gauge-3d .gauge-fill {
    animation: rotate3d 4s linear infinite;
    transform-style: preserve-3d;
}

.gauge-typewriter .gauge-fill {
    animation: typewriter 3s ease-in-out infinite;
    overflow: hidden;
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
.no-content {
    color: #999999;
    font-style: italic;
    font-size: 14px;
}

.background-highlight {
    background-color: transparent;
    padding: 0px 2px;
    border-bottom: 5px solid rgba(76, 175, 80, 0.3);
    font-weight: 500;
}

.view-highlight {
    background-color: transparent;
    padding: 0px 2px;
    border-bottom: 5px solid rgba(76, 175, 80, 0.3);
    font-weight: 500;
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
    
    .source-stats {
        gap: 12px;
    }
    
    .gauge-container {
        padding: 16px;
    }
}
"""
    
    def _highlight_last_sentence(self, text: str) -> str:
        """마지막 문장에 하이라이트 적용"""
        if not text or not text.strip():
            return text
        
        # 문장을 마침표로 분리하고 빈 문장 제거
        sentences = [s.strip() for s in text.split('.') if s.strip()]
        
        if len(sentences) <= 1:
            # 문장이 하나뿐이면 전체를 하이라이트
            return f"<span class='background-highlight'>{text.strip()}</span>"
        
        # 마지막 문장을 제외한 부분
        first_part = '. '.join(sentences[:-1])
        last_sentence = sentences[-1]
        
        return f"{first_part}. <span class='background-highlight'>{last_sentence}</span>"
    
    def _generate_gauge_bar(self, stats: Dict[str, int]) -> str:
        """게이지바 HTML 생성"""
        total = stats.get('total', 0)
        if total == 0:
            return '<div class="gauge-bar"><div class="gauge-fill"></div></div>'
        
        left_pct = (stats.get('left', 0) / total) * 100
        center_pct = (stats.get('center', 0) / total) * 100
        right_pct = (stats.get('right', 0) / total) * 100
        
        gauge_html = f'<div class="gauge-bar {self.animation_type}">'
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
    
    def _generate_issue_card(self, issue: Dict[str, Any]) -> str:
        """이슈 카드 HTML 생성"""
        stats = self._get_article_stats(issue['id'])
        gauge_bar = self._generate_gauge_bar(stats)
        
        return f"""
    <div class="issue-card">
        <div class="issue-header">
            <div class="created-at">{issue['created_at']}</div>
            <div class="title">{issue['title']}</div>
            <div class="subtitle">{issue['subtitle']}</div>
        </div>
        
        <div class="section">
            <div class="section-label">배경 정보</div>
            <div class="section-content">{self._highlight_last_sentence(issue['background'])}</div>
        </div>
        
        <div class="source-stats">
            <div class="source-item">
                <div class="source-number">{issue['total_articles']}</div>
                <div class="source-label">전체</div>
            </div>
            <div class="source-item">
                <div class="source-number">{stats['left']}</div>
                <div class="source-label">좌파</div>
            </div>
            <div class="source-item">
                <div class="source-number">{stats['center']}</div>
                <div class="source-label">중립</div>
            </div>
            <div class="source-item">
                <div class="source-number">{stats['right']}</div>
                <div class="source-label">우파</div>
            </div>
        </div>
        
        <div class="gauge-container">
            <div class="gauge-title">언론사 성향별 보도 비율</div>
            {gauge_bar}
        </div>
        
        
        {self._generate_view_sections(issue)}
    </div>
"""
    
    def _highlight_first_sentence(self, text: str) -> str:
        """첫 번째 문장에 하이라이트 적용"""
        if not text or not text.strip():
            return text
        
        # 문장을 마침표로 분리하고 빈 문장 제거
        sentences = [s.strip() for s in text.split('.') if s.strip()]
        
        if len(sentences) <= 1:
            # 문장이 하나뿐이면 전체를 하이라이트
            return f"<span class='view-highlight'>{text.strip()}</span>"
        
        # 첫 번째 문장을 하이라이트하고 나머지는 그대로
        first_sentence = sentences[0]
        rest_sentences = '. '.join(sentences[1:])
        
        return f"<span class='view-highlight'>{first_sentence}</span>. {rest_sentences}"
    
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
                # 좌파와 우파 관점에만 첫 번째 문장 하이라이트 적용
                if bias_class in ["left", "right"]:
                    highlighted_content = self._highlight_first_sentence(content)
                else:
                    highlighted_content = content
                
                view_html += f"""
        <div class="view-section">
            <div class="view-title {bias_class}">{title}</div>
            <div class="view-content">{highlighted_content}</div>
        </div>
"""
        return view_html
    
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
                'id, title, subtitle, background, summary, left_view, center_view, right_view, created_at'
            ).order('created_at', desc=True).execute()
            
            if not result.data:
                console.print("❌ 이슈 데이터가 없습니다.")
                return None
            
            console.print(f"✅ {len(result.data)}개 이슈 데이터 조회 완료")
            
            # 각 이슈에 통계 정보 추가
            for issue in result.data:
                stats = self._get_article_stats(issue['id'])
                issue['total_articles'] = stats['total']
            
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
    
    {''.join([self._generate_issue_card(issue) for issue in result.data])}
    
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
    import argparse
    
    parser = argparse.ArgumentParser(description='정치 이슈 HTML 보고서 생성기')
    parser.add_argument('--animation', choices=['wave', 'flow', 'pulse', 'sparkle', '3d', 'typewriter'], 
                       default='wave', help='게이지바 애니메이션 타입')
    
    args = parser.parse_args()
    
    try:
        generator = ReportGenerator(animation_type=args.animation)
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
