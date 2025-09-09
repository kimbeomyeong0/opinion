#!/usr/bin/env python3
"""
모던 디자인 HTML 리포트 생성기
깔끔한 디자인과 동적 애니메이션 포함
"""

import sys
import os
from datetime import datetime
from rich.console import Console

# 프로젝트 루트를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.supabase_manager import get_supabase_client

console = Console()

class ModernReportGenerator:
    """모던 디자인 HTML 리포트 생성기"""
    
    def __init__(self):
        self.supabase = get_supabase_client()
        if not self.supabase.client:
            raise ValueError("Supabase 연결 실패")
    
    def get_issues_data(self):
        """Issues 테이블에서 데이터 조회 (기사 수 순으로 정렬)"""
        try:
            result = self.supabase.client.table('issues')\
                .select('id, title, subtitle, summary, left_source, center_source, right_source, left_view, center_view, right_view, created_at, timeline, why, history')\
                .execute()
            
            if not result.data:
                return []
            
            issues = result.data
            
            # 기사 수가 많은 순서대로 정렬
            def get_total_source_count(issue):
                left_count = int(issue.get('left_source', 0)) if issue.get('left_source') else 0
                center_count = int(issue.get('center_source', 0)) if issue.get('center_source') else 0
                right_count = int(issue.get('right_source', 0)) if issue.get('right_source') else 0
                return left_count + center_count + right_count
            
            issues.sort(key=get_total_source_count, reverse=True)
            return issues
            
        except Exception as e:
            print(f"❌ 데이터 조회 실패: {str(e)}")
            return []
    
    def format_date(self, date_str):
        """날짜 포맷팅"""
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime('%Y년 %m월 %d일')
        except:
            return date_str
    
    def format_view_content(self, content):
        """관점 내용 포맷팅"""
        if not content:
            return "관점이 생성되지 않았습니다."
        
        # 줄바꿈 처리
        content = content.replace('\n', '<br>')
        return content
    
    def _generate_bias_gauge_html(self, left_source, center_source, right_source):
        """성향별 기사 수 게이지 HTML 생성"""
        # 문자열을 정수로 변환
        left_count = int(left_source) if left_source else 0
        center_count = int(center_source) if center_source else 0
        right_count = int(right_source) if right_source else 0
        
        total = left_count + center_count + right_count
        if total == 0:
            return ""
        
        left_percent = (left_count / total) * 100
        center_percent = (center_count / total) * 100
        right_percent = (right_count / total) * 100
        
        return f"""
        <div class="bias-gauge">
            <div class="gauge-title">📊 언론사별 기사 수 분포</div>
            <div class="gauge-container">
                <div class="gauge-bar">
                    <div class="gauge-fill">
                        <div class="gauge-left" style="width: {left_percent:.1f}%"></div>
                        <div class="gauge-center" style="width: {center_percent:.1f}%"></div>
                        <div class="gauge-right" style="width: {right_percent:.1f}%"></div>
                    </div>
                </div>
            </div>
            <div class="gauge-labels">
                <div class="gauge-label">
                    <div class="gauge-dot left"></div>
                    <span>진보 {left_count}개 ({left_percent:.1f}%)</span>
                </div>
                <div class="gauge-label">
                    <div class="gauge-dot center"></div>
                    <span>중도 {center_count}개 ({center_percent:.1f}%)</span>
                </div>
                <div class="gauge-label">
                    <div class="gauge-dot right"></div>
                    <span>보수 {right_count}개 ({right_percent:.1f}%)</span>
                </div>
            </div>
        </div>
        """
    
    def _generate_view_html(self, bias, title, source_count, view_content):
        """개별 관점 HTML 생성"""
        if not view_content:
            return f"""
            <div class="view {bias}">
                <div class="view-header">
                    <div class="view-title">{title}</div>
                    <div class="view-source">기사 {source_count}개</div>
                </div>
                <div class="no-views">관점이 생성되지 않았습니다.</div>
            </div>
            """
        
        return f"""
        <div class="view {bias}">
            <div class="view-header">
                <div class="view-title">{title}</div>
                <div class="view-source">기사 {source_count}개</div>
            </div>
            <div class="view-content">
                {self.format_view_content(view_content)}
            </div>
        </div>
        """
    
    def generate_issues_html(self):
        """이슈들 HTML 생성"""
        issues = self.get_issues_data()
        
        if not issues:
            return "<div class='no-issues'>이슈 데이터가 없습니다.</div>"
        
        html_parts = []
        
        for i, issue in enumerate(issues):
            issue_id = issue['id']
            title = issue.get('title', '제목 없음')
            subtitle = issue.get('subtitle', '')
            summary = issue.get('summary', '')
            why = issue.get('why', '')
            history = issue.get('history', '')
            timeline = issue.get('timeline', '')
            created_at = issue.get('created_at', '')
            
            left_source = issue.get('left_source', 0)
            center_source = issue.get('center_source', 0)
            right_source = issue.get('right_source', 0)
            
            left_view = issue.get('left_view', '')
            center_view = issue.get('center_view', '')
            right_view = issue.get('right_view', '')
            
            # 이슈 HTML 생성
            issue_html = f"""
            <div class="issue" style="animation-delay: {i * 0.1}s;">
                <div class="issue-content">
                    <div class="issue-title">{title}</div>
                    {f'<div class="issue-subtitle">{subtitle}</div>' if subtitle else ''}
                    {f'<div class="issue-summary">{summary}</div>' if summary else ''}
                    {f'<div class="issue-why"><strong>왜 이 이슈가 중요한가?</strong><br>{why}</div>' if why else ''}
                    {f'<div class="issue-history"><strong>이슈의 배경과 역사</strong><br>{history}</div>' if history else ''}
                    {f'<div class="issue-timeline"><strong>주요 일정과 흐름</strong><br>{timeline}</div>' if timeline else ''}
                    
                    {self._generate_bias_gauge_html(left_source, center_source, right_source)}
                    
                    <div class="views-container">
                        {self._generate_view_html('left', '진보적 관점', left_source, left_view)}
                        {self._generate_view_html('center', '중도적 관점', center_source, center_view)}
                        {self._generate_view_html('right', '보수적 관점', right_source, right_view)}
                    </div>
                    
                    <div class="issue-meta">
                        생성일: {self.format_date(created_at)} | ID: {issue_id[:8]}
                    </div>
                </div>
            </div>
            """
            
            html_parts.append(issue_html)
        
        return '\n'.join(html_parts)
    
    def generate_html(self, output_file='modern_issues.html'):
        """HTML 파일 생성"""
        issues_html = self.generate_issues_html()
        
        html_content = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>성향별 관점 분석 - 모던 디자인</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            color: #333;
            overflow-x: hidden;
            margin: 0;
            padding: 0;
        }}
        
        /* 네비게이션 바 */
        .navbar {{
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(20px);
            padding: 1rem 0;
            position: sticky;
            top: 0;
            z-index: 1000;
            box-shadow: 0 2px 20px rgba(0,0,0,0.1);
        }}
        
        .nav-container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 0 2rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        
        .logo {{
            font-size: 1.5rem;
            font-weight: 800;
            background: linear-gradient(135deg, #667eea, #764ba2);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}
        
        .nav-menu {{
            display: flex;
            list-style: none;
            gap: 2rem;
            margin: 0;
            padding: 0;
        }}
        
        .nav-item a {{
            text-decoration: none;
            color: #333;
            font-weight: 500;
            transition: color 0.3s ease;
            position: relative;
        }}
        
        .nav-item a:hover {{
            color: #667eea;
        }}
        
        .nav-item a::after {{
            content: '';
            position: absolute;
            bottom: -5px;
            left: 0;
            width: 0;
            height: 2px;
            background: linear-gradient(135deg, #667eea, #764ba2);
            transition: width 0.3s ease;
        }}
        
        .nav-item a:hover::after {{
            width: 100%;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }}
        
        .header {{
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(20px);
            border-radius: 20px;
            padding: 3rem;
            text-align: center;
            margin-bottom: 3rem;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            border: 1px solid rgba(255, 255, 255, 0.2);
            position: relative;
            overflow: hidden;
        }}
        
        .header::before {{
            content: '';
            position: absolute;
            top: -50%;
            left: -50%;
            width: 200%;
            height: 200%;
            background: radial-gradient(circle, rgba(102, 126, 234, 0.1) 0%, transparent 70%);
            animation: rotate 20s linear infinite;
        }}
        
        .header-content {{
            position: relative;
            z-index: 1;
        }}
        
        .header h1 {{
            font-size: 3.5rem;
            font-weight: 800;
            background: linear-gradient(135deg, #667eea, #764ba2);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 1rem;
            animation: fadeInUp 1s ease-out;
        }}
        
        .header p {{
            font-size: 1.3rem;
            color: #666;
            margin-bottom: 2rem;
            animation: fadeInUp 1s ease-out 0.2s both;
        }}
        
        .stats {{
            display: flex;
            justify-content: center;
            gap: 3rem;
            margin-top: 2rem;
            animation: fadeInUp 1s ease-out 0.4s both;
        }}
        
        .stat-item {{
            text-align: center;
        }}
        
        .stat-number {{
            font-size: 2.5rem;
            font-weight: 700;
            color: #667eea;
            display: block;
        }}
        
        .stat-label {{
            font-size: 0.9rem;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        
        .content {{
            display: grid;
            gap: 4rem;
            margin-top: 2rem;
        }}
        
        .issue {{
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(20px);
            border-radius: 24px;
            padding: 3rem;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            border: 1px solid rgba(255, 255, 255, 0.2);
            transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
            overflow: hidden;
            animation: slideUp 0.6s ease-out both;
        }}
        
        .issue::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 5px;
            background: linear-gradient(90deg, #667eea, #764ba2);
            transform: scaleX(0);
            transition: transform 0.3s ease;
        }}
        
        .issue::after {{
            content: '';
            position: absolute;
            top: -50%;
            right: -50%;
            width: 200%;
            height: 200%;
            background: radial-gradient(circle, rgba(102, 126, 234, 0.05) 0%, transparent 70%);
            animation: rotate 30s linear infinite;
        }}
        
        .issue:hover {{
            transform: translateY(-10px) scale(1.02);
            box-shadow: 0 30px 60px rgba(0,0,0,0.15);
        }}
        
        .issue:hover::before {{
            transform: scaleX(1);
        }}
        
        .issue-content {{
            position: relative;
            z-index: 1;
        }}
        
        .issue-title {{
            font-size: 2.5rem;
            font-weight: 700;
            color: #2c3e50;
            margin-bottom: 1.5rem;
            line-height: 1.2;
            transition: color 0.3s ease;
        }}
        
        .issue:hover .issue-title {{
            color: #667eea;
        }}
        
        .issue-subtitle {{
            font-size: 1.3rem;
            color: #7f8c8d;
            margin-bottom: 2rem;
            font-weight: 500;
            line-height: 1.4;
        }}
        
        .issue-summary {{
            font-size: 1.1rem;
            color: #555;
            margin-bottom: 2.5rem;
            line-height: 1.8;
            padding: 2rem;
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 16px;
            border-left: 5px solid #667eea;
            position: relative;
            overflow: hidden;
        }}
        
        .issue-summary::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: linear-gradient(45deg, transparent 30%, rgba(255,255,255,0.3) 50%, transparent 70%);
            transform: translateX(-100%);
            transition: transform 0.6s ease;
        }}
        
        .issue-summary:hover::before {{
            transform: translateX(100%);
        }}
        
        .bias-gauge {{
            margin: 3rem 0;
            padding: 2rem;
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 16px;
            border: 1px solid rgba(0,0,0,0.05);
            position: relative;
            overflow: hidden;
        }}
        
        .bias-gauge::before {{
            content: '';
            position: absolute;
            top: -50%;
            left: -50%;
            width: 200%;
            height: 200%;
            background: radial-gradient(circle, rgba(102, 126, 234, 0.1) 0%, transparent 70%);
            animation: rotate 20s linear infinite;
        }}
        
        .gauge-title {{
            font-size: 1.3rem;
            font-weight: 700;
            margin-bottom: 1.5rem;
            color: #333;
            position: relative;
            z-index: 1;
        }}
        
        .gauge-container {{
            display: flex;
            align-items: center;
            gap: 1rem;
            position: relative;
            z-index: 1;
        }}
        
        .gauge-bar {{
            flex: 1;
            height: 30px;
            background: rgba(0,0,0,0.1);
            border-radius: 15px;
            overflow: hidden;
            position: relative;
            box-shadow: inset 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        .gauge-fill {{
            height: 100%;
            display: flex;
            transition: all 1s cubic-bezier(0.4, 0, 0.2, 1);
            animation: fillGauge 1.5s ease-out;
        }}
        
        .gauge-left {{
            background: linear-gradient(90deg, #1971c2, #339af0);
            box-shadow: 0 2px 8px rgba(25, 113, 194, 0.3);
        }}
        
        .gauge-center {{
            background: linear-gradient(90deg, #fab005, #ffd43b);
            box-shadow: 0 2px 8px rgba(250, 176, 5, 0.3);
        }}
        
        .gauge-right {{
            background: linear-gradient(90deg, #c92a2a, #ff6b6b);
            box-shadow: 0 2px 8px rgba(201, 42, 42, 0.3);
        }}
        
        .gauge-labels {{
            display: flex;
            justify-content: space-between;
            margin-top: 1.5rem;
            font-size: 0.9rem;
            color: #666;
            position: relative;
            z-index: 1;
        }}
        
        .gauge-label {{
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 8px 12px;
            background: rgba(255,255,255,0.7);
            border-radius: 20px;
            transition: all 0.3s ease;
        }}
        
        .gauge-label:hover {{
            background: rgba(255,255,255,0.9);
            transform: translateY(-2px);
        }}
        
        .gauge-dot {{
            width: 10px;
            height: 10px;
            border-radius: 50%;
            animation: pulse 2s infinite;
        }}
        
        .gauge-dot.left {{
            background: linear-gradient(45deg, #1971c2, #339af0);
        }}
        
        .gauge-dot.center {{
            background: linear-gradient(45deg, #fab005, #ffd43b);
        }}
        
        .gauge-dot.right {{
            background: linear-gradient(45deg, #c92a2a, #ff6b6b);
        }}
        
        .views-container {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
            gap: 2rem;
            margin: 3rem 0;
        }}
        
        .view {{
            padding: 2rem;
            border-radius: 16px;
            border: 2px solid transparent;
            transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
            overflow: hidden;
            animation: slideInUp 0.6s ease-out;
        }}
        
        .view::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(45deg, transparent 30%, rgba(255,255,255,0.1) 50%, transparent 70%);
            transform: translateX(-100%);
            transition: transform 0.6s ease;
        }}
        
        .view:hover::before {{
            transform: translateX(100%);
        }}
        
        .view.left {{
            background: linear-gradient(135deg, #f0f8ff, #e3f2fd);
            border-color: #4dabf7;
        }}
        
        .view.center {{
            background: linear-gradient(135deg, #fffbf0, #fff8e1);
            border-color: #ffd93d;
        }}
        
        .view.right {{
            background: linear-gradient(135deg, #fff5f5, #ffebee);
            border-color: #ff6b6b;
        }}
        
        .view:hover {{
            transform: translateY(-5px) scale(1.02);
            box-shadow: 0 15px 35px rgba(0,0,0,0.1);
        }}
        
        .view-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.5rem;
            position: relative;
            z-index: 1;
        }}
        
        .view-title {{
            font-weight: 700;
            font-size: 1.2rem;
            position: relative;
            z-index: 1;
        }}
        
        .view.left .view-title {{
            color: #1971c2;
        }}
        
        .view.center .view-title {{
            color: #fab005;
        }}
        
        .view.right .view-title {{
            color: #c92a2a;
        }}
        
        .view-source {{
            font-size: 0.9rem;
            color: #666;
            background: rgba(255,255,255,0.8);
            padding: 6px 12px;
            border-radius: 20px;
            font-weight: 600;
            position: relative;
            z-index: 1;
        }}
        
        .view-content {{
            font-size: 1rem;
            line-height: 1.7;
            position: relative;
            z-index: 1;
        }}
        
        .no-views {{
            color: #999;
            font-style: italic;
            text-align: center;
            padding: 2rem;
            position: relative;
            z-index: 1;
        }}
        
        .issue-meta {{
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid rgba(0,0,0,0.1);
            font-size: 0.9rem;
            color: #666;
            text-align: center;
        }}
        
        /* 애니메이션 */
        @keyframes slideDown {{
            from {{
                opacity: 0;
                transform: translateY(-50px);
            }}
            to {{
                opacity: 1;
                transform: translateY(0);
            }}
        }}
        
        @keyframes fadeInUp {{
            from {{
                opacity: 0;
                transform: translateY(30px);
            }}
            to {{
                opacity: 1;
                transform: translateY(0);
            }}
        }}
        
        @keyframes slideUp {{
            from {{
                opacity: 0;
                transform: translateY(50px);
            }}
            to {{
                opacity: 1;
                transform: translateY(0);
            }}
        }}
        
        @keyframes slideInUp {{
            from {{
                opacity: 0;
                transform: translateY(30px);
            }}
            to {{
                opacity: 1;
                transform: translateY(0);
            }}
        }}
        
        @keyframes fillGauge {{
            from {{
                width: 0%;
            }}
            to {{
                width: 100%;
            }}
        }}
        
        @keyframes pulse {{
            0%, 100% {{
                transform: scale(1);
            }}
            50% {{
                transform: scale(1.1);
            }}
        }}
        
        @keyframes rotate {{
            from {{
                transform: rotate(0deg);
            }}
            to {{
                transform: rotate(360deg);
            }}
        }}
        
        /* 스크롤 애니메이션 */
        .issue {{
            opacity: 0;
            transform: translateY(50px);
            animation: slideUp 0.6s ease-out both;
        }}
        
        .issue:nth-child(1) {{ animation-delay: 0.1s; }}
        .issue:nth-child(2) {{ animation-delay: 0.2s; }}
        .issue:nth-child(3) {{ animation-delay: 0.3s; }}
        .issue:nth-child(4) {{ animation-delay: 0.4s; }}
        .issue:nth-child(5) {{ animation-delay: 0.5s; }}
        .issue:nth-child(6) {{ animation-delay: 0.6s; }}
        
        /* 반응형 디자인 */
        @media (max-width: 768px) {{
            .container {{
                padding: 10px;
            }}
            
            .header {{
                padding: 30px 20px;
            }}
            
            .header h1 {{
                font-size: 2.2rem;
            }}
            
            .issue {{
                padding: 25px 20px;
            }}
            
            .views-container {{
                grid-template-columns: 1fr;
                gap: 20px;
            }}
            
            .gauge-labels {{
                flex-direction: column;
                gap: 10px;
            }}
            
            .gauge-label {{
                justify-content: center;
            }}
        }}
    </style>
</head>
    <body>
    <!-- 네비게이션 바 -->
    <nav class="navbar">
        <div class="nav-container">
            <div class="logo">📊 OpinionAI</div>
            <ul class="nav-menu">
                <li class="nav-item"><a href="#home">홈</a></li>
                <li class="nav-item"><a href="#issues">이슈</a></li>
                <li class="nav-item"><a href="#analysis">분석</a></li>
                <li class="nav-item"><a href="#about">소개</a></li>
            </ul>
        </div>
    </nav>
    
    <div class="container">
        <div class="header" id="home">
            <div class="header-content">
                <h1>📊 정치 이슈 분석 리포트</h1>
                <p>AI가 분석한 주요 정치 이슈와 성향별 관점을 한눈에 확인하세요</p>
                <div class="stats">
                    <div class="stat-item">
                        <span class="stat-number" id="total-issues">-</span>
                        <span class="stat-label">총 이슈</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-number" id="total-articles">-</span>
                        <span class="stat-label">총 기사</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-number" id="analyzed-issues">-</span>
                        <span class="stat-label">분석 완료</span>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="content" id="issues">
            {issues_html}
        </div>
    </div>
    
    <!-- 푸터 -->
    <footer style="background: rgba(0,0,0,0.8); color: white; padding: 3rem 0; margin-top: 4rem; text-align: center;">
        <div style="max-width: 1400px; margin: 0 auto; padding: 0 2rem;">
            <h3 style="margin-bottom: 1rem; background: linear-gradient(135deg, #667eea, #764ba2); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;">OpinionAI</h3>
            <p style="margin-bottom: 2rem; opacity: 0.8;">AI 기반 정치 이슈 분석 플랫폼</p>
            <div style="display: flex; justify-content: center; gap: 2rem; margin-bottom: 2rem;">
                <a href="#" style="color: white; text-decoration: none; opacity: 0.8; transition: opacity 0.3s ease;">개인정보처리방침</a>
                <a href="#" style="color: white; text-decoration: none; opacity: 0.8; transition: opacity 0.3s ease;">이용약관</a>
                <a href="#" style="color: white; text-decoration: none; opacity: 0.8; transition: opacity 0.3s ease;">문의하기</a>
            </div>
            <p style="opacity: 0.6; font-size: 0.9rem;">© 2024 OpinionAI. All rights reserved.</p>
        </div>
    </footer>
    
    <script>
        // 스크롤 애니메이션
        const observerOptions = {{
            threshold: 0.1,
            rootMargin: '0px 0px -50px 0px'
        }};
        
        const observer = new IntersectionObserver((entries) => {{
            entries.forEach(entry => {{
                if (entry.isIntersecting) {{
                    entry.target.style.opacity = '1';
                    entry.target.style.transform = 'translateY(0)';
                }}
            }});
        }}, observerOptions);
        
        // 모든 이슈 요소 관찰
        document.querySelectorAll('.issue').forEach(issue => {{
            observer.observe(issue);
        }});
        
        // 게이지 애니메이션
        document.querySelectorAll('.gauge-fill').forEach(gauge => {{
            const leftBar = gauge.querySelector('.gauge-left');
            const centerBar = gauge.querySelector('.gauge-center');
            const rightBar = gauge.querySelector('.gauge-right');
            
            if (leftBar) {{
                const width = leftBar.style.width;
                leftBar.style.width = '0%';
                setTimeout(() => {{
                    leftBar.style.width = width;
                }}, 500);
            }}
            
            if (centerBar) {{
                const width = centerBar.style.width;
                centerBar.style.width = '0%';
                setTimeout(() => {{
                    centerBar.style.width = width;
                }}, 700);
            }}
            
            if (rightBar) {{
                const width = rightBar.style.width;
                rightBar.style.width = '0%';
                setTimeout(() => {{
                    rightBar.style.width = width;
                }}, 900);
            }}
        }});
        
        // 부드러운 스크롤
        document.querySelectorAll('a[href^="#"]').forEach(anchor => {{
            anchor.addEventListener('click', function (e) {{
                e.preventDefault();
                const target = document.querySelector(this.getAttribute('href'));
                if (target) {{
                    target.scrollIntoView({{
                        behavior: 'smooth',
                        block: 'start'
                    }});
                }}
            }});
        }});
        
        // 카드 호버 효과 강화
        document.querySelectorAll('.issue').forEach(issue => {{
            issue.addEventListener('mouseenter', function() {{
                this.style.transform = 'translateY(-10px) scale(1.02)';
            }});
            
            issue.addEventListener('mouseleave', function() {{
                this.style.transform = 'translateY(0) scale(1)';
            }});
        }});
        
        // 뷰 카드 호버 효과
        document.querySelectorAll('.view').forEach(view => {{
            view.addEventListener('mouseenter', function() {{
                this.style.transform = 'translateY(-5px) scale(1.02)';
            }});
            
            view.addEventListener('mouseleave', function() {{
                this.style.transform = 'translateY(0) scale(1)';
            }});
        }});
        
        // 통계 계산 및 표시
        function updateStats() {{
            const issues = document.querySelectorAll('.issue');
            const totalIssues = issues.length;
            
            let totalArticles = 0;
            let analyzedIssues = 0;
            
            issues.forEach(issue => {{
                const views = issue.querySelectorAll('.view');
                if (views.length > 0) {{
                    analyzedIssues++;
                }}
                
                // 게이지에서 기사 수 추출 (간단한 방법)
                const gaugeLabels = issue.querySelectorAll('.gauge-label');
                gaugeLabels.forEach(label => {{
                    const text = label.textContent;
                    const match = text.match(/(\d+)/);
                    if (match) {{
                        totalArticles += parseInt(match[1]);
                    }}
                }});
            }});
            
            document.getElementById('total-issues').textContent = totalIssues;
            document.getElementById('total-articles').textContent = totalArticles;
            document.getElementById('analyzed-issues').textContent = analyzedIssues;
        }}
        
        // 페이지 로드 시 통계 업데이트
        updateStats();
    </script>
</body>
</html>
        """
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            console.print(f"✅ HTML 리포트 생성 완료: {output_file}")
            return True
        except Exception as e:
            console.print(f"❌ HTML 생성 실패: {str(e)}")
            return False

def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='모던 디자인 HTML 리포트 생성기')
    parser.add_argument('html', nargs='?', default='modern_issues.html', help='출력 HTML 파일명')
    
    args = parser.parse_args()
    
    try:
        generator = ModernReportGenerator()
        generator.generate_html(args.html)
    except Exception as e:
        console.print(f"❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()
