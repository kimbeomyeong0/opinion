#!/usr/bin/env python3
"""
Issues 테이블 데이터를 HTML로 출력하는 제너레이터
미니멀한 디자인 + 성향별 포인트 컬러 적용
"""

import sys
import os
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_manager import get_supabase_client

class HTMLGenerator:
    def __init__(self):
        self.supabase = get_supabase_client()
        if not self.supabase.client:
            raise ValueError("Supabase 연결 실패")
    
    def get_issues_data(self):
        """Issues 테이블에서 데이터 조회"""
        try:
            result = self.supabase.client.table('issues')\
                .select('id, title, subtitle, summary, left_source, center_source, right_source, left_view, center_view, right_view, created_at')\
                .order('created_at', desc=True)\
                .execute()
            
            return result.data if result.data else []
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
    
    def format_view_content(self, view_content):
        """View 내용을 HTML로 포맷팅"""
        if not view_content:
            return "관점이 생성되지 않았습니다."
        
        # 줄바꿈을 <br>로 변환하고 문장을 <p>로 변환
        lines = view_content.split('\n')
        formatted_lines = []
        
        for line in lines:
            line = line.strip()
            if line.startswith('- '):
                # 불렛 포인트를 문장으로 변환 (하이픈 제거)
                content = line[2:].strip()
                formatted_lines.append(f'<p>• {content}</p>')
            elif line:
                # 일반 문장
                formatted_lines.append(f'<p>• {line}</p>')
        
        return '\n'.join(formatted_lines)
    
    def generate_html(self, output_file='issues.html'):
        """HTML 파일 생성"""
        issues = self.get_issues_data()
        
        if not issues:
            print("❌ 이슈 데이터가 없습니다.")
            return
        
        html_content = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>성향별 관점 분석</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            background-color: #fafafa;
        }}
        
        .container {{
            max-width: 800px;
            margin: 0 auto;
            padding: 40px 20px;
        }}
        
        .header {{
            text-align: center;
            margin-bottom: 60px;
        }}
        
        .header h1 {{
            font-size: 2.5rem;
            font-weight: 700;
            color: #1a1a1a;
            margin-bottom: 10px;
        }}
        
        .header p {{
            font-size: 1.1rem;
            color: #666;
        }}
        
        .issue-card {{
            background: white;
            border-radius: 12px;
            box-shadow: 0 2px 20px rgba(0, 0, 0, 0.08);
            margin-bottom: 40px;
            overflow: hidden;
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }}
        
        .issue-card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 30px rgba(0, 0, 0, 0.12);
        }}
        
        .issue-header {{
            padding: 30px;
            border-bottom: 1px solid #f0f0f0;
        }}
        
        .issue-title {{
            font-size: 1.8rem;
            font-weight: 600;
            color: #1a1a1a;
            margin-bottom: 8px;
            line-height: 1.3;
        }}
        
        .issue-subtitle {{
            font-size: 1.1rem;
            color: #666;
            margin-bottom: 15px;
        }}
        
        .issue-summary {{
            font-size: 1rem;
            color: #555;
            line-height: 1.6;
        }}
        
        .issue-meta {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 20px;
            padding-top: 20px;
            border-top: 1px solid #f0f0f0;
        }}
        
        .issue-date {{
            font-size: 0.9rem;
            color: #888;
        }}
        
        .source-counts {{
            display: flex;
            gap: 15px;
        }}
        
        .source-count {{
            font-size: 0.9rem;
            color: #666;
        }}
        
        .views-container {{
            display: grid;
            grid-template-columns: 1fr;
            gap: 0;
        }}
        
        .view-section {{
            padding: 25px 30px;
            border-bottom: 1px solid #f0f0f0;
        }}
        
        .view-section:last-child {{
            border-bottom: none;
        }}
        
        .view-header {{
            display: flex;
            align-items: center;
            margin-bottom: 20px;
        }}
        
        .view-title {{
            font-size: 1.2rem;
            font-weight: 600;
            margin-left: 10px;
        }}
        
        .view-title.no-bullet {{
            margin-left: 0;
        }}
        
        .view-content {{
            font-size: 1rem;
            line-height: 1.7;
        }}
        
        .view-content p {{
            margin-bottom: 15px;
        }}
        
        .view-content p:last-child {{
            margin-bottom: 0;
        }}
        
        /* 성향별 포인트 컬러 */
        .left-view {{
            border-left: 4px solid #3498db;
        }}
        
        .left-view .view-title {{
            color: #3498db;
        }}
        
        .center-view {{
            border-left: 4px solid #f1c40f;
        }}
        
        .center-view .view-title {{
            color: #f1c40f;
        }}
        
        .right-view {{
            border-left: 4px solid #e74c3c;
        }}
        
        .right-view .view-title {{
            color: #e74c3c;
        }}
        
        .view-icon {{
            width: 8px;
            height: 8px;
            border-radius: 50%;
        }}
        
        .left-icon {{
            background-color: #3498db;
        }}
        
        .center-icon {{
            background-color: #f1c40f;
        }}
        
        .right-icon {{
            background-color: #e74c3c;
        }}
        
        .no-view {{
            color: #999;
            font-style: italic;
        }}
        
        @media (max-width: 768px) {{
            .container {{
                padding: 20px 15px;
            }}
            
            .issue-header {{
                padding: 20px;
            }}
            
            .issue-title {{
                font-size: 1.5rem;
            }}
            
            .view-section {{
                padding: 20px;
            }}
            
            .source-counts {{
                flex-direction: column;
                gap: 5px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>성향별 관점 분석</h1>
            <p>다양한 시각에서 바라본 이슈 분석</p>
        </div>
        
        {self.generate_issues_html(issues)}
    </div>
</body>
</html>"""
        
        # HTML 파일 저장
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ HTML 파일 생성 완료: {output_file}")
        return output_file
    
    def generate_issues_html(self, issues):
        """이슈들 HTML 생성"""
        html_parts = []
        
        for issue in issues:
            html_parts.append(self.generate_issue_html(issue))
        
        return '\n'.join(html_parts)
    
    def generate_issue_html(self, issue):
        """개별 이슈 HTML 생성"""
        issue_id = issue.get('id', '')
        title = issue.get('title', '')
        subtitle = issue.get('subtitle', '')
        summary = issue.get('summary', '')
        created_at = issue.get('created_at', '')
        
        # 소스 카운트
        left_source = issue.get('left_source', '0')
        center_source = issue.get('center_source', '0')
        right_source = issue.get('right_source', '0')
        
        # View 데이터
        left_view = issue.get('left_view', '')
        center_view = issue.get('center_view', '')
        right_view = issue.get('right_view', '')
        
        return f"""
        <div class="issue-card">
            <div class="issue-header">
                <h2 class="issue-title">{title}</h2>
                {f'<p class="issue-subtitle">{subtitle}</p>' if subtitle else ''}
                <p class="issue-summary">{summary}</p>
                <div class="issue-meta">
                    <span class="issue-date">{self.format_date(created_at)}</span>
                    <div class="source-counts">
                        <span class="source-count">진보 {left_source}개</span>
                        <span class="source-count">중도 {center_source}개</span>
                        <span class="source-count">보수 {right_source}개</span>
                    </div>
                </div>
            </div>
            
            <div class="views-container">
                {self.generate_view_section('진보적 관점', left_view, 'left')}
                {self.generate_view_section('중도적 관점', center_view, 'center')}
                {self.generate_view_section('보수적 관점', right_view, 'right')}
            </div>
        </div>
        """
    
    def generate_view_section(self, title, content, bias):
        """View 섹션 HTML 생성"""
        if not content:
            return f"""
            <div class="view-section {bias}-view">
                <div class="view-header">
                    <h3 class="view-title no-bullet">{title}</h3>
                </div>
                <div class="view-content no-view">관점이 생성되지 않았습니다.</div>
            </div>
            """
        
        formatted_content = self.format_view_content(content)
        
        return f"""
        <div class="view-section {bias}-view">
            <div class="view-header">
                <h3 class="view-title no-bullet">{title}</h3>
            </div>
            <div class="view-content">{formatted_content}</div>
        </div>
        """

def main():
    """메인 함수"""
    try:
        generator = HTMLGenerator()
        output_file = generator.generate_html()
        print(f"🌐 브라우저에서 {output_file} 파일을 열어보세요!")
        
    except Exception as e:
        print(f"❌ HTML 생성 실패: {str(e)}")

if __name__ == "__main__":
    main()
