#!/usr/bin/env python3
"""
리포트 생성기 - HTML 리포트 생성 및 데이터 변환 통합
"""

import sys
import os
import json
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_manager import get_supabase_client

class ReportGenerator:
    """리포트 생성 클래스"""
    
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
    
    def convert_json_views_to_text(self):
        """JSON 형태의 view 데이터를 TEXT로 변환"""
        try:
            print("🔄 기존 view 데이터를 TEXT 형태로 변환 중...")
            
            # 모든 이슈 조회
            result = self.supabase.client.table('issues')\
                .select('id, left_view, center_view, right_view')\
                .execute()
            
            if not result.data:
                print("❌ 이슈 데이터가 없습니다.")
                return False
            
            converted_count = 0
            
            for issue in result.data:
                issue_id = issue['id']
                update_data = {}
                
                # 각 성향별 view 데이터 변환
                for bias in ['left', 'center', 'right']:
                    view_key = f'{bias}_view'
                    view_data = issue.get(view_key)
                    
                    if view_data:
                        # JSON 문자열인 경우 파싱해서 텍스트로 변환
                        if isinstance(view_data, str) and view_data.startswith('"'):
                            try:
                                # JSON 문자열에서 실제 텍스트 추출
                                parsed_data = json.loads(view_data)
                                if isinstance(parsed_data, str):
                                    # 줄바꿈 문자를 실제 줄바꿈으로 변환
                                    text_data = parsed_data.replace('\\n', '\n')
                                    update_data[view_key] = text_data
                                    print(f"✅ {bias} view 변환 완료")
                                else:
                                    update_data[view_key] = str(parsed_data)
                            except json.JSONDecodeError:
                                # JSON이 아닌 경우 그대로 사용
                                update_data[view_key] = view_data
                        else:
                            # 이미 텍스트인 경우 그대로 사용
                            update_data[view_key] = view_data
                
                # 변환된 데이터 업데이트
                if update_data:
                    update_result = self.supabase.client.table('issues')\
                        .update(update_data)\
                        .eq('id', issue_id)\
                        .execute()
                    
                    if update_result.data:
                        converted_count += 1
                        print(f"✅ 이슈 {issue_id} 변환 완료")
                    else:
                        print(f"❌ 이슈 {issue_id} 변환 실패")
            
            print(f"🎉 변환 완료! 총 {converted_count}개 이슈 처리")
            return True
            
        except Exception as e:
            print(f"❌ 변환 실패: {str(e)}")
            return False
    
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
        
        .issue {{
            background: white;
            border-radius: 12px;
            padding: 30px;
            margin-bottom: 40px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            border-left: 4px solid #e0e0e0;
        }}
        
        .issue-title {{
            font-size: 1.4rem;
            font-weight: 600;
            color: #1a1a1a;
            margin-bottom: 10px;
            line-height: 1.4;
        }}
        
        .issue-subtitle {{
            font-size: 1rem;
            color: #666;
            margin-bottom: 15px;
            font-style: italic;
        }}
        
        .issue-summary {{
            font-size: 1rem;
            color: #444;
            margin-bottom: 25px;
            padding: 15px;
            background-color: #f8f9fa;
            border-radius: 8px;
            border-left: 3px solid #007bff;
        }}
        
        .issue-why {{
            font-size: 0.95rem;
            color: #555;
            margin-bottom: 15px;
            padding: 12px;
            background-color: #fff3cd;
            border-radius: 8px;
            border-left: 3px solid #ffc107;
            line-height: 1.5;
        }}
        
        .issue-why strong {{
            color: #856404;
            font-size: 0.9rem;
        }}
        
        .issue-history {{
            font-size: 0.95rem;
            color: #555;
            margin-bottom: 15px;
            padding: 12px;
            background-color: #f8d7da;
            border-radius: 8px;
            border-left: 3px solid #dc3545;
            line-height: 1.5;
        }}
        
        .issue-history strong {{
            color: #721c24;
            font-size: 0.9rem;
        }}
        
        .issue-timeline {{
            font-size: 0.95rem;
            color: #555;
            margin-bottom: 20px;
            padding: 12px;
            background-color: #f0f8ff;
            border-radius: 8px;
            border-left: 3px solid #28a745;
            line-height: 1.5;
        }}
        
        .issue-timeline strong {{
            color: #155724;
            font-size: 0.9rem;
        }}
        
        .views-container {{
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            gap: 20px;
            margin-top: 20px;
        }}
        
        .view {{
            padding: 20px;
            border-radius: 8px;
            border: 2px solid #e0e0e0;
        }}
        
        .view.left {{
            background-color: #f0f8ff;
            border-color: #4dabf7;
        }}
        
        .view.center {{
            background-color: #fffbf0;
            border-color: #ffd93d;
        }}
        
        .view.right {{
            background-color: #fff5f5;
            border-color: #ff6b6b;
        }}
        
        .view-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }}
        
        .view-title {{
            font-weight: 600;
            font-size: 1rem;
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
            font-size: 0.85rem;
            color: #666;
            background-color: rgba(0,0,0,0.05);
            padding: 4px 8px;
            border-radius: 4px;
        }}
        
        .bias-gauge {{
            margin: 20px 0;
            padding: 15px;
            background-color: #f8f9fa;
            border-radius: 8px;
            border: 1px solid #e9ecef;
        }}
        
        .gauge-title {{
            font-size: 1.1rem;
            font-weight: 600;
            margin-bottom: 15px;
            color: #333;
        }}
        
        .gauge-container {{
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        
        .gauge-bar {{
            flex: 1;
            height: 20px;
            background-color: #e9ecef;
            border-radius: 10px;
            overflow: hidden;
            position: relative;
        }}
        
        .gauge-fill {{
            height: 100%;
            display: flex;
            transition: width 0.3s ease;
        }}
        
        .gauge-left {{
            background: linear-gradient(90deg, #1971c2, #339af0);
        }}
        
        .gauge-center {{
            background: linear-gradient(90deg, #fab005, #ffd43b);
        }}
        
        .gauge-right {{
            background: linear-gradient(90deg, #c92a2a, #ff6b6b);
        }}
        
        .gauge-labels {{
            display: flex;
            justify-content: space-between;
            margin-top: 8px;
            font-size: 0.85rem;
            color: #666;
        }}
        
        .gauge-label {{
            display: flex;
            align-items: center;
            gap: 5px;
        }}
        
        .gauge-dot {{
            width: 8px;
            height: 8px;
            border-radius: 50%;
        }}
        
        .gauge-dot.left {{
            background-color: #1971c2;
        }}
        
        .gauge-dot.center {{
            background-color: #fab005;
        }}
        
        .gauge-dot.right {{
            background-color: #c92a2a;
        }}
        
        .view-content {{
            font-size: 0.95rem;
            line-height: 1.6;
        }}
        
        .view-content p {{
            margin-bottom: 8px;
        }}
        
        .view-content p:last-child {{
            margin-bottom: 0;
        }}
        
        .issue-meta {{
            margin-top: 20px;
            padding-top: 15px;
            border-top: 1px solid #e0e0e0;
            font-size: 0.85rem;
            color: #666;
        }}
        
        .no-views {{
            text-align: center;
            color: #999;
            font-style: italic;
            padding: 40px;
        }}
        
        @media (max-width: 768px) {{
            .views-container {{
                grid-template-columns: 1fr;
            }}
            
            .container {{
                padding: 20px 15px;
            }}
            
            .issue {{
                padding: 20px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>성향별 관점 분석</h1>
            <p>정치적 성향에 따른 이슈별 관점 비교</p>
        </div>
        
        {self._generate_issues_html(issues)}
    </div>
</body>
</html>"""
        
        # HTML 파일 저장
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ HTML 리포트 생성 완료: {output_file}")
        return output_file
    
    def _generate_issues_html(self, issues):
        """이슈들 HTML 생성"""
        html_parts = []
        
        for issue in issues:
            # 이슈 메타데이터
            issue_id = issue.get('id', '')
            title = issue.get('title', '제목 없음')
            subtitle = issue.get('subtitle', '')
            summary = issue.get('summary', '')
            created_at = issue.get('created_at', '')
            timeline = issue.get('timeline', '')
            why = issue.get('why', '')
            history = issue.get('history', '')
            
            # 성향별 데이터
            left_source = issue.get('left_source', 0)
            center_source = issue.get('center_source', 0)
            right_source = issue.get('right_source', 0)
            
            left_view = issue.get('left_view', '')
            center_view = issue.get('center_view', '')
            right_view = issue.get('right_view', '')
            
            # 이슈 HTML 생성
            issue_html = f"""
            <div class="issue">
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
            """
            
            html_parts.append(issue_html)
        
        return '\n'.join(html_parts)
    
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

def main():
    """메인 함수"""
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        try:
            generator = ReportGenerator()
            
            if command == "html":
                output_file = sys.argv[2] if len(sys.argv) > 2 else 'issues.html'
                generator.generate_html(output_file)
            elif command == "convert":
                generator.convert_json_views_to_text()
            else:
                print("❌ 잘못된 명령어입니다.")
                print("사용법:")
                print("  python report_generator.py html [파일명]  # HTML 리포트 생성")
                print("  python report_generator.py convert       # JSON을 TEXT로 변환")
        except Exception as e:
            print(f"❌ 오류 발생: {str(e)}")
    else:
        print("🎯 리포트 생성기")
        print("\n사용법:")
        print("  python report_generator.py html [파일명]  # HTML 리포트 생성")
        print("  python report_generator.py convert       # JSON을 TEXT로 변환")

if __name__ == "__main__":
    main()
