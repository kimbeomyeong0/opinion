#!/usr/bin/env python3
"""
기존 JSON 형태의 view 데이터를 TEXT 형태로 변환하는 스크립트
"""

import sys
import os
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_manager import get_supabase_client

def convert_json_views_to_text():
    """JSON 형태의 view 데이터를 TEXT로 변환"""
    
    supabase = get_supabase_client()
    if not supabase.client:
        print("❌ Supabase 연결 실패")
        return False
    
    try:
        print("🔄 기존 view 데이터를 TEXT 형태로 변환 중...")
        
        # 모든 이슈 조회
        result = supabase.client.table('issues')\
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
                update_result = supabase.client.table('issues')\
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

if __name__ == "__main__":
    print("=" * 60)
    print("JSON 형태 view 데이터를 TEXT로 변환")
    print("=" * 60)
    
    success = convert_json_views_to_text()
    
    if success:
        print("\n✅ 변환 완료!")
        print("이제 view 컬럼들이 TEXT 형태로 저장되어 복사 붙여넣기가 편리합니다.")
    else:
        print("\n❌ 변환 실패!")
