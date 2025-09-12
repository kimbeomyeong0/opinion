#!/usr/bin/env python3
"""
Background 생성기 테스트 스크립트
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from content.run_background_generator import generate_background

def test_background_generation():
    """Background 생성 테스트"""
    
    # 테스트 데이터
    test_data = {
        'title': '국회의원 체포동의안',
        'subtitle': '국회의원의 체포에 대한 국회 동의 절차',
        'left_view': '정치적 탄압을 방지하고 국회의원의 면책특권을 보호해야 한다',
        'right_view': '법 앞에 평등한 원칙에 따라 일반 시민과 동일하게 수사받아야 한다',
        'summary': '국회의원의 체포에 대한 국회 동의 절차와 관련된 논란'
    }
    
    print("🧪 Background 생성 테스트 시작...")
    print(f"테스트 이슈: {test_data['title']}")
    print("-" * 50)
    
    # Background 생성
    background = generate_background(
        title=test_data['title'],
        subtitle=test_data['subtitle'],
        left_view=test_data['left_view'],
        right_view=test_data['right_view'],
        summary=test_data['summary']
    )
    
    if background:
        print("✅ Background 생성 성공!")
        print("\n📝 생성된 Background:")
        print(background)
        
        # 불렛 개수 확인
        bullet_count = len([line for line in background.split('\n') if line.strip().startswith('•')])
        print(f"\n📊 불렛 개수: {bullet_count}개")
        
    else:
        print("❌ Background 생성 실패!")

if __name__ == "__main__":
    test_background_generation()
