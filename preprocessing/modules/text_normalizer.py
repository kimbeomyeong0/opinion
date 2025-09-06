#!/usr/bin/env python3
"""
텍스트 정규화 모듈
- 한자 → 한글 변환
- 영문 약어 → 한글 변환
- 영문 대소문자 통일
- 실제 기사 데이터 분석 결과 반영 (핵심 용어 100개)
"""

import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class NormalizationResult:
    """텍스트 정규화 결과"""
    original_text: str
    normalized_text: str
    changes_made: List[Dict[str, str]]
    normalization_metadata: Dict[str, Any]
    success: bool
    error_message: Optional[str] = None

class TextNormalizer:
    """텍스트 정규화 클래스"""
    
    def __init__(self):
        """텍스트 정규화기 초기화"""
        self.hanja_dict = self._initialize_hanja_dict()
        self.abbrev_dict = self._initialize_abbrev_dict()
        self.case_normalization = self._initialize_case_normalization()
        
    def _initialize_hanja_dict(self) -> Dict[str, str]:
        """한자 → 한글 변환 사전 초기화 (실제 분석 결과 + 정치 핵심 용어)"""
        return {
            # 실제 분석에서 자주 나온 한자들
            '與': '여당',
            '野': '야당', 
            '中': '중국',
            '美': '미국',
            '北': '북한',
            '對': '대',
            '李': '이',
            '文': '문',
            '軍': '군',
            '新': '신',
            '檢': '검찰',
            
            # 정치/외교 핵심 한자 (빈도 높은 순)
            '中國': '중국',  # 긴 패턴 우선
            '美國': '미국',
            '韓國': '한국',
            '韓': '한국',
            '日': '일본',
            '露': '러시아',
            '英': '영국',
            '獨': '독일',
            '佛': '프랑스',
            
            # 정부/기관 관련
            '政': '정부',
            '國': '국가',
            '會': '회의',
            '黨': '당',
            '院': '원',
            '部': '부',
            '省': '성',
            '廳': '청',
            
            # 직책 관련
            '長': '장',
            '相': '상',
            '統': '통',
            '委': '위',
            '員': '원',
            '官': '관',
            '司': '사',
            
            # 경제 관련
            '經': '경제',
            '濟': '제',
            '財': '재정',
            '金': '금융',
            '銀': '은행',
            '企': '기업',
            '業': '업',
            '産': '산업',
            '貿': '무역',
            
            # 사회 관련
            '社': '사회',
            '民': '국민',
            '人': '인',
            '大': '대',
            '學': '학',
            '敎': '교육',
            '醫': '의료',
            '法': '법',
            '警': '경찰',
            
            # 군사/안보 관련
            '核': '핵',
            '武': '무기',
            '戰': '전쟁',
            '軍事': '군사',
            '安保': '안보',
            '防衛': '방위',
            
            # 지역 관련
            '東': '동',
            '西': '서',
            '南': '남',
            '北': '북',
            '京': '경',
            '州': '주',
            '道': '도',
            '市': '시',
            '區': '구'
        }
    
    def _initialize_abbrev_dict(self) -> Dict[str, str]:
        """영문 약어 → 한글 변환 사전 초기화 (실제 분석 결과 + 핵심 용어)"""
        return {
            # 실제 분석에서 자주 나온 약어들
            'AI': '인공지능',
            'ICBM': '대륙간탄도미사일',
            'JTBC': 'JTBC',  # 방송사명은 그대로
            'YTN': 'YTN',
            'MBC': 'MBC',
            'ROTC': '학군단',
            'APEC': '아시아태평양경제협력체',
            'GPS': '위성항법시스템',
            'LNG': '액화천연가스',
            'IPO': '기업공개',
            'NBA': 'NBA',  # 스포츠는 그대로
            'KOSPI': '코스피',
            'SLBM': '잠수함발사탄도미사일',
            
            # 국제기구/정치
            'UN': '유엔',
            'NATO': '나토',
            'WHO': '세계보건기구',
            'IMF': '국제통화기금',
            'WTO': '세계무역기구',
            'G7': '주요7개국',
            'G20': '주요20개국',
            'EU': '유럽연합',
            'ASEAN': '동남아시아국가연합',
            
            # 경제/금융
            'GDP': '국내총생산',
            'GNP': '국민총생산',
            'CPI': '소비자물가지수',
            'PPI': '생산자물가지수',
            'CEO': '최고경영자',
            'CFO': '최고재무책임자',
            'CTO': '최고기술책임자',
            'IPO': '기업공개',
            'M&A': '인수합병',
            'FTA': '자유무역협정',
            
            # 기술/IT
            'IT': '정보기술',
            'IoT': '사물인터넷',
            'VR': '가상현실',
            'AR': '증강현실',
            'API': '응용프로그램인터페이스',
            'OS': '운영체제',
            'CPU': '중앙처리장치',
            'GPU': '그래픽처리장치',
            'RAM': '주기억장치',
            'SSD': '고체저장장치',
            
            # 의료/보건  
            'FDA': '미국식품의약국',
            'CDC': '미국질병통제예방센터',
            'DNA': '디엔에이',
            'RNA': '리보핵산',
            'MRI': '자기공명영상',
            'CT': '컴퓨터단층촬영',
            
            # 군사/안보
            'CIA': '미국중앙정보국',
            'FBI': '미국연방수사국',
            'NSA': '미국국가안보국',
            'KGB': '소련국가보안위원회',
            'GPS': '위성항법시스템',
            'UAV': '무인항공기',
            
            # 환경/에너지
            'CO2': '이산화탄소',
            'PM2.5': '초미세먼지',
            'LED': '발광다이오드',
            'LCD': '액정디스플레이',
            'OLED': '유기발광다이오드'
        }
    
    def _initialize_case_normalization(self) -> Dict[str, str]:
        """영문 대소문자 통일 사전"""
        # 소문자/혼합 → 대문자로 통일
        normalization_dict = {}
        
        # 기본 약어들의 모든 케이스 변형을 대문자로 통일
        base_abbrevs = ['AI', 'IT', 'UN', 'EU', 'GDP', 'CEO', 'FBI', 'CIA', 'GPS', 'USB', 'URL', 'API']
        
        for abbrev in base_abbrevs:
            # 소문자 버전
            normalization_dict[abbrev.lower()] = abbrev
            # 첫글자만 대문자 버전
            normalization_dict[abbrev.capitalize()] = abbrev
            # 이미 대문자면 그대로
            normalization_dict[abbrev] = abbrev
        
        return normalization_dict
    
    def normalize_hanja(self, text: str) -> Tuple[str, List[Dict[str, str]]]:
        """한자 정규화"""
        normalized_text = text
        changes = []
        
        # 긴 한자 패턴부터 처리 (中國 → 중국이 中 → 중국보다 우선)
        sorted_hanja = sorted(self.hanja_dict.items(), key=lambda x: len(x[0]), reverse=True)
        
        for hanja, hangul in sorted_hanja:
            if hanja in normalized_text:
                old_text = normalized_text
                normalized_text = normalized_text.replace(hanja, hangul)
                
                if old_text != normalized_text:
                    changes.append({
                        'type': 'hanja',
                        'original': hanja,
                        'normalized': hangul,
                        'pattern': f'{hanja} → {hangul}'
                    })
        
        return normalized_text, changes
    
    def normalize_abbreviations(self, text: str) -> Tuple[str, List[Dict[str, str]]]:
        """영문 약어 정규화"""
        normalized_text = text
        changes = []
        
        for abbrev, hangul in self.abbrev_dict.items():
            # 한글-영문 경계에서도 작동하도록 개선된 패턴
            # 앞뒤가 공백이거나 한글, 숫자, 특수문자인 경우 매칭
            pattern = r'(?<![A-Za-z])' + re.escape(abbrev) + r'(?![A-Za-z])'
            
            if re.search(pattern, normalized_text):
                old_text = normalized_text
                normalized_text = re.sub(pattern, hangul, normalized_text)
                
                if old_text != normalized_text:
                    changes.append({
                        'type': 'abbreviation',
                        'original': abbrev,
                        'normalized': hangul,
                        'pattern': f'{abbrev} → {hangul}'
                    })
        
        return normalized_text, changes
    
    def normalize_case(self, text: str) -> Tuple[str, List[Dict[str, str]]]:
        """영문 대소문자 정규화"""
        normalized_text = text
        changes = []
        
        for original, normalized in self.case_normalization.items():
            if original != normalized:  # 실제로 변경이 필요한 경우만
                pattern = r'\b' + re.escape(original) + r'\b'
                
                if re.search(pattern, normalized_text):
                    old_text = normalized_text
                    normalized_text = re.sub(pattern, normalized, normalized_text)
                    
                    if old_text != normalized_text:
                        changes.append({
                            'type': 'case',
                            'original': original,
                            'normalized': normalized,
                            'pattern': f'{original} → {normalized}'
                        })
        
        return normalized_text, changes
    
    def normalize_text(self, text: str) -> NormalizationResult:
        """텍스트 전체 정규화"""
        try:
            original_text = text
            normalized_text = text
            all_changes = []
            
            # 1. 한자 정규화
            normalized_text, hanja_changes = self.normalize_hanja(normalized_text)
            all_changes.extend(hanja_changes)
            
            # 2. 영문 약어 정규화
            normalized_text, abbrev_changes = self.normalize_abbreviations(normalized_text)
            all_changes.extend(abbrev_changes)
            
            # 3. 대소문자 정규화
            normalized_text, case_changes = self.normalize_case(normalized_text)
            all_changes.extend(case_changes)
            
            # 정규화 메타데이터
            normalization_metadata = {
                'original_length': len(original_text),
                'normalized_length': len(normalized_text),
                'hanja_changes': len(hanja_changes),
                'abbrev_changes': len(abbrev_changes),
                'case_changes': len(case_changes),
                'total_changes': len(all_changes),
                'change_ratio': len(all_changes) / len(original_text.split()) if len(original_text.split()) > 0 else 0
            }
            
            return NormalizationResult(
                original_text=original_text,
                normalized_text=normalized_text,
                changes_made=all_changes,
                normalization_metadata=normalization_metadata,
                success=True
            )
            
        except Exception as e:
            logger.error(f"텍스트 정규화 중 오류 발생: {str(e)}")
            return NormalizationResult(
                original_text=text,
                normalized_text=text,
                changes_made=[],
                normalization_metadata={},
                success=False,
                error_message=str(e)
            )
    
    def normalize_articles(self, articles: List[Dict[str, Any]]) -> List[NormalizationResult]:
        """여러 기사 일괄 정규화"""
        results = []
        
        for article in articles:
            title = article.get('title', '')
            content = article.get('content', '')
            
            # 제목과 본문을 합쳐서 정규화
            combined_text = f"{title} {content}".strip()
            
            if combined_text:
                result = self.normalize_text(combined_text)
                results.append(result)
            else:
                # 빈 텍스트의 경우
                results.append(NormalizationResult(
                    original_text="",
                    normalized_text="",
                    changes_made=[],
                    normalization_metadata={},
                    success=True
                ))
        
        return results
    
    def get_normalization_statistics(self, results: List[NormalizationResult]) -> Dict[str, Any]:
        """정규화 통계 정보"""
        if not results:
            return {}
        
        successful_results = [r for r in results if r.success]
        
        total_articles = len(results)
        successful_articles = len(successful_results)
        
        # 변경 유형별 통계
        total_hanja_changes = sum(r.normalization_metadata.get('hanja_changes', 0) for r in successful_results)
        total_abbrev_changes = sum(r.normalization_metadata.get('abbrev_changes', 0) for r in successful_results)
        total_case_changes = sum(r.normalization_metadata.get('case_changes', 0) for r in successful_results)
        total_changes = sum(r.normalization_metadata.get('total_changes', 0) for r in successful_results)
        
        # 가장 자주 변경된 패턴들
        change_patterns = []
        for result in successful_results:
            for change in result.changes_made:
                change_patterns.append(change['pattern'])
        
        from collections import Counter
        most_common_changes = Counter(change_patterns).most_common(10)
        
        return {
            'total_articles': total_articles,
            'successful_articles': successful_articles,
            'failed_articles': total_articles - successful_articles,
            'success_rate': successful_articles / total_articles if total_articles > 0 else 0,
            'total_changes': total_changes,
            'hanja_changes': total_hanja_changes,
            'abbrev_changes': total_abbrev_changes,
            'case_changes': total_case_changes,
            'avg_changes_per_article': total_changes / successful_articles if successful_articles > 0 else 0,
            'most_common_changes': most_common_changes,
            'dictionary_sizes': {
                'hanja_dict': len(self.hanja_dict),
                'abbrev_dict': len(self.abbrev_dict),
                'case_normalization': len(self.case_normalization)
            }
        }

if __name__ == "__main__":
    # 테스트용 코드
    normalizer = TextNormalizer()
    
    # 테스트 텍스트들
    test_texts = [
        "李대통령이 美-中 관계에 대해 AI 기술 협력을 강조했다.",
        "北核 문제 해결을 위한 UN 제재가 필요하다고 gdp 성장률과 함께 발표했다.",
        "與野 합의로 IT 정책과 iot 기술 도입을 추진한다."
    ]
    
    print("=== 텍스트 정규화 테스트 ===")
    for i, text in enumerate(test_texts, 1):
        result = normalizer.normalize_text(text)
        
        print(f"\n테스트 {i}:")
        print(f"원본: {result.original_text}")
        print(f"정규화: {result.normalized_text}")
        print(f"변경사항: {len(result.changes_made)}개")
        
        for change in result.changes_made:
            print(f"  - {change['pattern']} ({change['type']})")
    
    # 통계 정보
    results = [normalizer.normalize_text(text) for text in test_texts]
    stats = normalizer.get_normalization_statistics(results)
    
    print(f"\n=== 통계 ===")
    print(f"총 변경사항: {stats['total_changes']}개")
    print(f"한자 변경: {stats['hanja_changes']}개")
    print(f"약어 변경: {stats['abbrev_changes']}개")
    print(f"대소문자 변경: {stats['case_changes']}개")
    print(f"사전 크기: 한자 {stats['dictionary_sizes']['hanja_dict']}개, 약어 {stats['dictionary_sizes']['abbrev_dict']}개")
