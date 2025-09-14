#!/usr/bin/env python3
"""
툴팁 UI 컴포넌트 및 스타일 시스템
나무위키 스타일의 클릭 가능한 툴팁 구현
"""

from typing import Dict, List
from dataclasses import dataclass

@dataclass
class TooltipStyle:
    """툴팁 스타일 설정"""
    background_color: str = "#2d3748"
    text_color: str = "#ffffff"
    border_color: str = "#4a5568"
    border_radius: str = "8px"
    font_size: str = "14px"
    max_width: str = "300px"
    padding: str = "12px"
    box_shadow: str = "0 4px 12px rgba(0, 0, 0, 0.15)"

class TooltipUIController:
    """툴팁 UI 컨트롤러"""
    
    def __init__(self):
        self.style = TooltipStyle()
        self.active_tooltip = None
    
    def generate_css(self) -> str:
        """툴팁 CSS 스타일 생성"""
        return f"""
/* 툴팁 시스템 CSS */
.tooltip-trigger {{
    color: #2563eb;
    text-decoration: underline;
    text-decoration-style: dotted;
    cursor: pointer;
    position: relative;
    border-bottom: 1px dotted #2563eb;
    transition: all 0.2s ease;
}}

.tooltip-trigger:hover {{
    color: #1d4ed8;
    text-decoration-color: #1d4ed8;
    background-color: rgba(37, 99, 235, 0.1);
    border-radius: 3px;
    padding: 1px 2px;
}}

.tooltip {{
    position: absolute;
    z-index: 1000;
    background-color: {self.style.background_color};
    color: {self.style.text_color};
    border: 1px solid {self.style.border_color};
    border-radius: {self.style.border_radius};
    font-size: {self.style.font_size};
    max-width: {self.style.max_width};
    padding: {self.style.padding};
    box-shadow: {self.style.box_shadow};
    opacity: 0;
    visibility: hidden;
    transition: all 0.3s ease;
    transform: translateY(-10px);
    pointer-events: none;
}}

.tooltip.show {{
    opacity: 1;
    visibility: visible;
    transform: translateY(0);
    pointer-events: auto;
}}

.tooltip-content {{
    position: relative;
}}

.tooltip-term {{
    font-weight: bold;
    font-size: 15px;
    margin-bottom: 6px;
    color: #fbbf24;
    border-bottom: 1px solid #4a5568;
    padding-bottom: 4px;
}}

.tooltip-explanation {{
    line-height: 1.4;
    color: #e2e8f0;
}}

.tooltip-arrow {{
    position: absolute;
    top: -6px;
    left: 50%;
    transform: translateX(-50%);
    width: 0;
    height: 0;
    border-left: 6px solid transparent;
    border-right: 6px solid transparent;
    border-bottom: 6px solid {self.style.background_color};
}}

.tooltip-arrow::before {{
    content: '';
    position: absolute;
    top: 1px;
    left: -7px;
    width: 0;
    height: 0;
    border-left: 7px solid transparent;
    border-right: 7px solid transparent;
    border-bottom: 7px solid {self.style.border_color};
}}

/* 반응형 디자인 */
@media (max-width: 768px) {{
    .tooltip {{
        max-width: 250px;
        font-size: 13px;
        padding: 10px;
    }}
    
    .tooltip-term {{
        font-size: 14px;
    }}
}}

/* 다크모드 지원 */
@media (prefers-color-scheme: dark) {{
    .tooltip-trigger {{
        color: #60a5fa;
        border-bottom-color: #60a5fa;
    }}
    
    .tooltip-trigger:hover {{
        color: #3b82f6;
        text-decoration-color: #3b82f6;
        background-color: rgba(96, 165, 250, 0.1);
    }}
}}

/* 접근성 개선 */
.tooltip-trigger:focus {{
    outline: 2px solid #2563eb;
    outline-offset: 2px;
}}

/* 애니메이션 효과 */
@keyframes tooltipFadeIn {{
    from {{
        opacity: 0;
        transform: translateY(-10px) scale(0.95);
    }}
    to {{
        opacity: 1;
        transform: translateY(0) scale(1);
    }}
}}

.tooltip.show {{
    animation: tooltipFadeIn 0.2s ease-out;
}}

/* 툴팁 그룹 스타일 */
.tooltip-group {{
    position: relative;
    display: inline-block;
}}

.tooltip-group .tooltip {{
    position: fixed;
    top: auto;
    left: auto;
}}
"""
    
    def generate_javascript(self) -> str:
        """툴팁 JavaScript 코드 생성"""
        return """
// 툴팁 시스템 JavaScript
class TooltipManager {
    constructor() {
        this.activeTooltip = null;
        this.tooltipElements = new Map();
        this.init();
    }
    
    init() {
        // 툴팁 트리거 요소들에 이벤트 리스너 추가
        document.querySelectorAll('.tooltip-trigger').forEach(trigger => {
            const tooltipId = trigger.getAttribute('data-tooltip-id');
            const tooltip = document.getElementById(tooltipId);
            
            if (tooltip) {
                this.tooltipElements.set(tooltipId, tooltip);
                
                // 마우스 이벤트
                trigger.addEventListener('mouseenter', (e) => this.showTooltip(e, tooltipId));
                trigger.addEventListener('mouseleave', () => this.hideTooltip());
                trigger.addEventListener('click', (e) => this.toggleTooltip(e, tooltipId));
                
                // 키보드 접근성
                trigger.addEventListener('keydown', (e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        this.toggleTooltip(e, tooltipId);
                    }
                });
                
                // 포커스 이벤트
                trigger.addEventListener('focus', (e) => this.showTooltip(e, tooltipId));
                trigger.addEventListener('blur', () => this.hideTooltip());
            }
        });
        
        // 문서 클릭 이벤트로 툴팁 숨기기
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.tooltip-trigger') && !e.target.closest('.tooltip')) {
                this.hideTooltip();
            }
        });
        
        // ESC 키로 툴팁 숨기기
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                this.hideTooltip();
            }
        });
        
        // 스크롤 시 툴팁 위치 업데이트
        window.addEventListener('scroll', () => this.updateTooltipPosition());
        window.addEventListener('resize', () => this.updateTooltipPosition());
    }
    
    showTooltip(event, tooltipId) {
        this.hideTooltip(); // 기존 툴팁 숨기기
        
        const tooltip = this.tooltipElements.get(tooltipId);
        if (!tooltip) return;
        
        this.activeTooltip = tooltipId;
        tooltip.classList.add('show');
        
        // 툴팁 위치 계산
        this.positionTooltip(event, tooltip);
        
        // 툴팁을 문서에 추가 (아직 추가되지 않은 경우)
        if (!tooltip.parentNode) {
            document.body.appendChild(tooltip);
        }
    }
    
    hideTooltip() {
        if (this.activeTooltip) {
            const tooltip = this.tooltipElements.get(this.activeTooltip);
            if (tooltip) {
                tooltip.classList.remove('show');
            }
            this.activeTooltip = null;
        }
    }
    
    toggleTooltip(event, tooltipId) {
        event.preventDefault();
        event.stopPropagation();
        
        if (this.activeTooltip === tooltipId) {
            this.hideTooltip();
        } else {
            this.showTooltip(event, tooltipId);
        }
    }
    
    positionTooltip(event, tooltip) {
        const rect = event.target.getBoundingClientRect();
        const tooltipRect = tooltip.getBoundingClientRect();
        const viewportWidth = window.innerWidth;
        const viewportHeight = window.innerHeight;
        
        let left = rect.left + (rect.width / 2) - (tooltipRect.width / 2);
        let top = rect.top - tooltipRect.height - 10;
        
        // 화면 경계 확인 및 조정
        if (left < 10) {
            left = 10;
        } else if (left + tooltipRect.width > viewportWidth - 10) {
            left = viewportWidth - tooltipRect.width - 10;
        }
        
        if (top < 10) {
            // 위쪽 공간이 부족하면 아래쪽에 표시
            top = rect.bottom + 10;
        }
        
        tooltip.style.left = `${left}px`;
        tooltip.style.top = `${top}px`;
        tooltip.style.position = 'fixed';
    }
    
    updateTooltipPosition() {
        if (this.activeTooltip) {
            const tooltip = this.tooltipElements.get(this.activeTooltip);
            const trigger = document.querySelector(`[data-tooltip-id="${this.activeTooltip}"]`);
            
            if (tooltip && trigger) {
                const rect = trigger.getBoundingClientRect();
                const tooltipRect = tooltip.getBoundingClientRect();
                
                let left = rect.left + (rect.width / 2) - (tooltipRect.width / 2);
                let top = rect.top - tooltipRect.height - 10;
                
                tooltip.style.left = `${left}px`;
                tooltip.style.top = `${top}px`;
            }
        }
    }
}

// 툴팁 매니저 초기화
document.addEventListener('DOMContentLoaded', () => {
    new TooltipManager();
});

// 전역 함수로 내보내기 (기존 코드와의 호환성)
function showTooltip(event, tooltipId) {
    // TooltipManager 인스턴스에 접근하기 위한 전역 변수
    if (window.tooltipManager) {
        window.tooltipManager.showTooltip(event, tooltipId);
    }
}

function hideTooltip() {
    if (window.tooltipManager) {
        window.tooltipManager.hideTooltip();
    }
}
"""
    
    def generate_html_template(self, content: str, tooltips: str) -> str:
        """완전한 HTML 템플릿 생성"""
        css = self.generate_css()
        js = self.generate_javascript()
        
        return f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>정치 용어 툴팁 시스템</title>
    <style>
        {css}
        
        /* 기본 페이지 스타일 */
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f8fafc;
        }}
        
        .content {{
            background: white;
            padding: 30px;
            border-radius: 12px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
        }}
        
        h1 {{
            color: #1e40af;
            border-bottom: 3px solid #3b82f6;
            padding-bottom: 10px;
        }}
        
        .demo-section {{
            margin: 30px 0;
            padding: 20px;
            background: #f1f5f9;
            border-radius: 8px;
            border-left: 4px solid #3b82f6;
        }}
        
        .demo-title {{
            font-weight: bold;
            color: #1e40af;
            margin-bottom: 15px;
            font-size: 18px;
        }}
        
        .instructions {{
            background: #fef3c7;
            border: 1px solid #f59e0b;
            border-radius: 6px;
            padding: 15px;
            margin: 20px 0;
        }}
        
        .instructions h3 {{
            margin-top: 0;
            color: #92400e;
        }}
        
        .instructions ul {{
            margin: 10px 0;
            padding-left: 20px;
        }}
        
        .instructions li {{
            margin: 5px 0;
        }}
    </style>
</head>
<body>
    <div class="content">
        <h1>🧠 정치 용어 툴팁 시스템</h1>
        
        <div class="instructions">
            <h3>📖 사용법</h3>
            <ul>
                <li><strong>마우스 호버:</strong> 용어에 마우스를 올리면 툴팁이 나타납니다</li>
                <li><strong>클릭:</strong> 용어를 클릭하면 툴팁이 토글됩니다</li>
                <li><strong>키보드:</strong> Tab으로 용어에 포커스하고 Enter/Space로 툴팁을 열 수 있습니다</li>
                <li><strong>닫기:</strong> 다른 곳을 클릭하거나 ESC 키를 누르면 툴팁이 닫힙니다</li>
            </ul>
        </div>
        
        <div class="demo-section">
            <div class="demo-title">📰 정치 뉴스 예시</div>
            <div class="demo-content">
                {content}
            </div>
        </div>
        
        <div class="demo-section">
            <div class="demo-title">📚 추가 예시</div>
            <div class="demo-content">
                <p>국회에서 대정부질문이 진행되는 동안 여당과 야당 간의 정치적 갈등이 표면화되었습니다. 
                사법개혁과 검찰개혁에 대한 의견 차이로 인해 국정감사 과정에서 심각한 대립이 발생했습니다. 
                하지만 국민의 복지와 국가 발전을 위해서는 정책 협의와 타협이 필요하다는 점에서 양당 모두 공감하고 있습니다.</p>
                
                <p>외교부는 한미동맹 강화를 위해 지속적인 협력이 필요하다고 강조했습니다. 
                북핵 문제와 관련해서는 국제사회와의 협력 하에 평화적 해결책을 모색해야 한다는 입장을 밝혔습니다.</p>
            </div>
        </div>
    </div>
    
    <!-- 툴팁 정의들 -->
    {tooltips}
    
    <script>
        {js}
        
        // 전역 툴팁 매니저 인스턴스 생성
        document.addEventListener('DOMContentLoaded', () => {{
            window.tooltipManager = new TooltipManager();
        }});
    </script>
</body>
</html>"""

def test_ui_system():
    """UI 시스템 테스트"""
    from sonar_tooltip_system import SonarTooltipSystem
    
    # 툴팁 시스템 초기화
    tooltip_system = SonarTooltipSystem()
    ui_controller = TooltipUIController()
    
    # 테스트 텍스트
    test_text = """
    이재명 대통령은 국회에서 대정부질문에 답변하며 사법개혁과 검찰개혁의 필요성을 강조했습니다. 
    여당인 더불어민주당과 야당인 국민의힘 사이에 정치적 갈등이 있었지만, 
    국민의 복지와 국가 발전을 위한 정책 협의가 필요하다고 밝혔습니다.
    국정감사 과정에서 양당 간의 의견 차이로 인한 대립이 있었지만, 
    결국 국민의 이익을 최우선으로 하는 합의점을 찾아야 한다는 점에서 공감대를 형성했습니다.
    """
    
    # HTML 생성
    content_html = tooltip_system.generate_tooltip_html(test_text)
    tooltip_definitions = tooltip_system.generate_tooltip_definitions(test_text)
    
    # 완전한 HTML 페이지 생성
    full_html = ui_controller.generate_html_template(content_html, tooltip_definitions)
    
    # HTML 파일로 저장
    with open('experiments/tooltip_demo.html', 'w', encoding='utf-8') as f:
        f.write(full_html)
    
    print("✅ 툴팁 UI 시스템 테스트 완료")
    print("📁 생성된 파일: experiments/tooltip_demo.html")
    print("🌐 브라우저에서 확인해보세요!")

if __name__ == "__main__":
    test_ui_system()
