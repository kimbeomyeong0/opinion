-- issues 테이블 스키마 업데이트
-- 기존 view 컬럼들을 삭제하고 새로운 구조로 변경

-- 기존 view 컬럼들 삭제
ALTER TABLE issues DROP COLUMN IF EXISTS left_view;
ALTER TABLE issues DROP COLUMN IF EXISTS center_view;
ALTER TABLE issues DROP COLUMN IF EXISTS right_view;

-- 새로운 view 구조 컬럼들 추가
ALTER TABLE issues ADD COLUMN IF NOT EXISTS left_view_title TEXT;
ALTER TABLE issues ADD COLUMN IF NOT EXISTS left_view_content TEXT;
ALTER TABLE issues ADD COLUMN IF NOT EXISTS center_view_title TEXT;
ALTER TABLE issues ADD COLUMN IF NOT EXISTS center_view_content TEXT;
ALTER TABLE issues ADD COLUMN IF NOT EXISTS right_view_title TEXT;
ALTER TABLE issues ADD COLUMN IF NOT EXISTS right_view_content TEXT;
