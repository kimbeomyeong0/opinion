-- Issues 테이블의 view 컬럼들을 JSONB에서 TEXT로 변경하는 SQL 쿼리
-- Supabase SQL Editor에서 실행하세요

-- 1. 기존 JSONB 컬럼을 TEXT로 변경
ALTER TABLE issues 
ALTER COLUMN left_view TYPE TEXT USING left_view::TEXT;

ALTER TABLE issues 
ALTER COLUMN center_view TYPE TEXT USING center_view::TEXT;

ALTER TABLE issues 
ALTER COLUMN right_view TYPE TEXT USING right_view::TEXT;

-- 2. 컬럼 코멘트 추가 (선택사항)
COMMENT ON COLUMN issues.left_view IS '진보적 관점 (TEXT 형식)';
COMMENT ON COLUMN issues.center_view IS '중도적 관점 (TEXT 형식)';
COMMENT ON COLUMN issues.right_view IS '보수적 관점 (TEXT 형식)';

-- 3. 변경 확인 쿼리 (실행 후 확인용)
SELECT 
    id,
    title,
    CASE 
        WHEN left_view IS NOT NULL THEN 'TEXT (' || LENGTH(left_view) || ' chars)'
        ELSE 'NULL'
    END as left_view_type,
    CASE 
        WHEN center_view IS NOT NULL THEN 'TEXT (' || LENGTH(center_view) || ' chars)'
        ELSE 'NULL'
    END as center_view_type,
    CASE 
        WHEN right_view IS NOT NULL THEN 'TEXT (' || LENGTH(right_view) || ' chars)'
        ELSE 'NULL'
    END as right_view_type
FROM issues 
LIMIT 5;
