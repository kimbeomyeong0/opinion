-- 기존 테이블 삭제 (있다면)
DROP TABLE IF EXISTS articles_cleaned CASCADE;

-- 새로운 articles_cleaned 테이블 생성
CREATE TABLE articles_cleaned (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_article_id UUID NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
    title_cleaned TEXT NOT NULL,
    content_cleaned TEXT NOT NULL,
    lead_paragraph TEXT,
    merged_content TEXT,
    preprocessing_metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 인덱스 생성
CREATE INDEX idx_articles_cleaned_original_id ON articles_cleaned(original_article_id);
CREATE INDEX idx_articles_cleaned_created_at ON articles_cleaned(created_at);
CREATE INDEX idx_articles_cleaned_merged_content ON articles_cleaned(merged_content) WHERE merged_content IS NOT NULL;

-- RLS 정책 설정
ALTER TABLE articles_cleaned ENABLE ROW LEVEL SECURITY;

-- 모든 사용자가 읽기/쓰기 가능하도록 설정
CREATE POLICY "articles_cleaned_all_access" ON articles_cleaned
    FOR ALL USING (true) WITH CHECK (true);

-- updated_at 자동 업데이트를 위한 트리거
CREATE OR REPLACE FUNCTION update_articles_cleaned_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_articles_cleaned_updated_at
    BEFORE UPDATE ON articles_cleaned
    FOR EACH ROW
    EXECUTE FUNCTION update_articles_cleaned_updated_at();
