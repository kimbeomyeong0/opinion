from dotenv import load_dotenv
import os

# .env 파일 로드
load_dotenv()

# 값 읽기
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")

print("SUPABASE_URL:", supabase_url)
print("SUPABASE_KEY:", supabase_key[:10] + "..." if supabase_key else None)
