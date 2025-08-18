# Yêu cầu 1: Chuẩn hóa và làm sạch dữ liệu
# Chuẩn hóa cột salary về dạng số, xử lý các giá trị như "Thoả thuận", "Trên X triệu", "X - Y triệu", "Tới X triệu", etc.
# Tạo thêm các cột phụ: min_salary, max_salary, salary_unit (VND/USD)
# Xử lý cột address để tách thành city và district
# Chuẩn hóa job_title để gom nhóm các vị trí tương tự (ví dụ: "Software Engineer", "Developer", "Programmer" có thể gom vào một nhóm)
#
# Yêu cầu 2: Xây dựng Data Pipeline
# Thiết kế luồng ETL cơ bản: Extract từ CSV -> Transform dữ liệu -> Load vào database (MySQL hoặc PostgresDB)
# Thiết lập schedule job tự động chạy pipeline
# Xây dựng error handling cho các trường hợp lỗi phổ biến
#
# Yêu cầu 3: Phân tích dữ liệu
# Vẽ biểu đồ phân bố mức lương theo vị trí
# Vẽ bản đồ nhiệt (heatmap) phân bố việc làm theo khu vực
# Biểu đồ xu hướng công nghệ hot
#
# Lưu ý quan trọng:
# Sử dụng version control (git) cho code
# Viết unit test cho các hàm xử lý dữ liệu

import pandas as pd
from collections import Counter
import re

file = 'data.csv'
df = pd.read_csv(file)
#print(df.head())
#print(df['link_description'].unique()[:100])

def clean_date(date_col):
    return pd.to_datetime(date_col, errors='coerce')

def parse_salary():
    pass

def parse_address():
    pass

date_col = df['created_date']
df['created_date'] = clean_date(date_col)
print(df.info())
print(df['job_title'].unique()[:100])
print(df['job_title'].nunique())
df['job_clean'] = re.sub('-|(|)|senior|junior|fresher|thực tập sinh','',df['job_title'])
all_words = " ".join(df["job_clean"]).split()
common_words = Counter(all_words).most_common(30)
print(common_words)