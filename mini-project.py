# Yêu cầu 1: Chuẩn hóa và làm sạch dữ liệu
# Chuẩn hóa cột salary về dạng số, xử lý các giá trị như 'Thoả thuận', 'Trên X triệu', 'X - Y triệu', 'Tới X triệu', etc.
# Tạo thêm các cột phụ: min_salary, max_salary, salary_unit (VND/USD)
# Xử lý cột address để tách thành city và district
# Chuẩn hóa job_title để gom nhóm các vị trí tương tự (ví dụ: 'Software Engineer', 'Developer', 'Programmer' có thể gom vào một nhóm)
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
import numpy as np

file = 'data.csv'
df = pd.read_csv(file)
#print(df['link_description'].unique()[:100])

def clean_date(date_col):
    return pd.to_datetime(date_col, errors='coerce')

def clean_title(job_title):
    new_title = job_title.lower()
    new_title = re.sub(r'[^0-9A-Za-zÀ-ỹ]',' ',new_title)
    new_title = re.sub(
        r'nhân viên|chuyên viên|onsite|dự án|thưởng|tháng|tới|usd|vnd|fresher|junior|senior|middle|manager|'
        r'thực tập sinh|thực tập|upto|up to|năm|triệu|lương|salary|kinh nghiệm|thu nhập|từ|hà nội|hồ chí minh|quận|'
        r'thanh xuân|tại|tiếng|anh|nhật|giao tiếp|[0-9]',
        '',
        new_title
    )
    new_title = re.sub(r'\b[a-z]\b','',new_title)

    new_title = ' '.join(new_title.split())
    return new_title

def parse_title(title):
    if re.search(
            r'lập trình|net|smartcontract|brse|kỹ sư|triển khai|java|engineer|angularjs|laravel|php|mobile|ios|'
            r'nodejs|android|front|back|end|python|react|delivery|ux|technical|dev|công nghệ thông tin|big data|ruby|'
            r'data scientist|enigneer|fullstack|data|blockchain|phần mềm',
            title
    ):
        return 'Developer / Engineer'
    elif re.search(r'test|tester|qa|qc|kiểm thử', title):
        return 'Tester / QA'
    elif re.search(r'business|analyst|product|project|account|nghiệp vụ|ba', title):
        return 'Business / Analyst / Product'
    elif re.search(r'devops|it|kỹ thuật|kĩ thuật|system|admin|giám sát|vận hành|quản trị|infra|hệ thống|hạ tầng|'
                   r'cntt|operation', title):
        return 'IT Operations / DevOps / Support'
    elif re.search(r'design|creator|artist', title):
        return 'Designer / Creative'
    else:
        return 'Other'

def convert_to_vnd(salary):
    return salary * 25000

def parse_salary(sal_str):
    if pd.isna(sal_str) or 'thỏa thuận' in sal_str:
        return np.nan, np.nan, np.nan
    elif re.search(r'trên',sal_str):
        sal_str = re.sub(r'trên|triệu|,','',sal_str)
        if 'usd' in sal_str:
            return float(sal_str.replace('usd','')), np.nan, 'USD'
        else:
            return float(sal_str.replace('usd',''))*1000000, np.nan, 'VND'
    elif re.search(r'tới',sal_str):
        sal_str = re.sub(r'tới|triệu|,','',sal_str)
        if 'usd' in sal_str:
            return np.nan, float(sal_str.replace('usd','')), 'USD'
        else:
            return np.nan, float(sal_str.replace('usd',''))*1000000, 'VND'
    elif '-' in sal_str:
        sal_str = re.sub(r'trên|tới|triệu|,', '', sal_str)
        min_val, max_val = sal_str.split('-')
        if 'usd' in sal_str:
            return float(min_val.replace('usd','').strip()), float(max_val.replace('usd','').strip()), 'USD'
        else:
            return float(min_val.replace('usd','').strip())*1000000, float(max_val.replace('usd','').strip())*1000000, 'VND'

def parse_address():
    pass

# Định dạng date cho cột created_date
date_col = df['created_date']
df['created_date'] = clean_date(date_col)

# Clean data cột job_title
df['job_clean'] = df['job_title'].apply(clean_title)
df['new_title'] = df['job_clean'].apply(parse_title)
# all_words = ' '.join(df['job_clean']).split()
# common_words = Counter(all_words).most_common(100)
# new_df = pd.DataFrame(df['job_clean'])
# new_df['new_title'] = df['job_clean'].apply(parse_title)
# new_df_filter = new_df[new_df['new_title'].isna()]
# new_df.to_csv('title.csv',index=False,header=False)

print(df.info())
print(df['new_title'].nunique())

df['salary'] = df['salary'].str.lower().str.strip()
df[['min_salary','max_salary','currency']] = df['salary'].apply(lambda x: pd.Series(parse_salary(x)))
print(df[['min_salary','max_salary','currency']])
new_df = df[['created_date','job_title','new_title','company','salary','min_salary','max_salary','currency']]
# new_df = df[['salary','min_salary','max_salary','currency']]
# print(new_df['salary'].unique())
# print(new_df.dtypes)
new_df.to_csv('title.csv',index=False,header=0)