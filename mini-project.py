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
import psycopg2

file = 'data.csv'
df = pd.read_csv(file)
#print(df['link_description'].unique()[:100])

def clean_date(date_col):
    return pd.to_datetime(date_col, errors='coerce')

def parse_title(title):
    title = title.lower()
    if re.search(
            r'lập trình|net|smartcontract|brse|kỹ sư|triển khai|java|engineer|angularjs|laravel|php|mobile|ios|'
            r'nodejs|android|front|back|end|python|react|ux|technical|dev|công nghệ thông tin|big data|ruby|'
            r'data scientist|enigneer|fullstack|blockchain|phần mềm|tích hợp|tech|c\+\+',
            title
    ):
        return 'Developer / Engineer'
    elif re.search(r'test|tester|qa|qc|kiểm thử', title):
        return 'Tester / QA'
    elif re.search(r'business|analyst|product|project|account|nghiệp vụ|ba|phân tích', title):
        return 'Business / Analyst / Product'
    elif re.search(r'devops|it|kỹ thuật|kĩ thuật|system|admin|giám sát|vận hành|quản trị|infra|hệ thống|hạ tầng|'
                   r'cntt|operation', title):
        return 'IT Operations / DevOps / Support'
    elif re.search(r'design|creator|artist|animation', title):
        return 'Designer / Creative'
    else:
        return 'Other'

def parse_salary(sal_str):
    sal_str = sal_str.lower().strip()
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

def parse_address(add_str):
    lst_str = [x.strip() for x in add_str.split(':')]
    if len(lst_str) > 1:
        if 'nước ngoài' in lst_str:
            lst_str = [x for x in lst_str if x.lower() != 'nước ngoài']
            city = lst_str[0::2]
            city.append('nước ngoài')
            city = ', '.join(city)
            district = ', '.join(lst_str[1::2])
            return city,district
        else:
            city = ', '.join(lst_str[0::2])
            district = ', '.join(lst_str[1::2])
            return city, district
    else:
        city = lst_str[0]
        district = np.nan
    return city, district



df['created_date'] = df['created_date'].apply(clean_date)

df['title'] = df['job_title'].apply(parse_title)

df[['min_salary','max_salary','currency']] = df['salary'].apply(lambda x: pd.Series(parse_salary(x)))
df[['city','district']] = df['address'].apply(lambda x: pd.Series(parse_address(x)))


new_df = df[['created_date','job_title','title','company','salary','min_salary','max_salary','currency','address','city','district','time','link_description']]
new_df.to_csv('title.csv',index=False,header=0)
print(new_df.info())

import psycopg2

conn = psycopg2.connect("dbname=mydb user=quannh password=123456 host=localhost port=5432")
cur = conn.cursor()

create_table_query = """
CREATE TABLE IF NOT EXISTS job_data (
    created_date      TIMESTAMP,
    job_title         TEXT,
    title             TEXT,
    company           TEXT,
    salary            TEXT,
    min_salary        DOUBLE PRECISION,
    max_salary        DOUBLE PRECISION,
    currency          TEXT,
    address           TEXT,
    city              TEXT,
    district          TEXT,
    time              TEXT,
    link_description  TEXT
);
"""
cur.execute(create_table_query)
conn.commit()

with open("title.csv", "r", encoding="utf-8") as f:
    # Bỏ dòng header
    next(f)
    cur.copy_expert("""
        COPY job_data(created_date, job_title, title, company, salary, 
                      min_salary, max_salary, currency, address, city, 
                      district, time, link_description)
        FROM STDIN WITH CSV DELIMITER ',';
    """, f)

conn.commit()
cur.close()
conn.close()

