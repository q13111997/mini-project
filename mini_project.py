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

from collections import Counter
import os
import logging
import pandas as pd
import re
import numpy as np
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns

file = 'data.csv'

try:
    if not os.path.exists(file):
        raise FileNotFoundError(f"File {file} không tồn tại!")
    df = pd.read_csv(file)
    logging.info("Đọc file thành công")
except Exception as e:
    logging.error(e)

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

    currency = ''
    if 'usd' in sal_str:
        currency = 'USD'
    else:
        currency = 'VND'

    if pd.isna(sal_str) or 'thỏa thuận' in sal_str:
        return np.nan, np.nan, np.nan

    if '-' in sal_str:
        sal_str = re.sub(r'trên|tới|triệu|usd|,', '', sal_str)
        val_parts = sal_str.split('-')
        min_value = float(val_parts[0].strip())
        max_value = float(val_parts[1].strip())
        if currency == 'VND':
            min_value *= 1000000
            max_value *= 1000000
            return min_value, max_value, currency
        else:
            return min_value, max_value, currency

    if re.search(r'trên', sal_str):
        sal_str = re.sub(r'trên|triệu|usd|,', '', sal_str)
        val = float(sal_str.strip())
        if currency == 'VND':
            val *= 1000000
            return val, np.nan, currency
        else:
            return val, np.nan, currency

    if re.search(r'tới', sal_str):
        sal_str = re.sub(r'tới|triệu|usd|,', '', sal_str)
        val = float(sal_str.strip())
        if currency == 'VND':
            val *= 1000000
            return np.nan, val, currency
        else:
            return np.nan, val, currency

    cleaned_str = re.sub(r'[^\d]', '', sal_str)
    if cleaned_str == '':
        return np.nan, np.nan, np.nan
    else:
        val = float(cleaned_str)
        if currency == 'VND':
            val *= 1_000_000
        return val, val, currency

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

# Ép kiểu cột created_date về dạng date
df['created_date'] = df['created_date'].apply(clean_date)
# Xử lý cột job_title
df['title'] = df['job_title'].apply(parse_title)
# Parse cột salary thành các cột min_salary, max_salary, currency
df[['min_salary','max_salary','currency']] = df['salary'].apply(lambda x: pd.Series(parse_salary(x)))
# Parse cột address thành cột city, district
df[['city','district']] = df['address'].apply(lambda x: pd.Series(parse_address(x)))
# Xuất file csv data đã clean
df = df[['created_date','title','company','min_salary','max_salary','currency','city','district','time','link_description']]
df.to_csv('cleaned_data.csv',index=False)

# Biểu đồ phân bố mức lương theo vị trí
df['avg_salary'] = df[['min_salary', 'max_salary']].mean(axis=1, skipna=True)
df.loc[df['currency'] == 'USD', 'avg_salary'] = df['avg_salary'] * 25
df.loc[df['currency'] == 'VND', 'avg_salary'] = df['avg_salary'] / 1000000
df_filtered = df[df['avg_salary'] <= 200]

plt.figure(figsize=(6, 3))
df_filtered.boxplot(column='avg_salary', by='title', grid=False, vert=False)
plt.title('Phân bố mức lương theo job title')
plt.suptitle('')  # bỏ dòng title phụ
plt.xlabel('Mức lương (triệu VND)')
plt.ylabel('Vị trí công việc')
plt.show()

# Biểu đồ phân bổ việc làm theo khu vực
df = df.assign(district=df['district'].str.split(', ')).explode('district')
df['district'] = df['district'].str.strip()
top_districts = df['district'].value_counts().nlargest(15).index
df_top = df[df['district'].isin(top_districts)]

heatmap_data = df_top.pivot_table(
    index='district',
    columns='title',
    values='company',
    aggfunc='count',
    fill_value=0
)
plt.figure(figsize=(6, 3))
sns.heatmap(heatmap_data, cmap='YlGnBu', annot=True, fmt='d')
plt.title('Phân bổ việc làm theo district và job title (top 15 district)')
plt.xlabel('Job_title')
plt.ylabel('District')
plt.tight_layout()
plt.show()

# Biểu đồ số lượng việc làm theo từng vị trí
job_counts = df['title'].value_counts()

plt.figure(figsize=(6,3))
sns.barplot(x=job_counts.values, y=job_counts.index, palette='viridis')
plt.title('Số lượng việc làm theo job title', fontsize=14)
plt.xlabel('Số lượng')
plt.ylabel('Job Title')
plt.show()

# Import file csv vào Postgresql
try:
    conn = psycopg2.connect('dbname=mydb user=quannh password=123456 host=localhost port=5432')
    cur = conn.cursor()

    create_table_query = '''
    CREATE TABLE IF NOT EXISTS job_data (
        created_date      TIMESTAMP,
        title             TEXT,
        company           TEXT,
        min_salary        DOUBLE PRECISION,
        max_salary        DOUBLE PRECISION,
        currency          TEXT,
        city              TEXT,
        district          TEXT,
        time              TEXT,
        link_description  TEXT
    );
    '''
    cur.execute(create_table_query)

    truncate_table_query = 'TRUNCATE TABLE job_data;'
    cur.execute(truncate_table_query)

    with open('cleaned_data.csv', 'r', encoding='utf-8') as f:
        # Bỏ dòng header
        # next(f)
        cur.copy_expert('''
            COPY job_data(
                created_date,
                title,
                company,
                min_salary,
                max_salary,
                currency,
                city,
                district,
                time,
                link_description
            )
            FROM STDIN WITH CSV HEADER DELIMITER ',';
        ''', f)

    conn.commit()

except Exception as e:
    conn.rollback()
    print('Error:', e)

finally:
    cur.close()
    conn.close()