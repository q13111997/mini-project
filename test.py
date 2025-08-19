import pandas as pd
from collections import Counter
import re
import numpy as np

a = 'Abc: CDe'
lst = [x.strip().lower() for x in a.split(':')]
lst.remove('abc')
print(lst)
def parse_address(add_str):
    lst_str = add_str.split(':')
    if len(lst_str) > 1:
        city, district = lst_str
    else:
        city = lst_str[0]
        district = np.nan
    return city, district

x,y = parse_address(a)

a = 'BASDFG'
a= a.lower()
print(a)


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

a = re.search(
            r'\b(?:lập trình|net|smartcontract|brse|kỹ sư|triển khai|java|engineer|angularjs|laravel|php|mobile|ios|'
            r'nodejs|android|front|back|end|python|react|delivery|ux|technical|dev|công nghệ thông tin|big data|ruby|'
            r'data scientist|enigneer|fullstack|data|blockchain|phần mềm)\b',
            'dev/Sre - Chuyên Viên Quản Trị Hệ Thống/Automation'
    )
print(a)
if a:
    print(1)
else:
    print(0)