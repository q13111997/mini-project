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
        min_value = float(val_parts[0].strip()) * 1000000
        max_value = float(val_parts[1].strip()) * 1000000
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

    val = float(re.sub(r"[^\d]", "", sal_str))
    if currency == "VND":
        val *= 1_000_000
    return val, val, currency

a = parse_salary('70 trieu')
print(a)



    # if pd.isna(sal_str) or 'thỏa thuận' in sal_str:
    #     return np.nan, np.nan, np.nan
    # elif re.search(r'trên',sal_str):
    #     sal_str = re.sub(r'trên|triệu|,','',sal_str)
    #     if 'usd' in sal_str:
    #         return float(sal_str.replace('usd','')), np.nan, 'USD'
    #     else:
    #         return float(sal_str.replace('usd',''))*1000000, np.nan, 'VND'
    # elif re.search(r'tới',sal_str):
    #     sal_str = re.sub(r'tới|triệu|,','',sal_str)
    #     if 'usd' in sal_str:
    #         return np.nan, float(sal_str.replace('usd','')), 'USD'
    #     else:
    #         return np.nan, float(sal_str.replace('usd',''))*1000000, 'VND'
    # elif '-' in sal_str:
    #     sal_str = re.sub(r'trên|tới|triệu|,', '', sal_str)
    #     min_val, max_val = sal_str.split('-')
    #     if 'usd' in sal_str:
    #         return float(min_val.replace('usd','').strip()), float(max_val.replace('usd','').strip()), 'USD'
    #     else:
    #         return float(min_val.replace('usd','').strip())*1000000, float(max_val.replace('usd','').strip())*1000000, 'VND'