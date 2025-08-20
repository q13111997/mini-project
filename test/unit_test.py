import sys
import os
import numpy as np
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from mini_project import parse_title, parse_salary, parse_address

def test_parse_title():
    assert parse_title('Thực Tập Sinh Lập Trình Viên .Net') == 'Developer / Engineer'
    assert parse_title('Secretary - Salary Up To 15M') == 'Other'
    assert parse_title('Devops/Sre - Chuyên Viên Quản Trị Hệ Thống/Automation') == 'IT Operations / DevOps / Support'
    assert parse_title('IT Business Analyst') == 'Business / Analyst / Product'
    assert parse_title('Tester Onsite') == 'Tester / QA'

def test_parse_salary():
    assert parse_salary('15 - 25 triệu') == (15000000, 25000000, 'VND')
    assert parse_salary('Thoả thuận') == (np.nan, np.nan, np.nan)
    assert parse_salary('Trên 10 triệu') == (10000000, np.nan,'VND')
    assert parse_salary('800 - 2,000 USD') == (800, 2000,'USD')
    assert parse_salary('Tới 10 triệu') == (np.nan, 10000000, 'VND')
    assert parse_salary('Tới 3000 USD') == (np.nan, 3000, 'USD')

def test_parse_address():
    assert parse_address('Hồ Chí Minh: Tân Bình') == ('Hồ Chí Minh', 'Tân Bình')
    assert parse_address('Nước Ngoài: Hà Nội: Nam Từ Liêm') == ('Hà Nội, Nước Ngoài', 'Nam Từ Liêm')
    assert parse_address('Thừa Thiên Huế: TP Huế: Hà Nội: Cầu Giấy') == ('Thừa Thiên Huế, Hà Nội', 'TP Huế, Cầu Giấy')
    assert parse_address('Hồ Chí Minh: Quận 9: Hà Nội: Cầu Giấy: Đà Nẵng: Ngũ Hành Sơn: Thừa Thiên Huế: TP Huế') == ('Hồ Chí Minh, Hà Nội, Đà Nẵng, Thừa Thiên Huế', 'Quận 9, Cầu Giấy, Ngũ Hành Sơn, TP Huế')