import pandas as pd
import sys
import os
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from mini_project import parse_title, parse_salary, parse_address

def test_parse_title():
    df = pd.DataFrame({
        'job_title': [
            'Thực Tập Sinh Lập Trình Viên .Net',
            'Secretary - Salary Up To 15M',
            'Devops/Sre - Chuyên Viên Quản Trị Hệ Thống/Automation',
            'IT Business Analyst'
        ]
    })

    result = parse_title(df)

    assert 'job_title_clean' in result.columns
    assert parse_title(df['job_title'][0]) == 'Developer / Engineer'
    # assert result.loc[1, 'job_title_clean'] == 'Other'
    # assert result.loc[2, 'job_title_clean'] == 'IT Operations / DevOps / Support'
    # assert result.loc[3, 'job_title_clean'] == 'Tester / QA'