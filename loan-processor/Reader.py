import pandas as pd

# read csv from `data/customer_credit_score.txt`
# sample data:
# customer_id,credit_score,monthly_salary
# 10001,820,5000

def customer_credit_score():
    return pd.read_csv('data/customer_credit_score.txt', sep=',')

# read csv from `data/existing_loans.txt`
# sample data:
# customer_id,loan_amount,loan_date,emi,interest_rate,open
# 10001,20500,2023-01-07,1010,4.5,1
def existing_loans():
    return pd.read_csv('data/existing_loans.txt', sep=',')
