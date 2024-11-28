import pandas as pd
from pandas import DataFrame
from MessageConsumer import MessageConsumer
import json

class LoanProcessor():
    def __init__(self, loan: DataFrame, credit_score: DataFrame, consumer: MessageConsumer):
        self.loan = loan
        self.credit_score = credit_score
        self.consumer = consumer

    def process_loan(self):
        try:
            while True:
                # take in message from Kafka
                msg=self.consumer.poll(1.0)
        
                if msg is None:
                    continue
                if msg.error():
                    print(" There is an error")
                    continue
                try:
                    loan_application=json.loads(msg.value())
                    # { "customer_id": 10009, "loan_application_amount": 19630 }
                    customer_id = loan_application['customer_id']
                    loan_applied = loan_application['loan_application_amount']
                    print("customer_id: ", customer_id, type(customer_id), "loan_application_amount: ", loan_applied, type(loan_applied))
                    result = self.apply_rules(customer_id, loan_applied)
                    print(result)
                except json.JSONDecodeError as e:
                    print("fails in reading the message", e)
        except KeyboardInterrupt:
            print("shutting down the Consumer")
        finally:
            self.consumer.close()

    def apply_rules(self, customer_id, loan_applied):
        # apply rules
        # rule 1
        credit_score = self.credit_score[self.credit_score['customer_id'] == customer_id]['credit_score'][0]
        if credit_score < 700:
            return "Loan rejected"
        # rule 2
        salary = self.credit_score[self.credit_score['customer_id'] == customer_id]['monthly_salary'][0]
        existing_loan = self.loan[self.loan['customer_id'] == customer_id]['loan_amount'][0]
        total_loan = existing_loan + loan_applied
        if total_loan > salary * 20:
            return "Loan rejected"

        return "Loan approved"
        