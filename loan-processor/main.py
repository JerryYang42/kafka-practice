from Reader import customer_credit_score, existing_loans
from LoanProcessor import LoanProcessor
from MessageFeeder import MessageFeeder
from Admin import Admin
from MessageConsumer import MessageConsumer

df_credit_score = customer_credit_score()
df_loans = existing_loans()
consumer = MessageConsumer()
loan_processor = LoanProcessor(df_loans, df_credit_score, consumer)
loan_processor.apply_rules(10004, 22000)

def create_topic(topic):
    admin = Admin()
    admin.create_topic(topic)
    topics = admin.list_topics()
    print("topics: ", topics)

def feed_messages(dir, topic):
    feeder = MessageFeeder(dir)
    feeder.feed_messages(topic)

def process_loan_applications(topic):
    df_credit_score = customer_credit_score()
    df_loans = existing_loans()

    consumer = MessageConsumer()
    consumer.subscribe(topic)

    loan_processor = LoanProcessor(df_loans, df_credit_score, consumer)

    loan_processor.process_loan()

def main():
    topic = 'loan-applications'
    input_json_parent_folder='data'
    create_topic(topic)
    feed_messages(input_json_parent_folder, topic)
    process_loan_applications(topic)

def debug():
    # result = loan_processor.apply_rules(10001, 5000 * 20 - 20500+1)
    # print("result: ", result)
    df_credit_score = customer_credit_score()
    df_loans = existing_loans()

    consumer = MessageConsumer()
    loan_processor = LoanProcessor(df_loans, df_credit_score, consumer)
    loan_processor.apply_rules(10004, 22000)

if __name__ == "__main__":
    # main()
    debug()

    

    
    

    



