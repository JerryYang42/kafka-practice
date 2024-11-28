# loan_consumer.py
from confluent_kafka import Consumer
import pandas as pd
import json
from datetime import datetime
 
def load_credit_data(filepath):
    """Load credit scores and salary data"""
    try:
        df = pd.read_csv(filepath)
        # Convert to dictionary for faster lookups
        return df.set_index('customer_id').to_dict('index')
    except Exception as e:
        print(f"Error loading credit data: {e}")
        return {}
 
def load_existing_loans(filepath):
    """Load existing loans data"""
    try:
        df = pd.read_csv(filepath)
        # Group by customer_id
        return df[df['open'] == 1].groupby('customer_id')['loan_amount'].sum().to_dict()
    except Exception as e:
        print(f"Error loading loan data: {e}")
        return {}
 
def validate_loan(application, credit_data, existing_loans):
    """Validate loan application based on rules"""
    customer_id = application['customer_id']
    loan_amount = application['loan_application_amount']
   
    # Check if customer exists in credit data
    if customer_id not in credit_data:
        return False, "Customer not found in credit database"
   
    # Rule 1: Credit score check
    if credit_data[customer_id]['credit_score'] < 700:
        return False, "Credit score below 700"
   
    # Rule 2: Total loan amount check
    monthly_salary = credit_data[customer_id]['monthly_salary']
    existing_loan_amount = existing_loans.get(customer_id, 0)
    total_loan_amount = existing_loan_amount + loan_amount
   
    if total_loan_amount > (monthly_salary * 20):
        return False, "Total loan amount exceeds 20 times monthly salary"
   
    return True, "Application approved"
 
def main():
    # Kafka configuration
    config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'loan_processing_group',
        'auto.offset.reset': 'earliest'
    }
   
    # Create Consumer instance
    consumer = Consumer(config)
   
    # Subscribe to topic
    topic = 'loan_applications'
    consumer.subscribe([topic])
   
    # Load reference data
    print("Loading reference data...")
    credit_data = load_credit_data('data/customer_credit_score.txt')
    existing_loans = load_existing_loans('data/existing_loans.txt')
    print("Reference data loaded successfully")
   
    try:
        while True:
            msg = consumer.poll(1.0)  # Wait for message
           
            if msg is None:
                continue
           
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
           
            try:
                # Parse message
                application = json.loads(msg.value().decode('utf-8'))
                customer_id = application['customer_id']
                loan_amount = application['loan_application_amount']
               
                print(f"\nProcessing application:")
                print(f"Customer ID: {customer_id}")
                print(f"Loan Amount: ${loan_amount:,.2f}")
               
                # Validate application
                approved, reason = validate_loan(application, credit_data, existing_loans)
               
                # Print result
                if approved:
                    print(f"APPROVED: {reason}")
                    # Add additional approval details
                    if customer_id in credit_data:
                        print(f"Credit Score: {credit_data[customer_id]['credit_score']}")
                        print(f"Monthly Salary: ${credit_data[customer_id]['monthly_salary']:,.2f}")
                else:
                    print(f"REJECTED: {reason}")
               
                print("-" * 50)
               
            except json.JSONDecodeError as e:
                print(f"Error decoding message: {e}")
            except Exception as e:
                print(f"Error processing message: {e}")
               
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        # Close consumer
        consumer.close()
        print("Consumer closed")
 
if __name__ == "__main__":
    main()
 