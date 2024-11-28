# loan_producer.py
from confluent_kafka import Producer
import json
import time
import os
 
def read_loan_files(directory_path):
    """Read all loan message files from the directory"""
    loan_applications = []
   
    # Get all loan message files
    for filename in os.listdir(directory_path):
        if filename.startswith('loan_message_'):
            try:
                with open(os.path.join(directory_path, filename), 'r') as file:
                    loan_data = json.load(file)
                    loan_applications.append(loan_data)
                    print(f"Read file: {filename}")
            except Exception as e:
                print(f"Error reading {filename}: {e}")
   
    return loan_applications
 
def delivery_callback(err, msg):
    """Callback function to check if message was delivered"""
    if err:
        print(f'Error: {err}')
    else:
        print(f'Message sent: Customer ID = {msg.key().decode()}, Partition = {msg.partition()}')
 
def main():
    # Kafka configuration
    kafka_config = {
        'bootstrap.servers': 'localhost:9092'
    }
   
    # Create Producer instance
    producer = Producer(kafka_config)
   
    # Topic name
    topic = 'loan_applications'
   
    # Read loan applications
    loan_applications = read_loan_files('.')
    print(f"\nTotal applications found: {len(loan_applications)}")
    ###b'application['customer_id']'
    # Send each application to Kafka
    try:
        for application in loan_applications:
            # Prepare message
            message = json.dumps(application).encode('utf-8')
            key = str(application['customer_id']).encode('utf-8')
           
            # Send message
            producer.produce(
                topic=topic,
                key=key,
                value=message,
                callback=delivery_callback
            )
           
            # Flush and wait
            producer.flush()
            print(f"\nSent application for customer: {application['customer_id']}")
            print(f"Loan amount: {application['loan_application_amount']}")
            time.sleep(2)  # Wait for 2 seconds before next message
           
    except KeyboardInterrupt:
        print("\nStopped by user")
    except Exception as e:
        print(f"\nError: {e}")
    finally:
        print("\nClosing producer...")
        producer.flush()  # Flush any remaining messages
        print("Done!")
 
if __name__ == "__main__":
    main()
 
