# read messages from the file prefixed "loan_message_" under `data` directory
# each file is in a format of json string
# send the json string to the Kafka topic "loan-application"
# then archive the file to the `archive` directory
import os
from confluent_kafka import Producer, KafkaError
import shutil

print("Created the producer")

class MessageFeeder():
    def __init__(self, dir):
        self.dir = dir
        self.producer = Producer({'bootstrap.servers': 'localhost:9092'})

    def feed_messages(self, topic):
        for file in os.listdir('data'):
            if file.startswith('loan_message_'):
                with open(f'{self.dir}/{file}', 'r') as f:
                    message = f.read()
                    self.producer.produce(topic, message)
                    self.producer.flush()
                    self.archive_message(file)

    def archive_message(self, file):
        shutil.move(f'data/{file}', f'archive/{file}')
