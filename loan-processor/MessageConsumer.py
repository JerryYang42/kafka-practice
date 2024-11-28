from confluent_kafka import Consumer
import json

class MessageConsumer():
    def __init__(self):
        conf={'bootstrap.servers':'localhost:9092',
        'group.id':'loan_applications_consumer_group',
        'auto.offset.reset':'earliest'}
        self.consumer=Consumer(conf)
    
    def subscribe(self, topic):
        self.consumer.subscribe([topic])
    
    def poll(self, timeout):
        return self.consumer.poll(timeout)

    def close(self):
        self.consumer.close()
