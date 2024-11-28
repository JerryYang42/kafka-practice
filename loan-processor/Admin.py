# create connection to Kafka

# Create topics in Kafka
from confluent_kafka.admin import AdminClient, NewTopic

class Admin():
    def __init__(self):
        self.admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})

    def create_topic(self, topic_name):
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        self.admin_client.create_topics([new_topic])

    def delete_topic(self, topic_name):
        self.admin_client.delete_topics([topic_name])

    def list_topics(self):
        return self.admin_client.list_topics().topics
        