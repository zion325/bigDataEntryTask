from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_HOST = "localhost"  # 服务器端口地址
KAFKA_PORT = 9092  # 端口号
KAFKA_TOPIC = "entrytask-mockdata-order"  # topic


class Kafka_producer():
    def __init__(self, kafka_host, kafka_port, kafka_topic):
        self.kafkaHost = kafka_host
        self.kafkaPort = kafka_port
        self.kafkaTopic = kafka_topic
        self.producer = KafkaProducer(bootstrap_servers='{kafka_host}:{kafka_port}'.format(
            kafka_host=self.kafkaHost,
            kafka_port=self.kafkaPort
        ))

    def send_data_json(self, data):
        try:
            message = data
            producer = self.producer
            producer.send(self.kafkaTopic, value=message.encode('utf-8'))
            producer.flush()
        except KafkaError as e:
            print(e)


def log_kafka(params):
    print("================producer:=================\n")
    print(params, "\n")
    producer = Kafka_producer(kafka_host=KAFKA_HOST, kafka_port=KAFKA_PORT, kafka_topic=KAFKA_TOPIC)
    producer.send_data_json(params)
