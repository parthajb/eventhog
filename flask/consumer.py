from confluent_kafka import Consumer, KafkaException
import sys
import json

def kafka_consumer():
    # Consumer configuration
    conf = {
        'bootstrap.servers': 'localhost:9092',  # Change this to your Kafka broker address
        'group.id': 'mygroup',  # Change this to your consumer group
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    topic = 'your_topic_name'  # Change this to your Kafka topic

    try:
        consumer.subscribe([topic])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Proper message
                print(f"Received message: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == '__main__':
    kafka_consumer()
