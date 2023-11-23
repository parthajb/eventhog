from flask import Flask, request, jsonify
from confluent_kafka import Producer
import json
import os

kafka_broker = os.environ.get('KAFKA_BROKER', 'kafka:9092')


app = Flask(__name__)

producer = Producer({'bootstrap.servers': kafka_broker})

@app.route('/produce', methods=['POST'])
def produce():
    try:
        data = request.get_json()
        topic = 'your_topic_name'  # Change this to your Kafka topic
        producer.produce(topic, key=None, value=json.dumps(data).encode('utf-8'))
        producer.flush()
        return jsonify({'success': True, 'message': 'Message sent to Kafka'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
