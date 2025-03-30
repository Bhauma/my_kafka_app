from kafka import KafkaProducer, KafkaConsumer
import time
import json

def produce_message():
    produce = KafkaProducer(bootstrap_servers='localhost:9092')
    message = {
        "text": "Hello, Kafka!",
        "timestamp": time.time(),
        "status": "success"
    }


    # Ручная сериализация в JSON и преобразование в байты
    json_message = json.dumps(message).encode('utf-8')
    produce.send('test-topic', json_message)
    produce.flush()
    print('Сообщение отправлено!', message)

def consume_message():
    consumer = KafkaConsumer('test-topic', 
                             bootstrap_servers='localhost:9092',
                             auto_offset_reset='earliest',
                             consumer_timeout_ms=5000)
    for message in consumer:
        # Ручная десериализация
        json_data = json.loads(message.value.decode('utf-8'))
        print('Сообщение получено:', json_data)
        print("Тип данных:", type(json_data))  # Будет <class 'dict'>
        time.sleep(1)

if __name__ == '__main__':
    produce_message()
    time.sleep(2)
    consume_message()
