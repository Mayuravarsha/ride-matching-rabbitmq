import json,os,pika,requests, time

consumer_ID = os.environ['CONSUMER_ID']
producer_addr = os.environ['PRODUCER_ADDRESS']

time.sleep(10)

response = requests.post(f'http://{producer_addr}/new_ride_matching_consumer',data=f'consumer_id={consumer_ID}')

assert response.status_code == 200

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))

channel = connection.channel()

channel.queue_declare(queue='ride_match', durable=True)

print(f'Consumer {consumer_ID} has started and is waiting for input', flush=True)


def callback(ch, method, properties, body):
    body = json.loads(body.decode())
    body['_id'] = properties.message_id
    print(f'Consumer ID: {consumer_ID} recieved {body} from ride_match queue and is now busy!! ', flush=True)
    time.sleep(int(body["time"]))
    print(f'Consumer ID: {consumer_ID} is now live', flush=True)
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='ride_match', on_message_callback=callback)

channel.start_consuming()