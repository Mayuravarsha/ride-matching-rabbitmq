import pika, pymongo , time,json


time.sleep(10)

client = pymongo.MongoClient("mongodb://mongodb:27017")
database_mongo = client['ride_matching']

collection =database_mongo['ride_details']

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='database', durable=True)
print('Databse is now live and is ready to take input!!!', flush=True)


def callback(ch, method, properties, body):
    body = json.loads(body)
    body['_id'] = properties.message_id

    print(f'Database consumed {body} from the database queue', flush=True)

    collection.insert_one(body)

    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='database', on_message_callback=callback)

channel.start_consuming()