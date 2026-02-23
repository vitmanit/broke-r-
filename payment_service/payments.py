from fastapi import FastAPI, HTTPException
import pika
import json
import time

app = FastAPI()

def get_rabbit_connection():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    return connection

def callback(ch, method, propertie, body):
    print(f'[*] Получено сообщение {body}')
    message = json.loads(body)
    order_id = message['order_id']
    amount = message['amount']

    if amount > 0:
        status = 'paid'
    else:
        status = 'failed'

    print(f" [x] Заказ {order_id}: сумма {amount} -> статус {status}")

    try:
        connection = get_rabbit_connection()
        channel  = connection.channel()

        channel.queue_declare(queue='payment_result_queue', durable=True)

        response = {
            'order_id': order_id,
            'status': status,
        }

        channel.basic_publish(
            exchange='',
            routing_key='payment_result_queue',
            body=json.dumps(response),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        connection.close()
        print(f" [x] Событие order.payment_processed для заказа {order_id} опубликовано")

    except Exception as e:
        print(f" [!] Ошибка публикации результата: {e}")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    print(" [*] Payment Service запускается...")

    connection = get_rabbit_connection()
    channel = connection.channel()


    channel.queue_declare(queue='order_created_queue', durable=True)
    channel.queue_declare(queue='payment_result_queue', durable=True)


    channel.basic_qos(prefetch_count=1)


    channel.basic_consume(
        queue='order_created_queue',
        on_message_callback=callback
    )

    print(' [*] Ожидание событий order.created. Для выхода нажми CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n [*] Остановлено пользователем")