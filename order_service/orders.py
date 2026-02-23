from fastapi import FastAPI
import pika
import json
import threading
import time
from datetime import datetime

app = FastAPI()

orders_db = {}
id_counter = 0


def get_rabbit_connection():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    return connection


# ========== Функция для прослушивания результатов ==========
def listen_for_payment_results():
    def callback(ch, method, properties, body):
        try:
            message = json.loads(body)
            order_id = message['order_id']
            new_status = message['status']

            if order_id in orders_db:
                old_status = orders_db[order_id]['status']
                orders_db[order_id]['status'] = new_status
                print(f" [✓] Заказ {order_id}: статус обновлен с {old_status} на {new_status}")
            else:
                print(f" [!] Получен результат для неизвестного заказа {order_id}")

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f" [!] Ошибка обработки результата: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag)

    # Бесконечный цикл переподключения
    while True:
        try:
            print(" [*] Подключаемся к RabbitMQ для прослушивания результатов...")
            connection = get_rabbit_connection()
            channel = connection.channel()

            channel.queue_declare(queue='payment_result_queue', durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue='payment_result_queue',
                on_message_callback=callback
            )

            print(" [*] Слушаем результаты оплаты. Ожидаем сообщения...")
            channel.start_consuming()

        except Exception as e:
            print(f" [!] Ошибка подключения: {e}. Переподключаемся через 5 секунд...")
            time.sleep(5)


# ========== Создаем очередь при старте ==========
try:
    connection = get_rabbit_connection()
    channel = connection.channel()
    channel.queue_declare(queue='order_created_queue', durable=True)
    channel.queue_declare(queue='payment_result_queue', durable=True)
    connection.close()
    print(" [*] Очереди проверены/созданы")
except Exception as e:
    print(f" [!] Не удалось подключиться к RabbitMQ: {e}")

# ========== ЗАПУСКАЕМ ПОТОК СЛУШАТЕЛЯ ==========
print(" [*] Запускаем поток для прослушивания результатов...")
listener_thread = threading.Thread(target=listen_for_payment_results, daemon=True)
listener_thread.start()
print(" [*] Поток слушателя запущен")


# ========== РУЧКА СОЗДАНИЯ ЗАКАЗА ==========
@app.post('/orders')
async def create_order(user_id: int, amount: int):
    global id_counter

    id_counter += 1
    new_id = id_counter

    new_order = {
        'id': new_id,
        'user_id': user_id,
        'amount': amount,
        'status': 'created',
        'created_at': datetime.now().isoformat()
    }

    orders_db[new_id] = new_order
    print(f" [*] Заказ {new_id} создан со статусом created")

    # Публикуем событие
    try:
        connection = get_rabbit_connection()
        channel = connection.channel()
        channel.queue_declare(queue='order_created_queue', durable=True)

        message = {
            'order_id': new_id,
            'amount': amount
        }

        channel.basic_publish(
            exchange='',
            routing_key='order_created_queue',
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        connection.close()
        print(f" [x] Событие order.created для заказа {new_id} опубликовано")

    except Exception as e:
        print(f" [!] Ошибка публикации: {e}")

    return new_order