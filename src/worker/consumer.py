# src/worker/consumer.py
import pika
import json
import sys
import os
import time
from src.models.tx import Tx
from src.database.mongodb import MongoDB
from src.utils.hash import tob2b

db = MongoDB()

def on_message(ch, method, properties, tx):
    tx = tx.decode("utf-8")
    tx = json.loads(tx)
    print("Received transaction:", tx)  # Отладка
    tx = Tx(**tx)
    tx.time = time.time_ns()
    tx.v = 1
    m = tx.credit + tx.debit + str(tx.amount) + tx.asset + tx.uniq + tx.msg
    this_hash = tob2b(m)
    last = db.txs.find_one({'$query': {}, '$orderby': {'_id': -1}})
    last_hash = last['hash'] if last else "0"  # Исправлено
    print(f"Last hash: {last_hash}, This hash: {this_hash}")  # Отладка
    tx.hash = tob2b(last_hash + this_hash)

    result = db.insert_tx(tx)
    print("Insert result:", result)  # Отладка
    if result:
        print("Saved transaction:", dict(tx))
        ch.basic_ack(delivery_tag=method.delivery_tag)
    else:
        print("Failed to save transaction")  # Отладка


def main():
    from src.database.queues import consume_messages
    consume_messages(on_message)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)