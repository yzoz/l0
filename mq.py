import pika
import json
import sys
import os
import time
from pymongo import MongoClient
import hashlib
from src.models import Tx

cli = MongoClient('localhost', 27017)

db = cli.feo
txs = db.tx

def tob2b(t):
    h = hashlib.blake2b()
    h.update(bytes(t.encode("utf-8")))
    return h.digest().hex()

def on_message(ch, method, properties, tx):
    tx = tx.decode("utf-8")
    tx = json.loads(tx)
    tx = Tx(**tx)
    tx.time = time.time_ns()
    m = tx.credit + tx.debit + str(tx.amount) + str(tx.msg) + str(tx.time)
    this_hash = tob2b(m)
    last = txs.find_one({'$query': {}, '$orderby': {'_id': -1}})
    last_hash = last['hash']
    tx.hash = tob2b(last_hash + this_hash)

    result = txs.insert_one(dict(tx))
    if result.inserted_id:
        print(dict(tx))
        ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.basic_consume('tx', on_message)

    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)