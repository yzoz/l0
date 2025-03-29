from src.models.tx import TxWrite
from pymongo import MongoClient
import time

cli = MongoClient('localhost', 27017)

db = cli.feo
txs = db.tx

"""
C8xjbHktooH8Pm1n57hHprfBSb4kxSfTcDS3ZZKBCTCR
"""

tx = TxWrite(
        debit = "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy",
        credit = "0",
        amount = 1000000000000000,
        asset = "0",
        uniq = "0",
        sign = "0",
        v = 1,
        time = time.time_ns(),
        hash = "0",
        msg = ""
    )

result = txs.insert_one(dict(tx))
if result.inserted_id:
    print(dict(tx))