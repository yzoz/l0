from src.models.tx import Tx
from pymongo import MongoClient
import time

cli = MongoClient('localhost', 27017)

db = cli.feo
txs = db.tx

"""
PRIV: DF3998wbqyBn9uSA4BcMJQAkpnZb21a1vRLo9Nw8GwWy
PUB: EDs139V82BerHAYyJNNzxniWPE7VETnxuw4nuAxk9fU1
"""

tx = Tx(
        debit = "EDs139V82BerHAYyJNNzxniWPE7VETnxuw4nuAxk9fU1",
        credit = "0",
        amount = 1000000,
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