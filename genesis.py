from src.models.tx import TxStore, TxWrite
import asyncio
import redis
import json
import time
from src.utils.data import hash_hex, ordered_sum
from pymongo import MongoClient
cli = MongoClient('localhost', 27017)

db = cli.feo
txs = db.tx
txs.drop()

tx = [TxStore(
    mom = "",
    dad = "",
    debit = "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy",
    credit = "",
    amount = 500000000000000,
    uniq = "",
    sign = "",
    msg = ""
), TxStore(
    mom = "",
    dad = "",
    debit = "RvCcJWKYpoEXGm6N72kDj3dA8zTxGTMSBpCLSKcJvdR",
    credit = "",
    amount = 500000000000000,
    uniq = "",
    sign = "",
    msg = ""
)]

async def main():
    p = redis.Redis(host='localhost', db=0)
    r = redis.Redis(host='localhost', db=1)
    p.flushall()
    for t in tx:
        d = dict(t)
        h = hash_hex(ordered_sum(dict(d)))
        d["time"] = int(time.time_ns() / 1000)
        m = json.dumps(dict(d))
        p.set(h, m)
        p.zadd("unconfirmed", {h: 0})
        r.set(h, "0")
        value = p.get(h)
        w = json.loads(value.decode("utf-8"))
        print(w)
        #await r.delete(h)


if __name__ == "__main__":
    asyncio.run(main())