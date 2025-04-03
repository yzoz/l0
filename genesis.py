from src.models.tx import TxStore
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
    parents = ["b","a"],
    debit = "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy",
    credit = "",
    amount = 500000000000000,
    uniq = "",
    sign = "",
    msg = "genesis"
), TxStore(
    parents = ["f","e"],
    debit = "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy",
    credit = "",
    amount = 330000000000000,
    uniq = "",
    sign = "",
    msg = "genesis2"
), TxStore(
    parents = ["g","i"],
    debit = "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy",
    credit = "",
    amount = 250000000000000,
    uniq = "",
    sign = "",
    msg = "genesis3"
)]

async def main():
    r = redis.Redis(host='localhost', db=0)
    r.flushall()
    for t in tx:
        d = dict(t)
        h = hash_hex(ordered_sum(dict(d)))
        d["time"] = int(time.time_ns() / 1000)
        m = json.dumps(dict(d))
        r.set(f"pool:{h}", m)
        r.zadd("unconfirmed", {h: 0})
        r.set(f"refs:{h}", "0")
        value = r.get(f"pool:{h}")
        w = json.loads(value.decode("utf-8"))
        print(w)
        #await r.delete(h)


if __name__ == "__main__":
    asyncio.run(main())