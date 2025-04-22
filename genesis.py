from src.models.tx import TxStore
import asyncio
from src.database.redis import Redis
import json
import time
from src.utils.data import hash_hex, ordered_sum
from src.utils.graph import Dag

from pymongo import MongoClient
cli = MongoClient('localhost', 27017)
db = cli.feo
txs = db.tx
txs.drop()

redis = Redis()
dag = Dag()

tx = [TxStore(
    parents = ["0","1","2"],
    debit = "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy",
    credit = "",
    amount = 500000000000000,
    uniq = "",
    sign = "",
    msg = "genesis"
), TxStore(
    parents = ["0","1","2"],
    debit = "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy",
    credit = "",
    amount = 330000000000000,
    uniq = "",
    sign = "",
    msg = "genesis2"
), TxStore(
    parents = ["0","1","2"],
    debit = "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy",
    credit = "",
    amount = 250000000000000,
    uniq = "",
    sign = "",
    msg = "genesis3"
)]

async def main():
    await redis.connect()
    await redis.client.flushall()
    await dag.load(redis)
    for t in tx:
        d = dict(t)
        h = hash_hex(ordered_sum(dict(d)))
        d["time"] = int(time.time_ns() / 1000)
        m = json.dumps(dict(d))
        dag.G.add_node(h, weight=0.0)
        await redis.client.set(f"tx:{h}", m)
        await dag.save(redis, dag.G)
        await dag.load(redis)
    print(dag.G.nodes(data=True))

if __name__ == "__main__":
    asyncio.run(main())