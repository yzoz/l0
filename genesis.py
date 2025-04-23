from src.models.tx import TxStore
import asyncio
from src.database.redis import Redis
import time
from src.utils.data import hash_hex, ordered_sum
from src.utils.graph import Dag
import networkx as nx

from pymongo import MongoClient
cli = MongoClient('localhost', 27017)
db = cli.feo
txs = db.tx
txs.drop()

redis = Redis()
dag = Dag()
G = nx.DiGraph()

tx = [TxStore(
    parents = ["0","1"],
    debit = "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy",
    credit = "",
    amount = 500000000000000,
    uniq = "",
    sign = "",
    msg = "genesis"
), TxStore(
    parents = ["0","1"],
    debit = "RvCcJWKYpoEXGm6N72kDj3dA8zTxGTMSBpCLSKcJvdR",
    credit = "",
    amount = 330000000000000,
    uniq = "",
    sign = "",
    msg = "genesis2"
), TxStore(
    parents = ["0","1"],
    debit = "2RAHCZg7SfyLY6x5uLmtDh1tNbZr4w5NTPMWXpHrqdyc",
    credit = "",
    amount = 250000000000000,
    uniq = "",
    sign = "",
    msg = "genesis3"
)]

async def main():
    await redis.connect()
    await redis.client.flushall()
    for t in tx:
        d = dict(t)
        h = hash_hex(ordered_sum(dict(d)))
        d["time"] = int(time.time_ns() / 1000)
        m = dict(d)
        G.add_node(h, weight=0.0, data=m)
        await dag.save(redis, G)
        #await asyncio.sleep(1.0)
        await dag.load(redis)
    print(dag.G.nodes(data=True))

if __name__ == "__main__":
    asyncio.run(main())