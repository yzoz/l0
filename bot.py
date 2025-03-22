import time
import random
import json
from nacl.signing import SigningKey
import asyncio
import httpx
import base58

client = httpx.AsyncClient()

sk = ['DF3998wbqyBn9uSA4BcMJQAkpnZb21a1vRLo9Nw8GwWy', '8no9RFLqKkC5HSLmSqkTnrX6pD2Fn5MNNbJmdGP1GivQ', '6savWyJQgcZEfDBt9N1u2dhPg68cng43zQFKgyM9qKDW']
pk = ['EDs139V82BerHAYyJNNzxniWPE7VETnxuw4nuAxk9fU1', 'A9huRv7vtELAYQ4nQYtEWfn5NEdqXBUs722TdsiZR3ST', '2RAHCZg7SfyLY6x5uLmtDh1tNbZr4w5NTPMWXpHrqdyc']

async def arange(count):
    for i in range(count):
        yield(i)
        await asyncio.sleep(0.0)

async def send():
    async for _ in arange(1000):
        i = random.randint(0, 2)
        c = pk[i]
        priv = sk[i]
        d = pk[random.randint(0, 2)]
        a = float(random.randint(100, 110))
        u = str(round(time.time() * 1000))
        m = str(random.randint(0, 100000))
        h = c + d + str(a) + "0" + u + m

        priv = base58.b58decode(priv)
        signing_key = SigningKey(priv)
        message_bytes = bytes(h.encode("utf-8"))
        signed = base58.b58encode(signing_key.sign(message_bytes))
        s = signed.decode("utf-8")
        tx = {
                'credit': c,
                'debit': d,
                'amount': a,
                'asset': "0",
                'uniq': u,
                'sign': s,
                'msg': m
        }
        print(tx)
        r = await client.post('http://127.0.0.1:8000/send/', data=json.dumps(tx))
        print(r.text)

async def main():
    await asyncio.gather(*[send() for i in range(50)])

if __name__ == '__main__':
    
    asyncio.run(main()) # creats an envent loop