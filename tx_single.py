import asyncio
import httpx
from src.models.tx import Tx
import uuid
import random
from src.utils.data import ordered_sum
from src.utils.crypto import sign_message
import json

client = httpx.AsyncClient()

sk = [
    'DF3998wbqyBn9uSA4BcMJQAkpnZb21a1vRLo9Nw8GwWy',
    '8no9RFLqKkC5HSLmSqkTnrX6pD2Fn5MNNbJmdGP1GivQ',
    '6savWyJQgcZEfDBt9N1u2dhPg68cng43zQFKgyM9qKDW',
    'C8xjbHktooH8Pm1n57hHprfBSb4kxSfTcDS3ZZKBCTCR',
    'GtTXBxARCLoRUFcmv5QtU6L1neQkRdivRXCEMuigKba5'
]
pk = [
    'EDs139V82BerHAYyJNNzxniWPE7VETnxuw4nuAxk9fU1',
    'A9huRv7vtELAYQ4nQYtEWfn5NEdqXBUs722TdsiZR3ST',
    '2RAHCZg7SfyLY6x5uLmtDh1tNbZr4w5NTPMWXpHrqdyc',
    '9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy',
    'RvCcJWKYpoEXGm6N72kDj3dA8zTxGTMSBpCLSKcJvdR'
]

async def main():
    p = await client.get('http://127.0.0.1:8000/pool')
    p = p.json()
    c = Tx(
        mom = p["mom"],
        dad = p["dad"],
        debit = pk[4],
        credit = pk[3],
        amount = random.randint(1, 1000000000),
        uniq = str(uuid.uuid4()),
        msg = str(random.randint(0, 100000))
    )
    m = ordered_sum(dict(c))
    s = sign_message(sk[3], m)
    tx = {
        "content": dict(c),
        "sign": s
    }
    print(tx)
    r = await client.post('http://127.0.0.1:8000/send/', data=json.dumps(tx))
    print(r.text)

if __name__ == "__main__":
    asyncio.run(main())