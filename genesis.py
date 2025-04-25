import asyncio
from neo4j import AsyncGraphDatabase
import redis.asyncio as redis
from src.models.tx import TxStore
from src.utils.data import hash_hex, ordered_sum
from pymongo import MongoClient, IndexModel, ASCENDING
import time
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from neo4j.exceptions import TransientError


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@retry(
    stop=stop_after_attempt(5),  # Максимум 3 попытки
    wait=wait_exponential(multiplier=1, min=0.1, max=2),  # Экспоненциальная задержка
    retry=retry_if_exception_type(TransientError),  # Повтор для TransientError
    reraise=True  # Проброс оригинальной ошибки при неудаче
)
async def main():
    # Инициализация подключений
    memgraph_driver = AsyncGraphDatabase.driver("bolt://localhost:7687")
    redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
    mongo_client = MongoClient('localhost', 27017)
    db = mongo_client.feo
    txs = db.tx

    # Очистка MongoDB
    txs.drop()
    logger.info("MongoDB collection 'tx' dropped")
    txs.create_indexes([IndexModel([("hash", ASCENDING)], unique=True)])
    txs.create_indexes([IndexModel([("credit", ASCENDING), ("uniq", 1)])])

    # Подключение к Redis
    await redis_client.flushall()
    logger.info("Redis flushed")

    # Определение генезис-транзакций
    genesis_txs = [
        TxStore(
            parents=["0", "1"],
            debit="9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy",
            credit="",
            amount=500000000000000,
            uniq="",
            sign="",
            msg="genesis"
        ),
        TxStore(
            parents=["0", "1"],
            debit="RvCcJWKYpoEXGm6N72kDj3dA8zTxGTMSBpCLSKcJvdR",
            credit="",
            amount=330000000000000,
            uniq="",
            sign="",
            msg="genesis2"
        ),
        TxStore(
            parents=["0", "1"],
            debit="2RAHCZg7SfyLY6x5uLmtDh1tNbZr4w5NTPMWXpHrqdyc",
            credit="",
            amount=250000000000000,
            uniq="",
            sign="",
            msg="genesis3"
        )
    ]

    try:
        async with memgraph_driver.session() as session:
            # Очистка Memgraph
            await session.run("MATCH (n) DETACH DELETE n")
            #await session.run("DROP GRAPH") memgraph --storage-mode=IN_MEMORY_ANALYTICAL
            logger.info("Memgraph cleared")

            # Добавление генезис-транзакций
            for tx in genesis_txs:
                tx_dict = dict(tx)
                tx_hash = hash_hex(ordered_sum(tx_dict))
                tx_dict["time"] = int(time.time_ns() / 1000)
                tx_dict["parents"] = tx.parents

                # Создание узла транзакции
                await session.run(
                    """
                    CREATE (t:Transaction {
                        hash: $hash,
                        own_weight: 1.0,
                        weight: 1.0,
                        data: $data
                    })
                    """,
                    hash=tx_hash,
                    data=tx_dict
                )

                # Установка баланса в Redis
                await redis_client.set(f"balance:{tx.debit}", tx.amount)
                logger.info(f"Created transaction {tx_hash} with balance {tx.amount} for {tx.debit}")
            
            await session.run("CREATE CONSTRAINT ON (t:Transaction) ASSERT t.hash IS UNIQUE")
            await session.run("CREATE INDEX ON :Transaction(hash)")

            # Вывод всех узлов для отладки
            result = await session.run("MATCH (t:Transaction) RETURN t")
            nodes = [record["t"] async for record in result]
            for node in nodes:
                logger.info(f"Node: {node}")

    except Exception as e:
        logger.error(f"Error in genesis setup: {str(e)}")
        raise

    finally:
        # Закрытие подключений
        await memgraph_driver.close()
        await redis_client.aclose()
        mongo_client.close()
        logger.info("All connections closed")

if __name__ == "__main__":
    asyncio.run(main())