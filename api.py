# api.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from src.database.mongodb import MongoDB
from src.database.queues import RabbitMQ
from src.api.routers import transactions, root, graphql, pool
import redis.asyncio as redis

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Инициализация подключений к БД и очередям
    app.state.db = MongoDB()
    app.state.rabbit = RabbitMQ(host='localhost', queue_name='tx')
    await app.state.rabbit.connect()
    app.state.pool = redis.Redis(host='localhost', db=0)
    app.state.home = redis.Redis(host='localhost', db=1)
    yield
    # Закрытие подключений
    app.state.db.client.close()
    await app.state.db.async_client.close()
    await app.state.rabbit.close()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение роутеров
app.include_router(root.router)
app.include_router(transactions.router)
app.include_router(pool.router)
app.include_router(graphql.router, prefix="/graphql")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
