# api.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from src.database.mongodb import MongoDB
from src.database.queues import RabbitMQ
from src.database.redis import Redis
from src.api.routers import transactions, root, graphql, pool

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Инициализация подключений к БД и очередям
    app.state.db = MongoDB()
    app.state.rabbit = RabbitMQ()
    app.state.redis = Redis()
    await app.state.rabbit.connect()
    await app.state.db.connect()
    await app.state.redis.connect()
    yield
    # Закрытие подключений
    #await app.state.db.client.close()
    await app.state.rabbit.close()
    await app.state.redis.close()

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
