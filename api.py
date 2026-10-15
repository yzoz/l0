from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from strawberry.fastapi import GraphQLRouter

from src.api.routers import pool, root, transactions
from src.api.schema import schema
from src.database.graph import Memgraph
from src.database.mongodb import MongoDB
from src.database.queues import RabbitMQ
from src.database.redis import Redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Инициализация подключений к БД и очередям
    app.state.db = MongoDB()
    app.state.rabbit = RabbitMQ()
    app.state.redis = Redis()
    app.state.graph = Memgraph()
    await app.state.rabbit.connect()
    await app.state.db.connect()
    await app.state.redis.connect()
    await app.state.graph.connect()
    yield
    # Закрытие подключений
    await app.state.db.close()
    await app.state.rabbit.close()
    await app.state.redis.close()
    await app.state.graph.close()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency для MongoDB
async def get_mongodb(request: Request):
    return request.app.state.db


# Dependency для Redis
async def get_redis(request: Request):
    return request.app.state.redis


# Настройка Strawberry GraphQL
async def get_context(mongodb: MongoDB = Depends(get_mongodb), redis: Redis = Depends(get_redis)):
    return {"db": mongodb, "redis": redis}


graphql_router = GraphQLRouter(schema, context_getter=get_context)

# Подключение роутеров
app.include_router(root.router)
app.include_router(transactions.router)
app.include_router(pool.router)
app.include_router(graphql_router, prefix="/graphql")
