from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from src.database.mongodb import MongoDB
from src.database.queues import RabbitMQ
from src.api.routers import transactions, root, graphql

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = MongoDB()
    app.state.rabbit = RabbitMQ(host='localhost', queue_name='tx')
    await app.state.rabbit.connect()
    yield
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

app.include_router(transactions.router, tags=["transactions"])
app.include_router(root.router)
app.include_router(graphql.router, prefix="/graphql")