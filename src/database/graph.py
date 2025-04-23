from src.database.redis import Redis
import networkx as nx
import pickle

class Dag:
    def __init__(self):
        self.G = nx.DiGraph()
        
    # Функция для загрузки графа из Redis
    async def load(self, redis: Redis) -> nx.DiGraph:
        graph_data = await redis.client.get("dag")
        if graph_data:
            self.G = pickle.loads(graph_data)

    # Функция для сохранения графа в Redis
    async def save(self, redis: Redis, G: nx.DiGraph):
        await redis.client.set("dag", pickle.dumps(G))
