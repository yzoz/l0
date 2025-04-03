from pymongo import MongoClient
import networkx as nx
from pyvis.network import Network
import json
import pprint
pp = pprint.PrettyPrinter(depth=4)


cli = MongoClient('localhost', 27017)
db = cli.feo
tx = db.tx

# Новый pipeline с массивом parents
pipeline = [
    {
        "$match": {
            "msg": "genesis"
        }
    },
    {
        "$graphLookup": {
            "from": "tx",
            "startWith": "$parents",  # Используем массив parents
            "connectFromField": "hash",
            "connectToField": "parents",  # Ищем в массиве parents
            "as": "descendants",
            "depthField": "level",
            "maxDepth": 10
        }
    }
]

# Получение данных
result = list(tx.aggregate(pipeline))[0]
#pp.pprint(result)
print(f"Найдено потомков: {len(result.get('descendants', []))}")

G = nx.DiGraph()

levels = {}

# Добавляем потомков и рёбра
for descendant in result.get("descendants", []):
    child = descendant["hash"]
    level = descendant["level"]
    parents = descendant.get("parents", [])  # Список родителей
    G.add_node(child, amount=descendant["amount"], credit=descendant["credit"], level=level)
    for parent in parents:
        G.add_edge(parent, child)

print("Узлы:", len(G.nodes))
print("Рёбра:", len(G.edges))

# Удаляем пустые узлы
nodes_to_remove = [node for node in G.nodes if G.nodes[node] == {}]
for node in nodes_to_remove:
    G.remove_node(node)
print(f"Удалено пустых узлов: {len(nodes_to_remove)}")

# Инициализация pyvis
net = Network(height="1500px", width="100%", directed=True, notebook=False)
net.from_nx(G)

# Настройка узлов
for node in net.nodes:
    full_hash = node["id"]
    level = G.nodes[node["id"]]["level"]
    amount = G.nodes[node["id"]]["amount"]
    credit = G.nodes[node["id"]]["credit"]
    short_credit = f"{credit[:4]}...{credit[-4:]}"
    readable_label = f"{short_credit} ({amount if amount is not None else 'N/A'})"
    node["title"] = f"Hash: {full_hash}<br>Amount: {amount if amount is not None else 'N/A'}<br>Level: {level}"
    node["label"] = readable_label
    #node["size"] = max(10, min(amount / 10, 30)) if amount is not None else 10
    node["color"] = ["#97c2fc", "#ffb6c1", "#98fb98", "#dda0dd", "#f08080", "#20b2aa", 
                     "#ffdead", "#87cefa", "#ff69b4", "#9acd32"][level % 10]

# Настройки графа
options = {
    "layout": {
        "hierarchical": {
            "enabled": True,
            "levelSeparation": 200,
            "nodeSpacing": 120,
            "direction": "UD",
            "sortMethod": "directed"
        }
    },
    "physics": {
        "enabled": False
    },
    "edges": {
        "arrows": "to",
        "smooth": {"type": "continuous"}
    },
    "nodes": {
        "font": {"size": 12}
    }
}

net.set_options(json.dumps(options))
net.save_graph("multi_parent_graph.html")
print("Граф сохранён")