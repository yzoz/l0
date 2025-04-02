from pymongo import MongoClient
cli = MongoClient('localhost', 27017)
import networkx as nx
import matplotlib.pyplot as plt
from pyvis.network import Network
import json

db = cli.feo
tx = db.tx

pipeline = [
    {
        "$match": {"hash": "a2efb96d5f53d1d8fe185e9abda6a1c802719008991cb06e5bcfcff5c69d1e08db5841bd70ae3d7fee972cb6bb2dcb2dd47ed1798efb005413451be91ae04ac2"}  # Находим стартовую транзакцию
    },
    {
        "$project": {  # Создаём массив родителей
            "parents": {
                "$filter": {
                    "input": ["$mom", "$dad"],
                    "cond": {"$ne": ["$$this", None]}
                }
            }
        }
    },
    {
        "$graphLookup": {  # Поиск по mom
            "from": "tx",
            "startWith": "$parents",
            "connectFromField": "hash",
            "connectToField": "mom",
            "as": "descendants_mom",
            "depthField": "level_mom",
            "maxDepth": 10
        }
    },
    {
        "$graphLookup": {  # Поиск по dad
            "from": "tx",
            "startWith": "$parents",
            "connectFromField": "hash",
            "connectToField": "dad",
            "as": "descendants_dad",
            "depthField": "level_dad",
            "maxDepth": 10
        }
    },
    {
        "$project": {
            "hash": 1,
            "credit": 1,
            "amount": 1,
            "descendants": {
                "$setUnion": ["$descendants_mom", "$descendants_dad"]
            }
        }
    }
]

# Получение данных
result = list(tx.aggregate(pipeline))[0]  # Берём первый документ
print(f"Найдено потомков: {len(result.get('descendants', []))}")
#print(result)

"""for doc in result:
    import json
    print(json.dumps(doc, indent=2, default=str))"""

G = nx.DiGraph()
G.add_node("1", amount=1, credit=1, level=0)

levels = {"1": 0}  # Инициализируем корень

# Добавляем потомков и рёбра

for descendant in result.get("descendants", []):
    child = descendant["hash"]
    mom = descendant["mom"]
    dad = descendant["dad"]
    # Вычисляем уровень на основе родителей
    level = min(
        descendant.get("level_mom", float("inf")) if mom else float("inf"),
        descendant.get("level_dad", float("inf")) if dad else float("inf")
    )
    if level == float("inf"):
        level = 0
    print(child, mom, dad)
    G.add_node(child, amount=descendant["amount"], credit=descendant["credit"], level=level)
    """level = float("inf")
    if mom and mom in levels:
        level = min(level, levels[mom] + 1)
    if dad and dad in levels:
        level = min(level, levels[dad] + 1)
    if level == float("inf"):
        level = 0  # Если нет родителей в графе, уровень 0 (хотя это редкий случай)
    
    levels[child] = level
    G.add_node(child, amount=descendant["amount"], credit=descendant["credit"], level=level)"""
    if mom:
        G.add_edge(mom, child)
    if dad:
        G.add_edge(dad, child)

"""for g in G.nodes:
    print(G.nodes[g])"""
# Инициализация pyvis
"""net = Network(height="1500px", width="100%", directed=True, notebook=False)  # Увеличиваем высоту
net.from_nx(G)

# Настройка узлов с читаемыми именами
print(len(net.nodes))
i = 1
for node in net.nodes:
    print(node)
    print("-" * 10)
for node in net.nodes:
    full_hash = node["id"]
    #print(G.nodes[node["id"]])
    #print("-" * 10)
    level = G.nodes[node["id"]]["level"]
    amount = G.nodes[node["id"]]["amount"]
    # Сокращённый хэш
    #short_hash = f"{full_hash[:4]}...{full_hash[-4:]}"
    #readable_label = f"{short_hash} ({amount})"
    credit = G.nodes[node["id"]]["credit"]
    short_credit = f"{credit[:4]}...{credit[-4:]}"
    # Используем credit вместо хэша
    readable_label = f"{short_credit} ({amount})"
    node["title"] = f"Hash: {full_hash}<br>Amount: {amount}<br>Level: {level}"
    node["label"] = readable_label
    node["size"] = max(10, min(amount / 10, 30))
    node["color"] = ["#97c2fc", "#ffb6c1", "#98fb98", "#dda0dd", "#f08080", "#20b2aa", 
                     "#ffdead", "#87cefa", "#ff69b4", "#9acd32"][level % 10]  # Больше цветов для уровней
    #print(node)
    #print("_" * 10)

# Настройки для глубокого графа
options = {
    "layout": {
        "hierarchical": {
            "enabled": True,
            "levelSeparation": 200,  # Увеличиваем расстояние между уровнями
            "nodeSpacing": 120,      # Увеличиваем расстояние между узлами
            "direction": "UD",
            "sortMethod": "directed"  # Сортировка по направлению
        }
    },
    "physics": {
        "enabled": False  # Физика отключена для иерархии
    },
    "edges": {
        "arrows": "to",
        "smooth": {"type": "continuous"}
    },
    "nodes": {
        "font": {"size": 12}  # Уменьшаем шрифт для читаемости
    }
}

net.set_options(json.dumps(options))

# Сохранение и отображение
net.save_graph("transaction_graph.html")"""


"""
options = {
    "physics": {
        "barnesHut": {
            "gravitationalConstant": -8000,
            "springLength": 250,
            "centralGravity": 0.3
        },
        "minVelocity": 0.75
    },
    "edges": {
        "arrows": "to",
        "smooth": {"type": "continuous"}
    }
}

for depth in range(0, 11, 5):  # 0-5, 5-10 и т.д.
    pipeline[1]["$graphLookup"]["maxDepth"] = depth
    result = list(collection.aggregate(pipeline))[0]
    # Создание и сохранение графа как "graph_depth_{depth}.html"
"""
"""# Визуализация
pos = nx.spring_layout(G)  # Расположение узлов
labels = nx.get_node_attributes(G, "amount")  # Метки с суммами

plt.figure(figsize=(10, 7))
nx.draw(G, pos, with_labels=True, node_color="lightblue", node_size=2000, font_size=12, arrows=True)
nx.draw_networkx_labels(G, pos, labels=labels)
plt.title("Transaction Graph")
plt.show()"""