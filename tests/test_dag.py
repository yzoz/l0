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
        "$graphLookup": {
            "from": "tx",         # Коллекция для поиска
            "startWith": "$hash",      # Начинаем с this_hash текущего документа
            "connectFromField": "hash",# Поле, которое связывает с потомками
            "connectToField": "dad",   # Поле, на которое ссылаются потомки
            "as": "descendants",            # Имя массива для результатов
            "maxDepth": 20,
            "depthField": "level",           # Поле для записи глубины
            # Опционально: "restrictSearchWithMatch": {"amount": {"$gt": 100}}  # Фильтр по сумме
        }
    }
]

# Получение данных
result = list(tx.aggregate(pipeline))[0]  # Берём первый документ
print(f"Найдено потомков: {len(result.get('descendants', []))}")

"""for doc in result:
    import json
    print(json.dumps(doc, indent=2, default=str))"""

# Создание графа
G = nx.DiGraph()  # Направленный граф

# Добавляем стартовый узел
start_node = result["hash"]
G.add_node(start_node, amount=result["amount"], level=0, credit=result["credit"])

# Добавляем потомков и рёбра
for descendant in result["descendants"]:
    child = descendant["hash"]
    parent = descendant["dad"]
    G.add_node(child, amount=descendant["amount"], level=descendant["level"], credit=descendant["credit"])
    if parent:  # Если есть mom_hash
        G.add_edge(parent, child)

# Передача в pyvis
net = Network(height="1000px", width="100%", directed=True, notebook=False)
net.from_nx(G)

# Инициализация pyvis
net = Network(height="1500px", width="100%", directed=True, notebook=False)  # Увеличиваем высоту
net.from_nx(G)

# Настройка узлов с читаемыми именами
for node in net.nodes:
    full_hash = node["id"]
    level = G.nodes[node["id"]]["level"]
    amount = G.nodes[node["id"]]["amount"]
    # Сокращённый хэш
    #short_hash = f"{full_hash[:4]}...{full_hash[-4:]}"
    #readable_label = f"{short_hash} ({amount})"
    credit = G.nodes[node["id"]]["credit"]
    short_credit = f"{credit[:4]}...{credit[-4:]}"
    # Используем credit вместо хэша
    readable_label = f"{short_credit} ({amount})"
    node["title"] = f"Hash: {full_hash}\nAmount: {amount}\nLevel: {level}"
    node["label"] = readable_label
    #node["size"] = max(10, min(amount / 10, 30))
    #node["color"] = ["#97c2fc", "#ffb6c1", "#98fb98", "#dda0dd", "#f08080", "#20b2aa", "#ffdead", "#87cefa", "#ff69b4", "#9acd32"][level % 10]  # Больше цветов для уровней

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
net.save_graph("transaction_graph.html")


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