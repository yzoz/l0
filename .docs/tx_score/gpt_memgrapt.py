from collections import defaultdict, deque
from typing import Any

from neo4j import GraphDatabase

# Импортируем существующую реализацию, а не дублируем её.
from score_dag_benchmark import (
    GraphStore,
    transfer_score,
    score_dfs_memo,
)


# Предполагается, что GraphDatabase подключается к Memgraph по Bolt.
URI = "bolt://localhost:7687"
AUTH = ("", "")

BATCH_SIZE = 1000


class TransactionScorer:
    def __init__(self, driver):
        self.driver = driver

    @staticmethod
    def _chunks(items: list[str], size: int = BATCH_SIZE):
        for i in range(0, len(items), size):
            yield items[i:i + size]

    def _expand(self, frontier: set[str], *, reverse: bool) -> set[str]:
        """
        reverse=False: от транзакции к её родителям.
        reverse=True:  от родителя к его потомкам.
        """
        if not frontier:
            return set()

        result: set[str] = set()

        if reverse:
            query = """
            UNWIND $hashes AS h
            MATCH (child:Transaction)-[:PARENT]->(parent:Transaction {hash: h})
            RETURN DISTINCT child.hash AS hash
            """
        else:
            query = """
            UNWIND $hashes AS h
            MATCH (child:Transaction {hash: h})-[:PARENT]->(parent:Transaction)
            RETURN DISTINCT parent.hash AS hash
            """

        with self.driver.session() as session:
            for batch in self._chunks(list(frontier)):
                rows = session.run(query, hashes=batch)
                result.update(
                    row["hash"] for row in rows
                    if row["hash"] is not None
                )

        return result

    def find_candidates(
        self,
        trigger_hash: str,
        max_depth: int | None,
    ) -> dict[str, int]:
        """
        Находит предков trigger-транзакции:
        DEPTH 1 — непосредственные родители.
        DEPTH 2 — родители родителей.
        Триггер в результат не включается.

        None означает отсутствие ограничения глубины.
        """
        if max_depth is not None and (
            not isinstance(max_depth, int)
            or isinstance(max_depth, bool)
            or max_depth < 0
        ):
            raise ValueError("max_depth должен быть >= 0 или None")

        with self.driver.session() as session:
            exists = session.run(
                """
                MATCH (tx:Transaction {hash: $hash})
                RETURN count(tx) > 0 AS found
                """,
                hash=trigger_hash,
            ).single()["found"]

        if not exists:
            raise KeyError(f"Транзакция не найдена: {trigger_hash}")

        if max_depth == 0:
            return {}

        depths: dict[str, int] = {}
        frontier = {trigger_hash}
        depth = 0

        while frontier and (
            max_depth is None or depth < max_depth
        ):
            parents = self._expand(frontier, reverse=False)
            depth += 1

            new_parents = parents - depths.keys() - {trigger_hash}
            for h in new_parents:
                depths[h] = depth

            # Дальше исследуем и ранее встречавшиеся узлы не нужно:
            # в DAG нас интересует минимальная глубина достижения.
            frontier = new_parents

        return depths

    def _load_dependency_closure(
        self,
        candidate_hashes: set[str],
    ) -> set[str]:
        """
        Загружает кандидатов и всех их потомков рекурсивно.
        Именно этот набор нужен для точного расчёта score.
        """
        closure = set(candidate_hashes)
        frontier = set(candidate_hashes)

        while frontier:
            descendants = self._expand(frontier, reverse=True)
            new_nodes = descendants - closure

            if not new_nodes:
                break

            closure.update(new_nodes)
            frontier = new_nodes

        return closure

    def _load_graph(self, hashes: set[str]) -> tuple[GraphStore, dict[str, int]]:
        """
        Загружает узлы и внутренние связи выбранного подграфа.
        В GraphStore вставляем родителей раньше детей.
        """
        if not hashes:
            return GraphStore(), {}

        query_nodes = """
        UNWIND $hashes AS h
        MATCH (tx:Transaction {hash: h})
        RETURN tx.hash AS hash, tx.amount AS amount
        """

        query_edges = """
        MATCH (child:Transaction)-[:PARENT]->(parent:Transaction)
        WHERE child.hash IN $hashes AND parent.hash IN $hashes
        RETURN child.hash AS child, parent.hash AS parent
        """

        nodes: dict[str, float] = {}
        parents: dict[str, set[str]] = defaultdict(set)
        children: dict[str, set[str]] = defaultdict(set)

        with self.driver.session() as session:
            for batch in self._chunks(list(hashes)):
                for row in session.run(query_nodes, hashes=batch):
                    amount = row["amount"]
                    if amount is None:
                        raise ValueError(
                            f"У транзакции {row['hash']} отсутствует amount"
                        )
                    nodes[row["hash"]] = transfer_score(float(amount))

            for batch in self._chunks(list(hashes)):
                for row in session.run(query_edges, hashes=batch):
                    child, parent = row["child"], row["parent"]
                    parents[child].add(parent)
                    children[parent].add(child)

        missing = hashes - nodes.keys()
        if missing:
            raise RuntimeError(
                f"Часть узлов исчезла или не загрузилась: {list(missing)[:5]}"
            )

        # Количество родителей, которые ещё нужно вставить.
        remaining = {
            h: len(parents[h])
            for h in hashes
        }
        ready = deque(h for h, count in remaining.items() if count == 0)

        graph = GraphStore()
        id_by_hash = {h: i for i, h in enumerate(nodes)}
        inserted = set()

        while ready:
            h = ready.popleft()
            if h in inserted:
                continue

            graph.add_transaction(
                id_by_hash[h],
                nodes[h],
                (id_by_hash[p] for p in parents[h]),
            )
            inserted.add(h)

            for child in children[h]:
                remaining[child] -= 1
                if remaining[child] == 0:
                    ready.append(child)

        if len(inserted) != len(nodes):
            raise ValueError("В загруженном подграфе обнаружен цикл")

        return graph, id_by_hash

    def score_transactions(
        self,
        trigger_hash: str,
        *,
        max_depth: int | None = None,
        threshold: float = 1.0,
    ) -> list[dict[str, Any]]:
        """
        Возвращает все найденные транзакции-кандидаты,
        у которых точный score >= threshold.
        """
        candidate_depths = self.find_candidates(
            trigger_hash,
            max_depth,
        )

        if not candidate_depths:
            return []

        # Расчёт веса кандидата требует всех его потомков,
        # а не только узлов в пределах max_depth.
        closure = self._load_dependency_closure(
            set(candidate_depths)
        )

        # Триггер должен входить в подграф зависимостей:
        # он является потомком найденных предков.
        closure.add(trigger_hash)

        graph, id_by_hash = self._load_graph(closure)

        result = score_dfs_memo(
            graph,
            trigger_id=id_by_hash[trigger_hash],
            max_depth=max_depth,
            threshold=threshold,
        )

        hash_by_id = {v: k for k, v in id_by_hash.items()}

        found = [
            {
                "hash": hash_by_id[tx_id],
                "depth": candidate_depths[hash_by_id[tx_id]],
                "score": score,
            }
            for tx_id, score in result.candidates.items()
            if hash_by_id[tx_id] in candidate_depths
        ]

        found.sort(key=lambda item: (-item["score"], item["hash"]))
        return found


if __name__ == "__main__":
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        scorer = TransactionScorer(driver)

        transactions = scorer.score_transactions(
            trigger_hash="YOUR_TRIGGER_HASH",
            max_depth=4,
            threshold=1.0,
        )

        for tx in transactions:
            print(tx)
