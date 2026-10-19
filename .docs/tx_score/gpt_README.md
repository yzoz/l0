# Тесты и benchmark на 5 000 узлах
python score_dag_benchmark.py

# 100 000 узлов, фиксированный seed
python score_dag_benchmark.py --nodes 100000 --seed 7

# Настроить глубину и количество повторов
python score_dag_benchmark.py \
    --nodes 100000 \
    --max-depth 5 \
    --bench-repeats 3

# Только автоматические тесты
python score_dag_benchmark.py --skip-benchmark
