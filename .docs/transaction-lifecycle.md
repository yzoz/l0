# Жизненный цикл обычной транзакции

> Область: обычный перевод через `POST /send/` → `in` → Consumer → Memgraph →
> `mid` → Confirmer → `out` → Writer → MongoDB/Redis/Memgraph. Genesis mint,
> его создание и первоначальная загрузка балансов исключены. Особая проверка
> Writer для mint отмечена только как совместимая ветка обработки.
>
> Описание отражает исполняемый код в `src/api/routers/transactions.py`,
> `service/consumer.py`, `service/confirmer.py`, `service/writer.py` и
> `src/database/queues.py`; TODO/README используются как связанные тексты и
> сопоставляются с кодом, а не принимаются за runtime-источник истины.

## 1. Краткая временная шкала

1. Клиент формирует содержимое и подпись; отправляет `POST /send/`.
2. FastAPI валидирует модель запроса. Handler проверяет Ed25519 подпись,
   вычисляет стабильный SHA-512 hash и публикует JSON в durable очередь `in`.
3. Consumer получает сообщение (RabbitMQ `prefetch_count=1`), разбирает JSON,
   повторно валидирует поля, считает тот же hash и блокирует обработку по hash.
4. Если hash уже есть в Memgraph — recovery: сверка данных → ремонт рёбер/цикла
   → повторная публикация hash в `mid` → ACK исходного `in`.
5. Иначе Consumer резервирует `(credit, uniq)` в Redis, проверяет баланс и
   родителей, ждёт rate-limit слот, записывает узел/рёбра в Memgraph и публикует
   hash в `mid`. Только после этого ACK `in`.
6. Confirmer получает hash из `mid`, валидирует его, загружает весь текущий
   граф одним запросом, находит предков trigger-транзакции вверх, вычисляет их
   полный рекурсивный score вниз и помещает прошедшие порог hash в `out`.
7. Для каждого кандидата `enqueue_candidate` держит `queued:{hash}` с lease и
   ownership token, чтобы дедуплицировать out-сообщения.
8. Writer получает кандидата, проверяет минимальный размер пула, сериализует
   обработку по hash, проверяет подтверждение/наличие узла/состояния родителей/
   средства, затем записывает Mongo → обновляет Redis → перепубликует отложенных
   детей → удаляет узел из Memgraph → снимает queued-маркер.
9. При временных ошибках отдельных стадий событие возвращается в соответствующую
   очередь; при терминальных ошибках оно исключается. Конкретные ветви и
   исключения перечислены далее.

```text
Client
  └─ POST /send/ → модель TxRead → подпись/hash → RabbitMQ in (Tx JSON)
       └─ Consumer: JSON/TxStore → hash → граф recovery?
            ├─ да: сверка → repair CONFIRMS → mid(hash) → ACK in
            └─ нет: reserve uniq → balance → parents → rate limit
                 → Memgraph node + CONFIRMS + cycle check → mid(hash) → ACK in
                    └─ Confirmer: load graph → ancestors → recursive scores
                         └─ each score ≥ 1: queued lease → RabbitMQ out(hash)
                              └─ Writer: pool guard → idempotency → node
                                   → parents → balance → Mongo → Redis
                                   → deferred children to out → delete graph
                                   → release queued → ACK out
```

## 2. Контракт данных и инфраструктура очередей

### Модель транзакции — `src/models/tx.py`

`TxRead` содержит `content: Tx` и `sign: str`; `TxStore` — поля `Tx` плюс
подпись; `TxWrite` дополнительно содержит `hash`, `v` (по умолчанию `2`) и
`time`.

- `parents`: ровно `PARENTS=3`, все строки непустые и уникальные.
- `credit`: отправитель/плательщик; `debit`: получатель.
- `debit` должен быть декодируемым base58 Ed25519 public key длиной 32 байта и
  не должен совпадать с `credit`.
- `amount`: `PositiveInt`; `uniq`: строка; `msg`: optional строка максимум 10.
- `sign`: строка без дополнительного ограничения формата в Pydantic.

FastAPI model validation происходит до выполнения `send`: невалидный REST
body возвращает стандартную ошибку FastAPI/Pydantic (обычно 422), handler и
RabbitMQ не вызываются.

### Hash и подпись

`ordered_sum` в `src/utils/data.py` сортирует ключи dict, сериализует значения
строками, списки склеивает через RS (`\x1e`), поля — через US (`\x1f`).
`hash_hex` — SHA-512 hex (128 lowercase hex chars). Подпись API проверяет над
`sha512(ordered_sum(content).encode()).digest()` публичным ключом `credit`;
функция `verify_message` декодирует подпись из hex и вызывает PyNaCl VerifyKey.
Tx hash считается отдельно по полям content плюс `sign`; Consumer считает hash
по сериализованному `TxStore`, то есть тем же набором полей и порядком.

### RabbitMQ и backpressure — `src/database/queues.py`

- `in`: подписанный JSON (`TxRead`, фактически JSON-compatible `TxStore`).
- `mid`: только plain-text hash транзакции, записанной в граф.
- `out`: только plain-text hash кандидата.
- Очереди durable, сообщения persistent, default exchange; `connect_robust`.
- `prefetch_count=1`: один unacked message на channel/consumer до следующего.
- `consume_messages` последовательно `await callback(message)`; выход по stop event.
- API `send` ждёт публикацию `in` не более `RABBITMQ_TIMEOUT=5s`.

## 3. Хронология подробно

### A. API — `src/api/routers/transactions.py:send`

1. FastAPI строит `TxRead`; `get_rabbit` получает `request.app.state.rabbit`.
2. Handler логирует `ordered_sum(content)` и transaction identifiers.
3. Считает bytes для проверки подписи: SHA-512 digest от сериализованного
   content, **без `sign`**.
4. `verify_message(credit, sign, digest)`:
   - успешный результат должен быть в точности равен переданному digest;
   - `BadSignatureError`, `ValueError`, `TypeError` считаются неверной подписью;
   - неверная подпись → HTTP 400 `Bad sign!`, ничего не публикуется.
5. Hash транзакции считается по `content + sign`; это идентификатор для
   корреляции, не receipt подтверждения.
6. Сериализует `tx` через `jsonable_encoder` + `json.dumps`, публикует в `in`
   через `asyncio.wait_for(..., timeout=5)`.
7. Publish timeout → HTTP 503 `RabbitMQ timeout`; возможна неопределённость,
   принял ли брокер публикацию до timeout/cancellation. Другие исключения API
   логируются и преобразуются в HTTP 500. Явные HTTPException пробрасываются.
8. Успех публикации → HTTP response success=1, msg=ok и `tx_hash`. Это означает
   только принятие в очередь, не принятие Consumer-ом и не подтверждение.

### B. Consumer — `service/consumer.py`

#### B1. Сервис и разбор

1. `ConsumerService.startup`: Redis → RabbitMQ → MongoDB → Memgraph; затем
   Consumer слушает `in`. `finally` закрывает соединения.
2. `process_message` вызывает `parse_message`: UTF-8 decode → JSON parse →
   `data["content"]`, присваивание `content["sign"] = data["sign"]`, создание
   `TxStore` (модельные проверки повторяются на границе очереди).
3. Ошибки до/при parse:
   - `JSONDecodeError` → `nack(requeue=False)` (bad-json);
   - `asyncio.TimeoutError` → `nack(requeue=True)`;
   - прочие исключения, в том числе UTF-8 decode, KeyError и validation errors,
     → общий catch → `nack(requeue=False)`.

#### B2. Вход в транзакцию и recovery

4. `process_transaction` вычисляет `tx_hash = hash_hex(ordered_sum(dict(tx)))`,
   увеличивает received и берёт Redis lock `consumer:{tx_hash}`.
5. До проверки duplicate читает `t.data` в Memgraph по hash. Этот порядок
   необходим для восстановления после записи узла без успешной публикации mid.
6. Если узел есть, `_retry_inserted`:
   - сравнивает входные поля TxStore со stored `t.data`, игнорируя только `time`;
   - несовпадение → conflict, `nack(requeue=False)`, не создаёт второй узел;
   - совпадение → `_ensure_edges`: Redis lock `dag:{hash}`, идемпотентный repair
     CONFIRMS и cycle check;
   - repair/cycle неуспешен → `nack(requeue=True)`;
   - успех → повторно публикует hash в `mid`; publish failure → `nack(requeue=True)`;
   - успешная публикация → ACK `in`; граф и uniq reservation не создаются/не
     меняются. Перепубликация mid может создать дубли события, downstream пути
     должны быть идемпотентными.

#### B3. Обычный путь — графа ещё нет

7. `check_duplicate` атомарно выполняет `SET NX uniq:{credit}:{uniq}` без TTL.
   - Redis key уже существует → duplicate=true;
   - reservation создана → Mongo ищет `{credit, uniq}`; запись есть → duplicate;
     нет → путь продолжается, ключ удерживается как reservation.
   - duplicate → `nack(requeue=False)`. В этой ветке release не вызывается.
8. Проверяет средства `verify_balance` → `get_balance(credit)`:
   - сначала `balance:{addr}` Redis с timeout 1s;
   - cache miss → aggregation подтверждённых Mongo записей (income по debit
     минус outcome по credit), затем cache set без TTL;
   - timeout возвращает 0; balance < amount → terminal `nack(requeue=False)` и
     `release_uniq` (Redis key delete best-effort).
9. Проверяет каждого родителя по порядку:
   - существует Transaction node в Memgraph → считается найденным;
   - иначе должен существовать `confirmed:{parent}` в Redis;
   - отсутствует и то и другое → terminal `nack(requeue=False)` + release uniq;
   - все родители найдены → далее.
10. До реальной записи графа `RateLimiter.acquire()` ждёт слот; ожидание
    измеряется. Текущая настройка `RATE_LIMIT_TPS=5`; при нескольких Consumer
    процессах предел применяется каждым процессом отдельно. Limiter использует
    monotonic clock, сериализует `acquire()` через `asyncio.Lock` и применяет
    constant spacing `1/TPS`; первый слот немедленный. При ожидании
    callback уже держит доставленное сообщение unacked; с `prefetch=1` этот
    Consumer не получает следующее, а новые сообщения остаются в очереди `in`.
11. `add_to_graph` под lock `dag:{hash}`:
    - ставит `time` в microseconds и один раз считает
      `internal_score = transfer_score(amount, alpha, beta)`;
    - `MERGE` node по hash, при создании сохраняет `data` и `internal_score`;
    - для каждого parent, если нет `confirmed:{parent}`, `MERGE`
      `(parent)-[:CONFIRMS]->(child)`; для confirmed parent ребро не создаётся;
    - проверяет path cycle от нового узла к самому себе; цикл → detach delete
      нового узла, false;
    - результат false → Consumer `nack(requeue=False)` + release uniq;
    - `Neo4jError` ловится внутри и превращается в false, после чего также
      terminal nack; если ошибка возникла после успешного MERGE node, возможен
      оставшийся частичный узел, а caller всё равно снимает uniq reservation.
      См. audit findings ниже.
12. На TransientError декоратор `tenacity` настроен на 5 попыток с exponential
    wait 0.1–2s, но фактически внутренний `except Neo4jError` перехватывает
    ошибки до выхода из функции, поэтому ветка retry может не достигаться.
13. После записи узла Consumer публикует `mid` до ACK `in`:
    - `_publish_mid` ловит исключения и возвращает false;
    - false → `nack(requeue=True)`, uniq reservation сохраняется; повторная
      доставка входит в B2 и допубликует mid без второго узла;
    - true → ACK `in`.
14. Внутренний `except PublishError` делает nack(true) и повторно пробрасывает
    исключение в `process_message`; внешний общий handler ловит его как
    `Exception` и также пытается nack(false). Это потенциальный двойной nack с
    противоречивым routing. В текущих обычных путях `_publish_mid` ловит все
    publish exceptions и возвращает false, поэтому эта ветка не является
    ожидаемым результатом штатного mid publish.
15. Общий `BaseException` внутри основной секции снимает uniq и re-raise; timeout
    наверху requeues, generic exception наверху терминально drop-ается.

### C. Confirmer — `service/confirmer.py`

1. `ConfirmationService` подключает Redis → RabbitMQ → Memgraph, слушает `mid`.
2. `process_event` декодирует UTF-8, trim; hash должен соответствовать regex
   ровно 128 lowercase hex chars.
   - malformed → `nack(requeue=False)`;
   - valid → `_process_hash`.
3. `_process_hash` увеличивает events и `_load_graph` делает один пакетный
   Cypher query текущих `Transaction` nodes, `internal_score` и прямых детей по
   направленным рёбрам `CONFIRMS` (родитель → ребёнок).
4. Валидация snapshot:
   - `internal_score` должен быть int/float, но не bool; иначе ValueError;
   - каждое ребро должно ссылаться на узел в snapshot; иначе ValueError.
   Ошибка уходит в общий handler и terminates mid event через nack(false).
5. Trigger hash отсутствует в графе → транзакция уже подтверждена/удалена либо
   событие устарело; skip и ACK `mid`.
6. Строит reverse adjacency (child → parents), затем `find_ancestors` BFS от
   trigger вверх. Trigger исключён; при нескольких путях hash берётся один раз,
   по кратчайшему пути. `MAX_DEPTH=None` сейчас означает без лимита.
7. Нет предков → закончить без публикации, ACK `mid`.
8. До первой публикации рассчитываются scores всех предков итеративным
   memoized DFS вниз по всем потомкам каждого кандидата:
   `score(t) = internal_score(t) + sum(score(child)/3)`.
   Учитываются все ветви поддерева; score не сохраняется. Цикл → ValueError,
   partial out publish не происходит, event terminal nack(false).
9. В BFS порядке для каждого предка:
   - score < 1.0 → skip;
   - score >= 1.0 → `enqueue_candidate(..., skip_if_queued=False)`.
10. `enqueue_candidate` создаёт UUID token, пытается SET NX EX
    `queued:{hash}` (lease 30s). Если занят — повторяет каждые 0.5s до бюджета
    60s; если истёк бюджет — PublishError. Получив маркер, публикует hash в out;
    publish exception → release только собственного token compare-and-delete,
    затем PublishError.
11. PublishError → `nack(requeue=True)` на mid. Прочие exceptions → лог +
    `nack(requeue=False)`. Успешная обработка, включая no ancestors/not found,
    → ACK `mid`.
12. Out publication не транзакционна с ACK `mid`: если публикация нескольких
    кандидатов прошла частично, затем следующая упала, повтор mid пересчитает
    всех заново; `queued` удерживает уже опубликованных кандидатов от дублей,
    остальные могут быть опубликованы при retry.

### D. Writer — `service/writer.py`

#### D1. Разбор out и общий outcome

1. `WriterService` подключает Redis → RabbitMQ → MongoDB → Memgraph и слушает
   `out`.
2. `process_message` decode/trim; hash не 128 lowercase hex → `nack(false)`.
3. Вызывает `process_candidate`:
   - PublishError (в частности, deferred-child publish) → `nack(true)`;
   - прочее exception → `nack(false)`;
   - outcome `retry` → `nack(true)`;
   - прочий outcome → ACK.

#### D2. Кандидат

4. Сначала `_count_txs` считает все графовые Transaction nodes.
   - `num_txs <= PARENTS` (≤3): очищается queued marker; `dropped` metric;
     **функция возвращает `retry`**, а message handler делает `nack(true)`.
     Узел остаётся без deferred метки; это может приводить к циклическому
     повтору того же out event, см. findings. В `.docs/TODO_003.md` требование
     иное — ACK + drop, без автоматического ожидания.
5. Иначе открывает граф session, берёт lock `writer:{hash}`, читает текущий
   queued ownership token.
6. Если есть `confirmed:{hash}` — обработка считается ранее начатой/завершённой;
   пропускает повторное списание/вставку и делает `_finish_tail`.
7. Иначе читает `t.data` по hash:
   - узла нет → clear queued и ACK как устаревшее/уже обработанное событие;
   - узел есть → `create_tx(hash, data)` валидирует/строит `TxWrite`.
8. `_parents_state` проверяет каждого родителя:
   - `GENESIS_PARENTS` sentinel `{"0","1","2"}` → ok;
   - Mongo `find_one({hash})` или Redis `confirmed:{hash}` → ok;
   - родитель node в Memgraph → pending;
   - иначе → dead немедленно;
   - после обхода: dead имеет приоритет, pending если хотя бы один остался,
     иначе ok.
9. `dead`: `_kill_subtree` удаляет узел и неподтверждённое downstream поддерево,
   затем снимает для каждого узла `uniq`, `deferred`, `queued`; далее очищается
   queued для кандидата и ACK. Ошибка удаления графа возвращается как false, но
   caller её не проверяет — см. findings.
10. `pending`: ставит `deferred:{hash}` без TTL, очищает queued, считает defer,
    ACK. Ожидание заканчивается, когда родитель подтверждён и writer повторно
    публикует этот прямой deferred child в out.
11. `ok`: удаляет `deferred:{hash}`. Затем проверяет balance:
    - ordinary transfer: `get_balance(credit)`; если < amount — kill subtree,
      clear queued, ACK;
    - mint (`credit.strip()` пуст) — balance guard пропускается. Это лишь
      условная совместимость пути; genesis создание вне scope.
12. `_confirm`:
    - сначала вставляет `TxWrite` в Mongo (источник истины);
    - DuplicateKeyError → `dup`, не вставляет второй документ, переходит к tail;
    - false insert → failed; outcome retry, queued marker остаётся;
    - успешный Mongo insert → Redis pipeline: `INCRBY balance:{debit}`,
      `DECRBY balance:{credit}`, `SET confirmed:{hash}=1 EX 360`;
    - сбой pipeline → best-effort компенсация: удаление Mongo doc по hash;
      failed → requeue; детали частичного Redis pipeline и компенсации ниже;
    - успешный Redis → confirmed metric, возвращает confirmed.
13. `_finish_tail` строго до удаления граф node:
    - ищет прямых CONFIRMS детей с `deferred:{child}`;
    - каждому вызывает `enqueue_candidate(..., skip_if_queued=True)`;
    - child queued занят → пропускает без ожидания, это уже стоящий out event;
    - publish error → PublishError, tail возвращает retry, confirmed node остаётся
      в графе, queued lease остаётся; re-delivery видит confirmed marker и
      повторяет tail;
    - если публикации успешны/не требуются → удаляет tx node (DETACH DELETE),
      затем освобождает queued marker по token через compare-and-delete; если
      token истёк/отсутствует — clear queued; ACK out.
14. `_delete_node` и `_kill_subtree` ловят Neo4jError и возвращают false, однако
    вызывающие ветви сейчас не проверяют результат. Поэтому может быть ACK при
    фактически не удалённом графе.

## 4. Все существенные циклы и ветвления

| Цикл/повтор | Когда начинается | Условие выхода / результат |
|---|---|---|
| Consumer queue loop | RabbitMQ `in` iterator | Остановка сервиса/stop event; callback ACK/nack каждого сообщения |
| Consumer transient graph retry | Tenacity на `add_to_graph`, максимум 5 попыток, 0.1–2s | Только если `TransientError` выходит из функции; сейчас Neo4jError пойман внутренним except и возвращается false, так что retry может не случиться |
| Consumer parents loop | По 3 parents | Первый не найден ни в графе, ни confirmed Redis → отказ; иначе все проходят |
| Consumer edge loop | По 3 parents | Для unconfirmed создаёт MERGE edge; confirmed parent пропускает |
| Consumer recovery | Повторная доставка `in` при существующем graph node | Сверка, repair, mid publish; успешный ACK или конфликт/drop/requeue |
| Confirmer queue loop | RabbitMQ `mid` iterator | На каждом событии ACK, requeue или terminal drop |
| Confirmer ancestor BFS | От trigger вверх по обратным связям | Все предки найдены; сейчас MAX_DEPTH=None; множество исключает повторные пути |
| Confirmer score DFS | Из каждого ancestor вниз через всех children | Memoized complete; цикл в `on_stack` вызывает ValueError |
| Confirmer candidate loop | Каждому найденному предку | score<1 пропуск, score≥1 enqueue; publish error прекращает событие, requeue mid |
| queued lock wait | SET NX попытки раз в 0.5s | Маркер взят, deferred enqueue сразу skip, либо 60s → PublishError/requeue; lease 30s |
| Writer queue loop | RabbitMQ `out` iterator | ACK финального решения, requeue временной ошибки, drop malformed/unhandled |
| Writer parents loop | Три родителя | Sentinel/Mongo/confirmed Redis пропускаются; graph parent pending; отсутствующий parent dead с ранним выходом |
| Writer deferred requeue loop | Прямые дети родителя | Публикует только отмеченных deferred; already queued skip; failure прерывает tail/requeue parent |
| Writer subtree traversal | Cypher `CONFIRMS*0..` | Собирает root и всех достижимых потомков, затем удаляет/чистит ключи |

## 5. ACK / NACK матрица

| Сообщение | Условие | Действие |
|---|---|---|
| API HTTP | model invalid | FastAPI validation response, обычно 422; в очередь не публикует |
| API HTTP | signature invalid | 400 |
| API HTTP | `in` publish timeout | 503; принятие брокером может быть неопределённым |
| API HTTP | unexpected | 500 |
| `in` | bad JSON | nack(false) |
| `in` | duplicate, insufficient balance, missing parent, graph false/cycle, hash-data conflict | nack(false); release uniq на ветвях с гарантированно отсутствующим узлом, кроме duplicate |
| `in` | timeout | nack(true) |
| `in` | successful graph + mid publish | ACK |
| `in` | node exists recovery: mismatch | nack(false) |
| `in` | recovery repair/mid failure | nack(true) |
| `in` | unexpected exception | nack(false), кроме timeout |
| `mid` | malformed hash | nack(false) |
| `mid` | out PublishError/queued wait exhausted | nack(true) |
| `mid` | unexpected graph/score error | nack(false) |
| `mid` | hash absent, no ancestors, candidates all below threshold, or successful enqueue | ACK |
| `out` | malformed hash | nack(false) |
| `out` | Mongo/Redis/deferred publish retry outcome | nack(true), queued held except branches that explicitly clear it |
| `out` | unexpected exception | nack(false) |
| `out` | confirmed/dead/deferred/no graph/funds failure/normal tail | ACK |
| `out` | min-pool ≤3 | **actual:** clears queued + returns retry + nack(true); **TODO_003 expected:** clear queued + ACK/drop |

## 6. Источники истины и ключи состояния

- Memgraph — неподтверждённый DAG/pool; обычная транзакция временно живёт в нём.
- Mongo `tx` — подтверждённый ledger и fallback balance source.
- Redis `balance:{addr}` — balance cache; Redis `confirmed:{hash}` — short-lived
  marker (360s); Mongo также проверяется как persistent confirmation source.
- Redis `uniq:{credit}:{uniq}` — reservation без TTL, предотвращает in-flight и
  повторный `(credit, uniq)`; сохраняется после добавления граф-узла, удаляется
  при отбрасывании неподтверждённого subtree.
- Redis `consumer:{hash}`, `dag:{hash}`, `writer:{hash}` — lock scopes.
- Redis `queued:{hash}` — out dedupe, uuid owner и 30s lease; обычный confirmer
  ждёт освобождения до 60s, writer deferred child path skip-ает занятую очередь.
- Redis `deferred:{hash}` — indefinite marker для кандидата, ожидающего parent.
- `internal_score` сохраняется в graph node; текущий aggregate score вычисляется
  только в памяти Confirmer и не сохраняется.

## 7. Несовпадения документации и кода

### README.md

README содержит устаревшие описание и контракт: указывает очередь `tx`,
периодический confirmer и обычное «затухание по глубине»; пример запроса имеет
только два parents, хотя модель требует ровно три; ответ обещает только `detail`
и не показывает возвращаемый `tx_hash`. Также live-терминал описывает старые
метрики/настройки `CONFIRM_WEIGHT`, `MAX_DEPTH=7`, `DAMPING=0.5`, хотя текущий
Confirmer — событийный и `MAX_DEPTH=None`.

### `.docs/TODO_003.md`

Содержит требование по min-pool: если `num_txs <= 3`, ACK и drop без повторной
попытки. Writer сейчас возвращает `retry`, следовательно callback делает
`nack(requeue=True)`. Остальные существенные требования TODO_003 (in/mid/out,
recovery, queued lease, parents/deferred, score thresholds) в целом отражены в
реализации. Документ сам помечен как исходное ТЗ, но не как подтверждение
текущего исполнения.

### `.docs/TODO_004.md`

Заголовок/цель старого ТЗ описывают только прямых детей и score родителей,
тогда как его поздние разделы уже задают рекурсивный score; код следует поздней
рекурсивной спецификации. Документ говорит «три родителя», но фактический
Confirmer находит всех графовых ancestors (при текущем `MAX_DEPTH=None`), а не
только три trigger parents. Нужно читать позднюю рекурсивную часть как
актуальную спецификацию, а не первые абзацы.

## 8. Найденные дефекты и риски (по важности)

1. **Min-pool образует активный requeue-loop.** `service/writer.py:164-169`
   очищает queued, но возвращает `retry`; `process_message` на `retry` делает
   `nack(requeue=True)`. При пуле ≤3 то же сообщение немедленно переобрабатывается,
   расходует цикл и может не дать очереди нормально продвинуться. Это прямо
   расходится с `.docs/TODO_003.md:133-141` (ACK/drop). Требует исправления.
2. **Writer теряет финальность при ошибках очистки графа.** `_kill_subtree` и
   `_delete_node` возвращают `False` на Neo4jError, но caller игнорирует bool,
   очищает marker/ACK-ает. Dead/funds ветка оставит узел/поддерево; tail может
   оставить подтверждённый узел. Сообщение уже не гарантированно вернётся.
3. **Timeout баланса трактуется как нулевой баланс.** `get_balance` возвращает
   `0` на `asyncio.TimeoutError`; Consumer затем terminal-drop-ает in, Writer
   может kill subtree. Транзиентная недоступность Redis/Mongo способна стать
   необратимым business rejection, вместо retry.
4. **Consumer graph retry подавлен локальным catch.** `add_to_graph` декорирован
   retry для `TransientError`, но его тело ловит общий `Neo4jError` (включая
   transient subclasses) и возвращает False. Caller считает это terminal graph
   failure и делает nack(false). Заявленная retry-логика может не работать.
5. **Consumer может оставить частичный node и снять его uniq guard.** Ошибка
   Neo4j после `MERGE` попадает в catch, который возвращает false; в отличие от
   cycle ветки, здесь нет гарантированного `DETACH DELETE`. Caller делает
   terminal nack и `release_uniq`, хотя hash-node уже мог сохраниться. Следующая
   транзакция с тем же `(credit, uniq)` может пройти reservation, а повтор этого
   hash уйдёт в recovery; исходное сообщение уже удалено из очереди.
6. **Mongo→Redis компенсация не атомарна с Redis pipeline.** При исключении
   `pipe.execute()` удаляется Mongo документ, но код не откатывает уже
   выполненные Redis balance commands/confirmed marker (в зависимости от того,
   какие команды успели примениться). Возможны рассинхрон балансов/marker и
   повторная обработка; retry может увидеть `confirmed` и пропустить `_confirm`.
7. **Частичный subtree cleanup также не transactional.** Сначала граф nodes
   удаляются, затем Redis keys чистятся best-effort; ошибки key deletion только
   логируются. Возможны застрявшие `uniq`/`deferred`/`queued` markers после
   необратимого удаления узлов.
8. **API timeout не доказывает не-публикацию.** `wait_for` отменяет ожидание
   publish; если брокер успел принять сообщение, клиент получает 503, но
   транзакция может всё равно пройти. Deterministic hash облегчает повторную
   отправку/поиск, однако API ответа статуса нет.
9. **Balance check и списание не сериализованы по адресу.** Два разных hash
   одного `credit` могут одновременно пройти Consumer/Writer balance check до
   применения обоих Redis списаний. `DECRBY` не проверяет non-negative balance;
   конкурентный поток может привести к отрицательному балансу.
10. **Consumer broad BaseException release шире заявленного.** Внутренний try
   включает операцию после записи узла и ACK; исключение/отмена в поздней фазе
   может удалить `uniq` несмотря на то, что узел уже в графе, вопреки комменту
   «сбой до записи». Recovery по hash смягчает повторную доставку, но invariant
   reservation не абсолютен.
11. **Duplicate Mongo record не восстанавливает Redis state.** `_confirm`
    возвращает `dup` сразу после DuplicateKeyError, не проверяя/не восстанавливая
    Redis balances или `confirmed:{hash}`. Если Mongo документ остался от
    предыдущего частичного commit/компенсации, Writer может удалить графовый node
    и ACK-нуть out при несогласованном Redis состоянии.
12. **Некоторые ошибки считаются терминальными слишком широко.** Consumer общий
    exception и Writer общий exception используют nack(false); например,
    Mongo/Redis/graph временная ошибка, не классифицированная отдельно, теряет
    сообщение. Это политика кода, не гарантированно безопасная retry политика.

## 9. Тестовое покрытие, найденное в репозитории

- `tests/test_consumer.py`: parse valid/invalid JSON; duplicate reservation и
  Mongo duplicate; balance sufficient/insufficient; parent outcomes;
  success graph→mid→ACK; duplicate/funds/missing-parent rejection; mid failure
  requeue without release; inserted-node recovery; hash/data mismatch; successful
  recovery without second graph insert; malformed message terminal reject.
- `tests/test_score.py`: transfer score, рекурсивная chain/branches, DAG merge,
  shared subtree memoization, cycle exception и порог.
- `tests/test_console.py`: instrumentation Consumer/Writer, Confirmer score
  event; это не полное покрытие Writer lifecycle.
- `tests/test_e2e_pipeline.py`: integration invariants балансов/генезиса, но
  найденный тест не доказывает исчерпывающий набор ветвей обычного transfer.
- В репозитории не найдены `tests/test_writer.py` или `tests/test_confirmer.py`;
  branch coverage Writer (min-pool retry, dead/funds cleanup failure, partial
  Mongo/Redis compensation, deferred-tail failure) не подтверждено отдельным
  unit suite.

## 10. Основные ссылки на код

- API/model/hash: `src/api/routers/transactions.py`, `src/models/tx.py`,
  `src/utils/crypto.py`, `src/utils/data.py`.
- Queue contract/leases: `src/database/queues.py`, `src/config.py`.
- Consumer: `service/consumer.py`, `service/ratelimit.py`, `src/utils/profile.py`.
- Confirmer: `service/confirmer.py`, `src/utils/fn.py`.
- Writer: `service/writer.py`, `src/database/mongodb.py`.
- Existing narrative: `README.md`, `.docs/TODO_003.md`, `.docs/TODO_004.md`,
  `CHANGELOG.md`.
