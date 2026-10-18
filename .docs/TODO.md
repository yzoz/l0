компенсация (упал Redis → откат Mongo) - пока пусть - но вообще-то разрушает целостность хэш-связей - отдельный таск и тест - проверить может уже решено

давай так: internal score считается consumer и записывается в memgraph - сортировать в pool согласно ему - это логичней и не нужно лишних пересчётов - confirmer просто делит их на три и суммирует. -> вероятно можно сделать в один cypher запрос. как понял? - DONE

поправить tx_bot - чтоб было больше мелких, потом средних и больших меньше
реализовать атаки волнами, дать наработки из l0-python

И так узкие места:
aio-pika - нет ли каких-то глюков из-за асинхронности
TransientError в memgraph
вообще чекнуть всю асинхронную логику
чекнуть все self.redis.client.lock - многопоточность, асинхронность
у меня есть такая самописная реализация src/utils/lock.py


у меня по коду в vscode очень много подобных ошибок: var is not a known attribute of "None" PylancereportOptionalMemberAccess
я понимаю суть и предлагаются подобные движухи:
if var is not None:
if var is None:
assert var is not None
но как по мне - это такая узбыточность, ведь подобных мест реально очень много
и понятно, что все они так или иначе идут из dag, db и redis и rabbit
может предусмотреть это где-то здесь: src/database


надо разобраться с dlq .как и где он сейчас работает, что делает, зачем? какие лимиты? в общем детально распиши мне логику работы Dead-letter в том виде, как она есть сейчас.

обработка overload.
нужно искусттвено где-то грамотно задать, зашить, реализовать, запрограммировать фундаментальное ограничение системы: количество обрабатываемых, записываемых в Memgraph (потому-что это самое узкое место) транзакций в секунду.
логика такая: при перегрузке они начнут копиться в очереди in, при достижении лимита T - они попадают в dead-letter. очередь постепенно рассасывается, когда overload проходит и сообщения из DL переходят в in

И меня есть такие наработки на эту тему:

```python
async def connect(self):
    """Инициализация подключения к RabbitMQ с dead-letter очередью."""
    self.connection = await connect_robust(f"amqp://guest:guest@localhost/")
    self.channel = await self.connection.channel()
    await self.channel.set_qos(prefetch_count=250) #TODO понять что делает
    # Создание dead-letter exchange
    dlx_exchange = await self.channel.declare_exchange(
        "dlx_exchange",
        type="direct",
        durable=True
    )
    
    # Создание dead-letter очереди
    dl_queue = await self.channel.declare_queue(
        "dead-letter",
        durable=True
    )
```  
# Привязка dead-letter очереди к exchange
```python
T = 100

await dl_queue.bind(dlx_exchange, routing_key="in")
async def consume_messages(self, queue_name: str, callback: callable, stop_event=None):
    """Потребление сообщений из очереди."""
    # Объявляем очередь с ограничением на T сообщений и dead-letter конфигурацией
    queue = await self.channel.declare_queue(
        queue_name,
        durable=True,
        arguments={
            "x-max-length": T,  # Ограничение на максимальное количество сообщений
            "x-dead-letter-exchange": "dlx_exchange",  # Exchange для отклоненных сообщений
            "x-dead-letter-routing-key": "in"  # Routing key для dead-letter очереди
        }
    )
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            if stop_event and stop_event.is_set():
                logger.info("Stop event detected, stopping message consumption")
                break
            await callback(message)
```
# не знаю это нужно или нет:::
```python
async def retry_dead_letter(self):
    """Попытка повторной отправки сообщений из dead-letter очереди в tx."""
    queue = await self.channel.declare_queue("dead-letter", durable=True)
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            try:
                await self.publish_message("in", message.body.decode())
                await message.ack()
                logger.info("Successfully retried dead-letter message")
            except Exception as e:
                logger.error(f"Failed to retry dead-letter message: {str(e)}")
                await message.nack(requeue=True)  # Возвращаем в dead-letter, если не удалось
```

проверить и прокомментить весь lifecycle транзакции (исключае минт genesis) со всеми исключениями и ветвлениями - сначала в виде документа с хронологией. хорошо бы вообще svg граф на эту тему. API -> Consumer -> Confirmer -> Writer - и АБСОЛЮТНО ВСЕ действия в хронологическом порядке, образующиеся циклы, ветвления, проверки.

проверять целостность графа Mongo

alpha, beta - дефолтные параметры.. в будущем динамические

метрики influx