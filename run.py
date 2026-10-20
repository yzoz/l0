import argparse
import asyncio
import logging
import os
import sys
import threading
from multiprocessing import Event, Process

from watchfiles import PythonFilter, watch

from src.telemetry import metrics, start_dumper

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(process)d - %(levelname)s - %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

SERVICE_TYPES = ("cons", "conf", "write")
WATCH_DIRS = ["service", "src"]


async def run_service(service_type, stop_event=None):
    service = None
    try:
        if service_type == "cons":
            from service.consumer import ConsumerService

            service = ConsumerService()
        elif service_type == "conf":
            from service.confirmer import ConfirmationService

            service = ConfirmationService()
        elif service_type == "write":
            from service.writer import WriterService

            service = WriterService()
        else:
            # Тип сервиса валидируется в main(), сюда попасть нельзя.
            logger.error("Invalid service type. Use 'cons', 'conf', or 'write'")
            return

        await service.run(stop_event)
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception:
        logger.error("Service failed", exc_info=True)
    finally:
        if service is not None:
            await service.shutdown()


async def run_coroutines(service_type, c, stop_event=None):
    await asyncio.gather(*[run_service(service_type, stop_event) for _ in range(c)])


async def run_prod_coroutines(service_type, c, stop_event):
    metrics.set("coroutines", c)
    dumper = start_dumper(service_type)
    try:
        await run_coroutines(service_type, c, stop_event)
    finally:
        try:
            metrics.dump()
        except OSError:
            pass
        dumper.cancel()


def process_target(service_type, c, stop_event):
    """Функция-цель для каждого процесса (prod)."""
    metrics.set_role(f"{service_type}-w{os.getpid()}")
    try:
        asyncio.run(run_prod_coroutines(service_type, c, stop_event))
    except KeyboardInterrupt:
        logger.info("Process interrupted")


def run_prod(service_type, n, c):
    stop_event = Event()
    processes = [Process(target=process_target, args=(service_type, c, stop_event)) for _ in range(n)]
    for p in processes:
        p.start()
    logger.info("Started %d process(es) for '%s', %d coroutine(s) each", n, service_type, c)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        logger.info("Main process interrupted, stopping processes...")
        stop_event.set()
        for p in processes:
            p.join(timeout=5)
            if p.is_alive():
                p.terminate()
                p.join()


def run_watch(service_type, c):
    # Dev-режим: один процесс без multiprocessing, сервис в потоке, вотчер в главном.
    logger.info("Starting service '%s' with file watching on %s", service_type, WATCH_DIRS)
    threading.Thread(target=lambda: asyncio.run(run_coroutines(service_type, c)), daemon=True).start()
    try:
        for _ in watch(*WATCH_DIRS, watch_filter=PythonFilter()):
            logger.info("Detected change, restarting...")
            # os.execv перезапускает весь процесс, включая вотчер и поток сервиса.
            os.execv(sys.executable, [sys.executable, *sys.argv])
    except KeyboardInterrupt:
        logger.info("Watcher stopped by user")


def main():
    parser = argparse.ArgumentParser(description="Запуск сервиса L0.")
    parser.add_argument("service_type", choices=SERVICE_TYPES, help="тип сервиса")
    parser.add_argument("n", nargs="?", type=int, default=1, help="число процессов (prod); в --watch не используется")
    parser.add_argument("c", nargs="?", type=int, default=1, help="число корутин-сервисов в процессе")
    parser.add_argument("--watch", action="store_true", help="режим разработки: перезапуск при изменениях .py")
    args = parser.parse_args(sys.argv[1:])

    if args.watch:
        run_watch(args.service_type, args.c)
    else:
        run_prod(args.service_type, args.n, args.c)


if __name__ == "__main__":
    main()
