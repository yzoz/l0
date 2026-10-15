import argparse
import asyncio
import logging
import sys

from watchfiles import Change, awatch

from src.telemetry import metrics, start_dumper

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SERVICE_TYPES = ("cons", "conf", "write")
WATCH_DIRS = ["service", "src"]


async def run_service(service_type):
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
            # Тип сервиса валидируется в parse_args(), сюда попасть нельзя.
            logger.error("Invalid service type. Use 'cons', 'conf', or 'write'")
            return

        await service.run()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception:
        logger.error("Service failed", exc_info=True)
    finally:
        if service is not None:
            await service.shutdown()


async def watch_and_run():
    parser = argparse.ArgumentParser(description="Запуск сервиса L0.")
    parser.add_argument("service_type", choices=SERVICE_TYPES, help="тип сервиса")
    parser.add_argument(
        "--watch",
        action="store_true",
        help="режим разработки: перезапуск при изменениях .py (по умолчанию выключен — прод)",
    )
    args = parser.parse_args(sys.argv[1:])
    service_type = args.service_type

    metrics.set_role(service_type)
    dumper = start_dumper(service_type)

    if not args.watch:
        # Прод-режим (дефолт): сервис крутится сам, без file-watching.
        logger.info("Starting service '%s' (prod mode, no file watching)", service_type)
        try:
            await run_service(service_type)
        finally:
            dumper.cancel()
        return

    # Режим разработки: следим за .py и перезапускаем сервис. Best-effort —
    # если вотчер не поднялся (например, исчерпан inotify), работаем без reload.
    logger.info("Starting service '%s' with file watching on %s", service_type, WATCH_DIRS)
    service_task = asyncio.create_task(run_service(service_type))

    try:
        try:
            async for changes in awatch(*WATCH_DIRS):
                py_changes = [(change, path) for change, path in changes if path.endswith(".py")]
                if not py_changes:
                    continue

                for change, path in py_changes:
                    logger.info("Detected %s in %s", Change(change).name, path)
                logger.info("Restarting service...")

                service_task.cancel()
                try:
                    await service_task
                except asyncio.CancelledError:
                    logger.info("Previous service instance cancelled")
                # Локальные счётчики сбрасываются при перезапуске сервиса.
                metrics.reset()
                service_task = asyncio.create_task(run_service(service_type))
        except Exception as exc:
            logger.warning("File watching unavailable (%s); continuing without reload", exc)
            await service_task
    finally:
        dumper.cancel()
        if not service_task.done():
            service_task.cancel()
        await asyncio.gather(service_task, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(watch_and_run())
    except KeyboardInterrupt:
        logger.info("Watcher stopped by user")
