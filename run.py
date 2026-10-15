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
    parser = argparse.ArgumentParser(description="Запуск сервиса с перезапуском по изменениям файлов.")
    parser.add_argument("service_type", choices=SERVICE_TYPES, help="тип сервиса")
    args = parser.parse_args(sys.argv[1:])
    service_type = args.service_type

    logger.info("Starting service '%s' with file watching on %s", service_type, WATCH_DIRS)
    metrics.set_role(service_type)
    dumper = start_dumper(service_type)
    service_task = asyncio.create_task(run_service(service_type))

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
    finally:
        dumper.cancel()
        service_task.cancel()
        await asyncio.gather(service_task, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(watch_and_run())
    except KeyboardInterrupt:
        logger.info("Watcher stopped by user")
