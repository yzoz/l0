import argparse
import asyncio
import logging
import sys

from watchfiles import Change, awatch

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SERVICE_TYPES = ("cons", "conf", "write")


async def run_service(service_type):
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

    try:
        await service.run()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception:
        logger.error("Service failed", exc_info=True)
    finally:
        await service.shutdown()


async def watch_and_run():
    parser = argparse.ArgumentParser(description="Запуск сервиса с перезапуском по изменениям файлов.")
    parser.add_argument("service_type", choices=SERVICE_TYPES, help="тип сервиса")
    args = parser.parse_args(sys.argv[1:])
    service_type = args.service_type

    watch_dirs = ["service", "src"]  # Directory to watch for changes
    logger.info("Starting service '%s' with file watching on %s", service_type, watch_dirs)

    while True:
        try:
            # Run the service in the background
            service_task = asyncio.create_task(run_service(service_type))

            # Watch for file changes
            async for changes in awatch(*watch_dirs):
                for change, path in changes:
                    if change == Change.modified:
                        logger.info("Detected change in %s. Restarting service...", path)
                        service_task.cancel()  # Cancel the running service
                        try:
                            await service_task  # Wait for cancellation
                        except asyncio.CancelledError:
                            logger.info("Previous service instance cancelled")
                        # Start a new service instance
                        service_task = asyncio.create_task(run_service(service_type))
                        break  # Break inner loop to continue watching

            await service_task  # Wait for the service to complete if no changes

        except KeyboardInterrupt:
            logger.info("Watcher stopped by user")
            if "service_task" in locals():
                service_task.cancel()
                try:
                    await service_task
                except asyncio.CancelledError:
                    pass
            break


if __name__ == "__main__":
    asyncio.run(watch_and_run())
