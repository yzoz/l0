import asyncio
import logging
import sys
from watchfiles import awatch, Change

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_service(service_type):
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
            logger.error("Invalid service type. Use 'cons', 'conf', or 'write'")
            sys.exit(1)
        
        await service.run()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception as e:
        logger.error(f"Service failed: {e}", exc_info=True)
    finally:
        await service.shutdown()

async def watch_and_run():
    args = sys.argv[1:]
    try:
        service_type = args[0]
    except IndexError:
        logger.error("Service type required (cons, conf, write)")
        sys.exit(1)

    watch_dirs = ['service', 'src']  # Directory to watch for changes
    logger.info(f"Starting service '{service_type}' with file watching on {watch_dirs}")

    while True:
        try:
            # Run the service in the background
            service_task = asyncio.create_task(run_service(service_type))
            
            # Watch for file changes
            async for changes in awatch(*watch_dirs):
                for change, path in changes:
                    if change == Change.modified:
                        logger.info(f"Detected change in {path}. Restarting service...")
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
            if 'service_task' in locals():
                service_task.cancel()
                try:
                    await service_task
                except asyncio.CancelledError:
                    pass
            break

if __name__ == "__main__":
    asyncio.run(watch_and_run())