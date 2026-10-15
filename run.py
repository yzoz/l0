import argparse
import asyncio
import logging
import signal
import sys
import time
from multiprocessing import Event, Process

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(process)d - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SERVICE_TYPES = ("cons", "conf", "write")


def run_process(stop_event, s):
    async def async_run():
        if s == "cons":
            from service.consumer import ConsumerService

            service = ConsumerService()
        elif s == "conf":
            from service.confirmer import ConfirmationService

            service = ConfirmationService()
        elif s == "write":
            from service.writer import WriterService

            service = WriterService()
        else:
            # Сюда попасть нельзя: тип сервиса валидируется в main().
            logger.error("Unknown service type: %s", s)
            return

        try:
            await service.run(stop_event)
        except Exception:
            logger.error("Process failed", exc_info=True)
        finally:
            await service.shutdown()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(async_run())
    except KeyboardInterrupt:
        logger.info("Process interrupted")
    finally:
        loop.close()


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Запуск фоновых сервисов реестра.")
    parser.add_argument("service", choices=SERVICE_TYPES, help="тип сервиса")
    parser.add_argument(
        "processes",
        nargs="?",
        type=int,
        default=1,
        help="количество процессов (по умолчанию 1)",
    )
    args = parser.parse_args(argv)
    if args.processes < 1:
        parser.error("число процессов должно быть не меньше 1")
    return args


def main():
    args = parse_args(sys.argv[1:])
    num_processes = args.processes

    processes = []
    stop_events = []

    for _ in range(num_processes):
        stop_event = Event()
        process = Process(target=run_process, args=(stop_event, args.service))
        processes.append(process)
        stop_events.append(stop_event)
        process.start()

    logger.info("Started %s processes", num_processes)

    def signal_handler(sig, frame):
        logger.info("Received shutdown signal, stopping processes...")
        for stop_event in stop_events:
            stop_event.set()

        start_time = time.time()
        for process in processes:
            if process.is_alive():
                logger.info("Waiting for process %s to stop gracefully...", process.pid)
                process.join(timeout=10 - (time.time() - start_time))
                if process.is_alive():
                    logger.warning("Process %s did not stop gracefully, terminating...", process.pid)
                    process.terminate()
                    process.join()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        for process in processes:
            process.join()
    except Exception:
        logger.error("Main process error", exc_info=True)
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join()


if __name__ == "__main__":
    main()
