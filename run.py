import asyncio
import logging
import sys
from multiprocessing import Process, Event
import signal

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_process(stop_event, s):
    async def async_run():
        if s == "cons":
            from service.consumer import ConsumerService
            service = ConsumerService()
        if s == "conf":
            from service.confirmer import ConfirmationService
            service = ConfirmationService()
        if s == "write":
            from service.writer import WriterService
            service = WriterService()
        try:
            await service.run()
        except Exception as e:
            logger.error(f"Process failed: {e}", exc_info=True)
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

def main():
    args = sys.argv[1:]

    try:
        n = args[1]
    except IndexError:
        n = 1

    NUM_PROCESSES = int(n)
    processes = []
    stop_events = []

    for _ in range(NUM_PROCESSES):
        stop_event = Event()
        process = Process(target=run_process, args=(stop_event, args[0]))
        processes.append(process)
        stop_events.append(stop_event)
        process.start()

    logger.info(f"Started {NUM_PROCESSES} processes")

    def signal_handler(sig, frame):
        logger.info("Received shutdown signal, stopping processes...")
        for stop_event in stop_events:
            stop_event.set()
        for process in processes:
            process.terminate()
            process.join()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        for process in processes:
            process.join()
    except Exception as e:
        logger.error(f"Main process error: {e}")
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join()

if __name__ == "__main__":
    main()