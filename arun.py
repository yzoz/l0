import asyncio
import logging
import sys
import signal
from multiprocessing import Process, Event
from watchfiles import awatch, Change
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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

async def watch_files(processes, stop_events, service_type, num_processes):
    """Следит за изменениями файлов и перезапускает процессы"""
    watch_dirs = ['service', 'src']  # Директории для отслеживания
    async for changes in awatch(*watch_dirs, rust_timeout=1000):
        for change, path in changes:
            if change == Change.modified:
                logger.info(f"File {path} changed, restarting processes...")
                # Останавливаем текущие процессы
                for stop_event in stop_events:
                    stop_event.set()
                for process in processes:
                    process.terminate()
                    process.join()
                
                # Очищаем списки
                processes.clear()
                stop_events.clear()
                
                # Запускаем новые процессы
                for _ in range(num_processes):
                    stop_event = Event()
                    process = Process(target=run_process, args=(stop_event, service_type))
                    processes.append(process)
                    stop_events.append(stop_event)
                    process.start()
                
                logger.info(f"Restarted {num_processes} processes for service {service_type}")
                break  # Прерываем цикл изменений, чтобы не обрабатывать остальные файлы в этом событии

def main():
    args = sys.argv[1:]
    is_dev = 'dev' in args
    if is_dev:
        args.remove('dev')
    
    try:
        service_type = args[0]
        n = int(args[1]) if len(args) > 1 else 1
    except IndexError:
        logger.error("Service type required (cons, conf, write)")
        sys.exit(1)

    NUM_PROCESSES = n
    processes = []
    stop_events = []

    # Запускаем процессы
    for _ in range(NUM_PROCESSES):
        stop_event = Event()
        process = Process(target=run_process, args=(stop_event, service_type))
        processes.append(process)
        stop_events.append(stop_event)
        process.start()

    logger.info(f"Started {NUM_PROCESSES} processes for service {service_type}")

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
        if is_dev:
            # Запускаем наблюдение за файлами в режиме разработки
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(watch_files(processes, stop_events, service_type, NUM_PROCESSES))
        else:
            # Ожидаем завершения процессов
            for process in processes:
                process.join()
    except Exception as e:
        logger.error(f"Main process error: {e}", exc_info=True)
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join()

if __name__ == "__main__":
    main()