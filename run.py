import asyncio
import logging
import sys
from multiprocessing import Process, Event
import signal
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, restart_callback):
        self.restart_callback = restart_callback
        self.last_modified = time.time()

    def on_any_event(self, event):
        path = event.src_path.lower()
        if any(ignored in path for ignored in ['__pycache__', 'env', '.git']) or path.endswith('.pyc'):
            return
        if event.is_directory:
            return
        if not path.endswith('.py'):
            return
        current_time = time.time()
        if current_time - self.last_modified > 1:
            self.last_modified = current_time
            logger.info(f"Detected file change: {event.src_path}")
            self.restart_callback()

def run_process(stop_event, service_type):
    async def async_run():
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
                logger.error(f"Unknown service type: {service_type}")
                return
            await service.run(stop_event)  # Передаём stop_event в run
        except Exception as e:
            logger.error(f"Process failed: {e}", exc_info=True)
        #finally:
            #await service.shutdown()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(async_run())
    except KeyboardInterrupt:
        logger.info("Process interrupted")
    finally:
        loop.close()

class ProcessManager:
    def __init__(self, service_type, num_processes):
        self.service_type = service_type
        self.num_processes = num_processes
        self.processes = []
        self.stop_events = []
        self.running = False
        self.GRACEFUL_TIMEOUT = 2.5

    def start_processes(self):
        self.stop_processes()
        self.processes = []
        self.stop_events = []
        for _ in range(self.num_processes):
            stop_event = Event()
            process = Process(target=run_process, args=(stop_event, self.service_type))
            self.processes.append(process)
            self.stop_events.append(stop_event)
            process.start()
        logger.info(f"Started {self.num_processes} processes for {self.service_type}")
        self.running = True

    def stop_processes(self):
        if not self.running:
            return
        logger.info(f"Stopping {self.num_processes} processes for {self.service_type} (graceful shutdown)")
        
        for stop_event in self.stop_events:
            stop_event.set()
        
        start_time = time.time()
        for process in self.processes:
            if process.is_alive():
                logger.info(f"Waiting for process {process.pid} to stop gracefully...")
                process.join(timeout=self.GRACEFUL_TIMEOUT - (time.time() - start_time))
                if process.is_alive():
                    logger.warning(f"Process {process.pid} did not stop gracefully, terminating...")
                    process.terminate()
                    process.join()
        
        self.running = False
        logger.info(f"All processes for {self.service_type} stopped")

def main():
    args = sys.argv[1:]
    dev_mode = 'dev' in args
    service_type = args[0] if args else None
    num_processes = int(args[1]) if len(args) > 1 else 1

    def restart_processes():
        manager.start_processes()
        for process in manager.processes:
            process.join()

    if not service_type:
        logger.error("Service type not provided")
        sys.exit(1)

    manager = ProcessManager(service_type, num_processes)
    manager.start_processes()

    if dev_mode:
        event_handler = FileChangeHandler(restart_callback=restart_processes)
        observer = Observer()
        observer.schedule(event_handler, path='.', recursive=True)
        observer.start()
        logger.info("Watchdog started in dev mode, monitoring .py files")
    
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal, stopping processes...")
        manager.stop_processes()
        if dev_mode:
            observer.stop()
            observer.join()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        for process in manager.processes:
            process.join()
    except Exception as e:
        logger.error(f"Main process error: {e}")
    finally:
        manager.stop_processes()
        if dev_mode:
            observer.stop()
            observer.join()


if __name__ == "__main__":
    main()