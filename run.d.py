import asyncio
import logging
import sys
from multiprocessing import Process, Event
import signal
from pathlib import Path
import threading
import time
from watchfiles import watch, DefaultFilter

logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s - %(process)d - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

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
      await service.run(stop_event)
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

class ProcessManager:
  def __init__(self, service_type, num_processes):
    self.service_type = service_type
    self.num_processes = num_processes
    self.processes = []
    self.stop_events = []
    self.running = False
    self.GRACEFUL_TIMEOUT = 1
    self.last_restart = 0
    self.DEBOUNCE_INTERVAL = 1

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

    for stop_event in self.stop_events:
      stop_event.set()

    for process in self.processes:
        process.terminate()
        process.join()

    self.running = False
    logger.info(f"All processes for {self.service_type} stopped")

def main():
  args = sys.argv[1:]
  service_type = args[0] if args else None
  num_processes = int(args[1]) if len(args) > 1 else 1

  if not service_type:
    logger.error("Service type not provided")
    sys.exit(1)

  manager = ProcessManager(service_type, num_processes)
  manager.start_processes()

  restart_event = threading.Event()

  def watch_files():
    for changes in watch(".", recursive=True, watch_filter=DefaultFilter()):
      for change in changes:
        path = Path(change[1]).resolve().as_posix().lower()
        if any(ignored in path for ignored in ["__pycache__", "env", ".git", ".idea", ".vscode"]):
          continue
        if path.endswith(".pyc") or Path(change[1]).name.startswith((".", "~")):
          continue
        if not path.endswith(".py"):
          continue
        logger.info(f"Detected file change: {change[1]}")
        restart_event.set()
        break

  watching_thread = threading.Thread(target=watch_files, daemon=True)
  watching_thread.start()

  def signal_handler(sig, frame):
    logger.info("Received shutdown signal, stopping processes...")
    manager.stop_processes()
    sys.exit(0)

  signal.signal(signal.SIGINT, signal_handler)
  signal.signal(signal.SIGTERM, signal_handler)

  try:
    while True:
      if restart_event.is_set():
        logger.info("Restart signal received, restarting processes...")
        restart_event.clear()
        manager.start_processes()
      time.sleep(0.5)
      for process in manager.processes:
        if not process.is_alive():
          logger.error(f"Process {process.pid} unexpectedly stopped, restarting...")
          manager.start_processes()
          break
  except Exception as e:
    logger.error(f"Main process error: {e}")
  finally:
    manager.stop_processes()

if __name__ == "__main__":
  main()