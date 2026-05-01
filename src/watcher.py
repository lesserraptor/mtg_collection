"""watcher.py — Watchdog file monitor for automatic collection reload.

Architecture:
  - Watchdog Observer runs in its own OS thread (cross-platform FSEvents/inotify).
  - CollectionFileHandler.on_modified() is called from that OS thread.
  - loop.call_soon_threadsafe(queue.put_nowait, path) bridges to the asyncio event loop.
  - collection_reload_consumer() is an asyncio coroutine that dequeues events,
    debounces duplicates (2s sleep + drain), then calls upsert_collection() on
    app.state.db which is on the asyncio thread — safe, no cross-thread DB access.

SQLite isolation: upsert_collection() uses app.state.db (asyncio thread only).
The watchdog OS thread never touches any DB connection.
"""

import asyncio
import logging
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from src.collection import find_collection_file, upsert_collection

logger = logging.getLogger(__name__)


class CollectionFileHandler(FileSystemEventHandler):
    """Bridges watchdog OS-thread events to the asyncio event loop queue."""

    def __init__(self, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue, target_path: Path):
        self._loop = loop
        self._queue = queue
        self._target = str(target_path.resolve())

    def on_modified(self, event):
        if event.is_directory:
            return
        if str(Path(event.src_path).resolve()) != self._target:
            return
        # Only thread-safe call allowed from a non-asyncio thread:
        self._loop.call_soon_threadsafe(self._queue.put_nowait, event.src_path)


async def collection_reload_consumer(app, queue: asyncio.Queue) -> None:
    """Asyncio coroutine: dequeue file events, debounce, reload collection."""
    while True:
        path = await queue.get()
        # Debounce: MTGA writes the DB in multiple flushes; wait 2s then drain
        await asyncio.sleep(2.0)
        while not queue.empty():
            queue.get_nowait()
        try:
            collection_path = find_collection_file()
            if collection_path is None:
                logger.warning("watcher: collection file not found after modification event")
                continue
            count = upsert_collection(app.state.db, collection_path)
            logger.info("watcher: auto-reload triggered — %d rows processed", count)
        except Exception:
            logger.exception("watcher: error during auto-reload")


async def start_watcher(app, collection_path: Path) -> None:
    """Start the watchdog Observer and the asyncio consumer coroutine.

    Stores observer and queue on app.state so shutdown() can stop the observer.
    Watches the DIRECTORY containing collection_path (watchdog requires directory
    watches); CollectionFileHandler filters to only the target file.
    """
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()

    handler = CollectionFileHandler(loop, queue, collection_path)
    observer = Observer()
    observer.schedule(handler, path=str(collection_path.parent.resolve()), recursive=False)
    observer.start()

    app.state.watcher_observer = observer
    app.state.watcher_queue = queue

    asyncio.create_task(collection_reload_consumer(app, queue))
    logger.info("watcher: started watching %s", collection_path)
