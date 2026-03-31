"""inotify watch loop + periodic fallback for config change detection."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Callable, Coroutine

import structlog
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

log = structlog.get_logger()


class ConfigWatcher:
    """Watch /config/ for changes and call on_change when a YAML file is modified.

    Primary: inotify via watchdog (sub-second reaction).
    Fallback: poll every poll_interval seconds to catch changes inotify might miss
    (e.g., ConfigMap remounts, network filesystem quirks).
    """

    def __init__(
        self,
        watch_dir: Path,
        poll_interval: int,
        on_change: Callable[[], Coroutine],
    ) -> None:
        self._watch_dir = watch_dir
        self._poll_interval = poll_interval
        self._on_change = on_change
        self._loop = asyncio.get_event_loop()
        self._pending = asyncio.Event()

    def _schedule_reconcile(self) -> None:
        self._loop.call_soon_threadsafe(self._pending.set)

    async def run(self) -> None:
        handler = _Handler(self._schedule_reconcile)
        observer = Observer()
        observer.schedule(handler, str(self._watch_dir), recursive=True)
        observer.start()
        log.info("watching config dir", path=str(self._watch_dir))

        try:
            while True:
                try:
                    await asyncio.wait_for(
                        self._wait_for_event(), timeout=self._poll_interval
                    )
                except asyncio.TimeoutError:
                    # Periodic fallback: always trigger a hash check
                    pass
                await self._on_change()
                self._pending.clear()
        finally:
            observer.stop()
            observer.join()

    async def _wait_for_event(self) -> None:
        await self._pending.wait()


class _Handler(FileSystemEventHandler):
    def __init__(self, callback: Callable) -> None:
        self._callback = callback

    def on_modified(self, event: FileSystemEvent) -> None:
        if not event.is_directory and str(event.src_path).endswith(".yaml"):
            self._callback()

    def on_created(self, event: FileSystemEvent) -> None:
        self.on_modified(event)
