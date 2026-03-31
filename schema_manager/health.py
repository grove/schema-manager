"""/health, /ready, /reconcile, /promote HTTP endpoints (port 9080)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import structlog
from aiohttp import web

if TYPE_CHECKING:
    from schema_manager.reconciler import Reconciler

log = structlog.get_logger()


def build_app(reconciler: "Reconciler") -> web.Application:
    app = web.Application()
    app["reconciler"] = reconciler
    app.router.add_get("/health", handle_health)
    app.router.add_get("/ready", handle_ready)
    app.router.add_post("/reconcile", handle_reconcile)
    app.router.add_post("/promote", handle_promote)
    return app


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def handle_ready(request: web.Request) -> web.Response:
    reconciler: Reconciler = request.app["reconciler"]
    if reconciler.is_ready:
        return web.json_response({"status": "ready"})
    return web.json_response({"status": "not_ready"}, status=503)


async def handle_reconcile(request: web.Request) -> web.Response:
    reconciler: Reconciler = request.app["reconciler"]
    dry_run = "dry_run" in request.rel_url.query
    if dry_run:
        asyncio.create_task(reconciler.reconcile_once(dry_run=True))
        return web.json_response({"status": "triggered", "dry_run": True})
    try:
        await reconciler.reconcile_once(dry_run=False)
        return web.json_response({"status": "completed"})
    except Exception as exc:
        log.error("reconcile failed", error=str(exc))
        return web.json_response({"status": "error", "detail": str(exc)}, status=500)


async def handle_promote(request: web.Request) -> web.Response:
    from schema_manager.shadow import promote
    reconciler: Reconciler = request.app["reconciler"]
    try:
        await promote(reconciler.config_path)
        return web.json_response({"status": "promoted"})
    except Exception as exc:
        log.error("promote failed", error=str(exc))
        return web.json_response({"status": "error", "detail": str(exc)}, status=500)


async def start_server(app: web.Application, listen: str) -> None:
    host, port = listen.rsplit(":", 1)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, int(port))
    await site.start()
    log.info("health server listening", address=listen)
