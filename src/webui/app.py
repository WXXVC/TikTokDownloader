import asyncio

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src.application.main_server import APIServer

from .config import APP_NAME, APP_VERSION, FRONTEND_DIR
from .db import ensure_database
from .routers import creators, engine, health, panel, profiles, scans, tasks
from .services.auto_download_runtime import (
    bind_auto_download_wake_event,
    scheduler_loop,
)
from .services.tasks import task_dispatcher_loop, task_runtime_cleanup_loop


def build_api_app(parameter, database) -> FastAPI:
    api_app = FastAPI(title="DouK-Downloader API", version=APP_VERSION)
    api_server = APIServer(parameter, database)
    api_server.server = api_app
    api_server.setup_routes()
    return api_app


def create_webui_app(parameter, database) -> FastAPI:
    ensure_database()

    app = FastAPI(title=APP_NAME, version=APP_VERSION)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.mount("/api", build_api_app(parameter, database))
    app.include_router(health.router, prefix="/panel-api")
    app.include_router(profiles.router, prefix="/panel-api")
    app.include_router(creators.router, prefix="/panel-api")
    app.include_router(scans.router, prefix="/panel-api")
    app.include_router(engine.router, prefix="/panel-api")
    app.include_router(panel.router, prefix="/panel-api")
    app.include_router(tasks.router, prefix="/panel-api")
    app.mount(
        "/webui-static",
        StaticFiles(directory=FRONTEND_DIR),
        name="webui-static",
    )

    @app.on_event("startup")
    async def startup_scheduler():
        app.state.auto_download_stop = asyncio.Event()
        app.state.auto_download_wake = asyncio.Event()
        bind_auto_download_wake_event(app.state.auto_download_wake)
        app.state.auto_download_task = asyncio.create_task(
            scheduler_loop(app.state.auto_download_stop)
        )
        app.state.task_dispatcher_stop = asyncio.Event()
        app.state.task_dispatcher_task = asyncio.create_task(
            task_dispatcher_loop(app.state.task_dispatcher_stop)
        )
        app.state.task_cleanup_stop = asyncio.Event()
        app.state.task_cleanup_task = asyncio.create_task(
            task_runtime_cleanup_loop(app.state.task_cleanup_stop)
        )

    @app.on_event("shutdown")
    async def shutdown_scheduler():
        stop_events = [
            getattr(app.state, "auto_download_stop", None),
            getattr(app.state, "task_dispatcher_stop", None),
            getattr(app.state, "task_cleanup_stop", None),
        ]
        tasks_to_wait = [
            getattr(app.state, "auto_download_task", None),
            getattr(app.state, "task_dispatcher_task", None),
            getattr(app.state, "task_cleanup_task", None),
        ]
        for stop_event in stop_events:
            if stop_event:
                stop_event.set()
        for task in tasks_to_wait:
            if task:
                await task

    @app.get("/", include_in_schema=False)
    async def webui_index():
        return FileResponse(FRONTEND_DIR / "index.html")

    return app
