import logging
import time

from fastapi import FastAPI, Request

from common.logging import configure_logging

from .routes import router
from .settings import get_settings


settings = get_settings()

configure_logging(settings.logging_level.upper())

logger = logging.getLogger(__name__)


app = FastAPI(
    title="Ledger Service",
    openapi_url="/openapi.json" if settings.enable_docs else None,
    docs_url="/docs" if settings.enable_docs else None,
    redoc_url="/redoc" if settings.enable_docs else None,
)


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    start = time.perf_counter()

    response = await call_next(request)

    duration_ms = round((time.perf_counter() - start) * 1000, 2)

    logger.info(
        "api request completed method=%s path=%s status_code=%s duration_ms=%s",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )

    return response


@app.get("/")
def root():
    return {"service": "ledger-service"}


@app.get("/health")
def health():
    return {"status": "ok"}


app.include_router(router)
