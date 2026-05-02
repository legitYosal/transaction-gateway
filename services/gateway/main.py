import time
import logging

from fastapi import FastAPI, Request

from common.logging import configure_logging

from .settings import get_settings
from .routes import router


configure_logging(get_settings().logging_level.upper())
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Transaction Gateway",
    openapi_url="/openapi.json" if get_settings().enable_docs else None,
    docs_url="/docs" if get_settings().enable_docs else None,
    redoc_url="/redoc" if get_settings().enable_docs else None,
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

@app.get("/health")
def health():
    return {"status": "ok"}


app.include_router(router)
