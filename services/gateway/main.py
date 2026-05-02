from fastapi import FastAPI

from .routes import router


app = FastAPI(title="Transaction Gateway")


@app.get("/health")
def health():
    return {"status": "ok"}


app.include_router(router)
