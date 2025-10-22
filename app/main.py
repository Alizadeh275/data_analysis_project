# main.py
from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.database import Base, engine  # AsyncEngine
import app.models  # ensure all models are imported

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    # Shutdown: optional cleanup, if needed
    await engine.dispose()


app = FastAPI(title="FastAPI + Postgres Example", lifespan=lifespan)


@app.get("/")
def root():
    return {"message": "🚀 FastAPI + PostgreSQL + Docker is running!"}
