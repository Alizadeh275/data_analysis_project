from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.core.database import Base, engine
from app.routers.metrics_router import router as metrics_router
from app.routers.etl_router import router as etl_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()

app = FastAPI(title="Open Work Orders App", lifespan=lifespan)

# Register Routers
app.include_router(metrics_router)
app.include_router(etl_router)



