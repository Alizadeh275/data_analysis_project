from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.core.database import Base, engine
from app.routers.aggregations_router import router as aggregations_router
from app.routers.etl_router import router as etl_router
from app.routers.dimensions_router import router as dimensions_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


app = FastAPI(title="Open Work Orders App", lifespan=lifespan)

# Register Routers
app.include_router(etl_router, prefix="/open-work-orders")
app.include_router(dimensions_router, prefix="/open-work-orders")
app.include_router(aggregations_router, prefix="/open-work-orders")
