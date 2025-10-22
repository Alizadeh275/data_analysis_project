from fastapi import FastAPI, Depends, HTTPException
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import Base, engine, get_async_session
from app.services.etl_service import run_etl

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()

app = FastAPI(title="Work Orders ETL", lifespan=lifespan)

@app.post("/refresh-workorders/")
async def refresh_workorders(db: AsyncSession = Depends(get_async_session)):
    try:
        result = await run_etl(db)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
