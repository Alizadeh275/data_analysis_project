from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_async_session
from app.services.etl_service import run_etl

router = APIRouter(prefix="/etl", tags=["ETL"])

@router.post("/refresh-open-workorders/")
async def refresh_workorders(db: AsyncSession = Depends(get_async_session)):
    try:
        result = await run_etl(db)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))