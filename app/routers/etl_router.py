from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_async_session
from app.services.etl_service.runner import WorkOrderETLManager

router = APIRouter(prefix="/etl", tags=["ETL"])

@router.post("/refresh")
async def refresh(db: AsyncSession = Depends(get_async_session)):
    try:
        etl_manager = WorkOrderETLManager(db)
        return await etl_manager.run()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))