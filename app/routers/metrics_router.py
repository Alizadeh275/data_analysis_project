from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_async_session


router = APIRouter(prefix="/metrics", tags=["Metrics"])

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_async_session
from app.services.metrics_service import MetricsService

router = APIRouter(prefix="/metrics", tags=["Metrics"])

@router.get("/total-open-workorders/")
async def get_total_workorders(
    project_type_id: int,
    status_id: int,
    year: int,
    month: int,
    db: AsyncSession = Depends(get_async_session)
):
    result = await MetricsService.get_total_workorders_sum(
        db=db,
        project_type_id=project_type_id,
        status_id=status_id,
        year=year,
        month=month
    )
    return result
