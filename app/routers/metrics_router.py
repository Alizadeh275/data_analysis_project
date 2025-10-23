from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_async_session
from app.services.metrics_service import get_workorder_sum

router = APIRouter(prefix="/metrics", tags=["Metrics"])


@router.get("/sum")
async def get_open_workorder_sum(
    location_id: int = 0,
    date_id: int = 0,
    project_type_id: int = 0,
    status_id: int = 0,
    db: AsyncSession = Depends(get_async_session),
):
    """
    Returns total open work orders filtered by dimensions (optional).
    Pass 0 to ignore any filter.
    """
    return await get_workorder_sum(
        db,
        location_id=location_id,
        date_id=date_id,
        project_type_id=project_type_id,
        status_id=status_id,
    )
