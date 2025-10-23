from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_async_session
from app.services.metrics_service import WorkOrderMetrics

router = APIRouter(prefix="/metrics", tags=["Metrics"])

@router.get("/dynamic-sum")
async def dynamic_sum(
    location_id: int = 0,
    project_type_id: int = 0,
    status_id: int = 0,
    year: int = 0,
    month: int = 0,
    db: AsyncSession = Depends(get_async_session),
):
    """
    Returns work orders aggregated by all unfiltered dimensions.
    total_count = sum of all combinations in chart_data.
    """
    metrics = WorkOrderMetrics(
        location_id=location_id,
        project_type_id=project_type_id,
        status_id=status_id,
        year=year,
        month=month
    )
    return await metrics.aggregate(db)
