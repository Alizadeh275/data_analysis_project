from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from app.core.database import get_async_session
from app.services.metrics_service import WorkOrderMetrics

router = APIRouter(prefix="/metrics", tags=["Metrics"])

@router.get("/aggregate")
async def aggregate_metrics(
    location_id: Optional[int] = None,
    project_type_id: Optional[int] = None,
    status_id: Optional[int] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    group_by: List[str] = Query(default=[]),
    db: AsyncSession = Depends(get_async_session),
):
    """
    Aggregate work order metrics based on selected filters and group_by dimensions.
    Example:
      /metrics/aggregate?group_by=location&group_by=year
      /metrics/aggregate?location_id=5&group_by=status
    """
    metrics = WorkOrderMetrics(
        location_id=location_id,
        project_type_id=project_type_id,
        status_id=status_id,
        year=year,
        month=month,
        group_by=group_by,
    )
    return await metrics.aggregate(db)
