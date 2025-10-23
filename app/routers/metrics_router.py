from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from app.core.database import get_async_session
from app.services.metrics_service import WorkOrderMetrics

router = APIRouter(prefix="/metrics", tags=["Metrics"])

ALLOWED_GROUP_BY = {"location", "project_type", "status", "year", "month"}

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
    Returns 422 if unsupported group_by fields are provided.
    """
    # --- Validate group_by fields ---
    invalid_fields = [f for f in group_by if f not in ALLOWED_GROUP_BY]
    if invalid_fields:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported group_by fields: {invalid_fields}. Allowed values: {list(ALLOWED_GROUP_BY)}"
        )

    metrics = WorkOrderMetrics(
        location_id=location_id,
        project_type_id=project_type_id,
        status_id=status_id,
        year=year,
        month=month,
        group_by=group_by,
    )
    return await metrics.aggregate(db)
