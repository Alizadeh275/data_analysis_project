from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from app.core.database import get_async_session
from app.services.aggregation_service import WorkOrderMetrics

router = APIRouter(prefix="/aggregations", tags=["Aggregations"])

ALLOWED_GROUP_BY = {"location", "project_type", "status", "year", "month"}

@router.get("/sum")
async def sum_aggregate(
    location_id: Optional[int] = None,
    project_type_id: Optional[int] = None,
    status_id: Optional[int] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    group_by: List[str] = Query(default=[]),
    db: AsyncSession = Depends(get_async_session),
):
    """
    Return the **sum aggregation** of work order metrics based on optional filters 
    and user-selected group_by dimensions.

    **Filters (all optional):**
    - `location_id` → filter by location
    - `project_type_id` → filter by project type
    - `status_id` → filter by work order status
    - `year` → filter by year
    - `month` → filter by month

    **Group by dimensions:**
    - `group_by` accepts a list of dimensions to group results by.
    - Allowed values: `"location"`, `"project_type"`, `"status"`, `"year"`, `"month"`

    **Examples:**
    - `/aggregations/sum?group_by=location`
    - `/aggregations/sum?year=2024&group_by=month`
    - `/aggregations/sum?group_by=location&group_by=status`

    Returns **422** if unsupported `group_by` fields are provided.

    **Note:** This endpoint currently only performs **sum aggregation**. 
    Future endpoints may support other aggregation types (e.g., avg, max, min).
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
