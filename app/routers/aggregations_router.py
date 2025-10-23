from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from app.core.database import get_async_session
from app.services.aggregation_service import WorkOrderMetrics

router = APIRouter(prefix="/aggregations", tags=["Aggregations"])

ALLOWED_GROUP_BY = {"location", "project_type", "status", "year", "month"}
ALLOWED_ORDER_DIR = {"asc", "desc"}


@router.get("/sum")
async def sum_aggregate(
    location_id: Optional[int] = None,
    project_type_id: Optional[int] = None,
    status_id: Optional[int] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    group_by: List[str] = Query(default=[]),
    order_by: Optional[str] = Query(None, description="Field to order by: count or one of group_by fields"),
    order_dir: str = Query("desc", description="Order direction: asc or desc"),
    db: AsyncSession = Depends(get_async_session),
):
    """
    Return the **sum aggregation** of work order metrics based on optional filters
    and user-selected group_by dimensions.

    **Filters (all optional):**
    - `location_id`
    - `project_type_id`
    - `status_id`
    - `year`
    - `month`

    **Group by dimensions:**
    - `group_by` accepts a list of dimensions to group results by.
    - Allowed values: "location", "project_type", "status", "year", "month"

    **Ordering:**
    - `order_by` → "count" or any of the selected `group_by` fields
    - `order_dir` → "asc" or "desc"

    Returns 422 if unsupported `group_by` fields or invalid order fields are provided.
    """
    # --- Validate group_by fields ---
    invalid_fields = [f for f in group_by if f not in ALLOWED_GROUP_BY]
    if invalid_fields:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported group_by fields: {invalid_fields}. Allowed values: {list(ALLOWED_GROUP_BY)}",
        )

    # --- Validate order_by ---
    if order_by:
        allowed_order_fields = group_by + ["count"]
        if order_by not in allowed_order_fields:
            raise HTTPException(
                status_code=422, detail=f"Invalid order_by field: {order_by}. Must be one of: {allowed_order_fields}"
            )
    if order_dir not in ALLOWED_ORDER_DIR:
        raise HTTPException(status_code=422, detail=f"Invalid order_dir: {order_dir}. Must be 'asc' or 'desc'")

    metrics = WorkOrderMetrics(
        location_id=location_id,
        project_type_id=project_type_id,
        status_id=status_id,
        year=year,
        month=month,
        group_by=group_by,
        order_by=order_by,
        order_dir=order_dir,
    )
    return await metrics.aggregate(db)
