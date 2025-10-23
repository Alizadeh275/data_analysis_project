from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import FactWorkOrder, DimDate


async def get_workorder_sum(
    db: AsyncSession,
    location_id: int = 0,
    year: int = 0,
    month: int = 0,
    project_type_id: int = 0,
    status_id: int = 0,
):
    """
    Returns the sum of work orders filtered by optional dimensions.
    year/month: 0 means no filter
    """
    query = select(func.sum(FactWorkOrder.count))
    filters = []

    if location_id != 0:
        filters.append(FactWorkOrder.location_id == location_id)
    if project_type_id != 0:
        filters.append(FactWorkOrder.project_type_id == project_type_id)
    if status_id != 0:
        filters.append(FactWorkOrder.status_id == status_id)

    # تاریخ: join با DimDate
    if year != 0 or month != 0:
        query = query.join(DimDate, FactWorkOrder.date_id == DimDate.id)
        if year != 0:
            filters.append(DimDate.year == year)
        if month != 0:
            filters.append(DimDate.month == month)

    if filters:
        query = query.where(and_(*filters))

    result = await db.execute(query)
    total = result.scalar() or 0
    return {"total_work_orders": total}
