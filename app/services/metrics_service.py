from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import FactWorkOrder


async def get_workorder_sum(
    db: AsyncSession,
    location_id: int = 0,
    date_id: int = 0,
    project_type_id: int = 0,
    status_id: int = 0,
):
    """
    Returns the sum of work orders filtered by optional dimensions.
    """

    query = select(func.sum(FactWorkOrder.count))

    filters = []
    if location_id != 0:
        filters.append(FactWorkOrder.location_id == location_id)
    if date_id != 0:
        filters.append(FactWorkOrder.date_id == date_id)
    if project_type_id != 0:
        filters.append(FactWorkOrder.project_type_id == project_type_id)
    if status_id != 0:
        filters.append(FactWorkOrder.status_id == status_id)

    if filters:
        query = query.where(and_(*filters))

    result = await db.execute(query)
    total = result.scalar() or 0
    return {"total_work_orders": total}
