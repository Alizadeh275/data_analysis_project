from typing import Optional
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import FactWorkOrder, DimDate

class WorkOrderMetrics:
    def __init__(
        self,
        location_id: int = 0,
        year: int = 0,
        month: int = 0,
        project_type_id: int = 0,
        status_id: int = 0,
    ):
        self.location_id = location_id
        self.year = year
        self.month = month
        self.project_type_id = project_type_id
        self.status_id = status_id

    async def get_sum(self, db: AsyncSession) -> dict:
        """
        Returns the sum of work orders filtered by optional dimensions.
        Pass 0 to ignore a filter.
        """
        query = select(func.sum(FactWorkOrder.count))
        filters = []

        if self.location_id != 0:
            filters.append(FactWorkOrder.location_id == self.location_id)
        if self.project_type_id != 0:
            filters.append(FactWorkOrder.project_type_id == self.project_type_id)
        if self.status_id != 0:
            filters.append(FactWorkOrder.status_id == self.status_id)

        if self.year != 0 or self.month != 0:
            query = query.join(DimDate, FactWorkOrder.date_id == DimDate.id)
            if self.year != 0:
                filters.append(DimDate.year == self.year)
            if self.month != 0:
                filters.append(DimDate.month == self.month)

        if filters:
            query = query.where(and_(*filters))

        result = await db.execute(query)
        total = result.scalar() or 0
        return {"total_work_orders": total}
