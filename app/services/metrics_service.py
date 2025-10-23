from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import FactWorkOrder, DimDate

class MetricsService:
    @staticmethod
    async def get_total_workorders_sum(
        db: AsyncSession,
        project_type_id: int,
        status_id: int,
        year: int,
        month: int
    ):
        query = (
            select(func.sum(FactWorkOrder.count).label("total_sum"))
            .join(DimDate, FactWorkOrder.date_id == DimDate.id)
            .where(
                FactWorkOrder.project_type_id == project_type_id,
                FactWorkOrder.status_id == status_id,
                DimDate.year == year,
                DimDate.month == month
            )
        )

        result = await db.execute(query)
        total_sum = result.scalar() or 0
        return {"total_sum": total_sum}
