from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete
from app.models import DimLocation, DimDate, DimProjectType, DimStatus, FactWorkOrder

async def clear_workorder_tables(db: AsyncSession):
    await db.execute(delete(FactWorkOrder))
    await db.execute(delete(DimLocation))
    await db.execute(delete(DimDate))
    await db.execute(delete(DimProjectType))
    await db.execute(delete(DimStatus))
    await db.commit()
