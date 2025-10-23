from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete
from app.models import DimLocation, DimDate, DimProjectType, DimStatus, FactWorkOrder


class WorkOrderCleaner:
    """
    Class to clear all work order fact and dimension tables.
    """

    FACT_TABLES = [FactWorkOrder]
    DIM_TABLES = [DimLocation, DimDate, DimProjectType, DimStatus]

    def __init__(self, db: AsyncSession):
        self.db = db

    async def clear_facts(self):
        """Delete all records from fact tables."""
        for table in self.FACT_TABLES:
            await self.db.execute(delete(table))
        await self.db.commit()

    async def clear_dimensions(self):
        """Delete all records from dimension tables."""
        for table in self.DIM_TABLES:
            await self.db.execute(delete(table))
        await self.db.commit()

    async def clear_all(self):
        """Clear both fact and dimension tables in correct order."""
        await self.clear_facts()
        await self.clear_dimensions()
