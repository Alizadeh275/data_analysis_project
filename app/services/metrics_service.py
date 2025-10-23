from typing import Optional, List, Dict
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import FactWorkOrder, DimLocation, DimDate, DimProjectType, DimStatus


class WorkOrderMetrics:
    def __init__(
        self,
        location_id: int = 0,
        project_type_id: int = 0,
        status_id: int = 0,
        year: int = 0,
        month: int = 0,
    ):
        self.location_id = location_id
        self.project_type_id = project_type_id
        self.status_id = status_id
        self.year = year
        self.month = month

    async def aggregate(self, db: AsyncSession) -> Dict:
        """
        Returns chart_data and total_count.
        Aggregates over all dimensions that are not filtered.
        """
        filters = []
        group_cols = []

        query = select(FactWorkOrder.count)

        # Filters
        if self.location_id != 0:
            filters.append(FactWorkOrder.location_id == self.location_id)
        else:
            group_cols.append(DimLocation.city_name)
            query = query.join(DimLocation, FactWorkOrder.location_id == DimLocation.id)

        if self.project_type_id != 0:
            filters.append(FactWorkOrder.project_type_id == self.project_type_id)
        else:
            group_cols.append(DimProjectType.name)
            query = query.join(DimProjectType, FactWorkOrder.project_type_id == DimProjectType.id)

        if self.status_id != 0:
            filters.append(FactWorkOrder.status_id == self.status_id)
        else:
            group_cols.append(DimStatus.name)
            query = query.join(DimStatus, FactWorkOrder.status_id == DimStatus.id)

        # Date
        if self.year != 0 or self.month != 0:
            query = query.join(DimDate, FactWorkOrder.date_id == DimDate.id)
            if self.year != 0:
                filters.append(DimDate.year == self.year)
            else:
                group_cols.append(DimDate.year)
            if self.month != 0:
                filters.append(DimDate.month == self.month)
            else:
                group_cols.append(DimDate.month)
        else:
            query = query.join(DimDate, FactWorkOrder.date_id == DimDate.id)
            group_cols.append(DimDate.year)
            group_cols.append(DimDate.month)

        # Apply filters
        if filters:
            query = query.where(and_(*filters))

        # Add grouping and aggregation
        # Add grouping and aggregation
        if group_cols:
            query = query.with_only_columns(*group_cols, func.sum(FactWorkOrder.count).label("count"))
            query = query.group_by(*group_cols)
        else:
            query = query.with_only_columns(func.sum(FactWorkOrder.count).label("count"))

        result = await db.execute(query)
        rows = result.all()

        chart_data = []
        for row in rows:
            if group_cols:
                item = {}
                for idx, col in enumerate(group_cols):
                    # Assign human-readable keys
                    if col == DimLocation.city_name:
                        item["city_name"] = row[idx]
                    elif col == DimProjectType.name:
                        item["project_type_name"] = row[idx]
                    elif col == DimStatus.name:
                        item["status_name"] = row[idx]
                    elif col == DimDate.year:
                        item["year"] = row[idx]
                    elif col == DimDate.month:
                        item["month"] = row[idx]
                item["count"] = row[-1]
                chart_data.append(item)
            else:
                chart_data.append({"count": row[0]})

        total_count = sum(item["count"] for item in chart_data)

        return {"total_count": total_count, "chart_data": chart_data}
