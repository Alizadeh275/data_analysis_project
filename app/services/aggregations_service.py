from typing import List, Dict, Optional
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import FactWorkOrder, DimLocation, DimDate, DimProjectType, DimStatus


class WorkOrderMetrics:
    def __init__(
        self,
        location_id: Optional[int] = None,
        project_type_id: Optional[int] = None,
        status_id: Optional[int] = None,
        year: Optional[int] = None,
        month: Optional[int] = None,
        group_by: List[str] = None,
    ):
        self.location_id = location_id
        self.project_type_id = project_type_id
        self.status_id = status_id
        self.year = year
        self.month = month
        self.group_by = group_by or []

    async def aggregate(self, db: AsyncSession) -> Dict:
        filters = []
        joins = set()
        group_cols = []

        query = select(FactWorkOrder.count)

        # --- Filters ---
        if self.location_id is not None:
            filters.append(FactWorkOrder.location_id == self.location_id)
        if self.project_type_id is not None:
            filters.append(FactWorkOrder.project_type_id == self.project_type_id)
        if self.status_id is not None:
            filters.append(FactWorkOrder.status_id == self.status_id)
        if self.year is not None or self.month is not None:
            joins.add("date")
            if self.year is not None:
                filters.append(DimDate.year == self.year)
            if self.month is not None:
                filters.append(DimDate.month == self.month)

        # --- Grouping ---
        if "location" in self.group_by:
            joins.add("location")
            group_cols.append(DimLocation.city_name)
        if "project_type" in self.group_by:
            joins.add("project_type")
            group_cols.append(DimProjectType.name)
        if "status" in self.group_by:
            joins.add("status")
            group_cols.append(DimStatus.name)
        if "year" in self.group_by or "month" in self.group_by:
            joins.add("date")
            if "year" in self.group_by:
                group_cols.append(DimDate.year)
            if "month" in self.group_by:
                group_cols.append(DimDate.month)

        # --- Joins ---
        if "location" in joins:
            query = query.join(DimLocation, FactWorkOrder.location_id == DimLocation.id)
        if "project_type" in joins:
            query = query.join(DimProjectType, FactWorkOrder.project_type_id == DimProjectType.id)
        if "status" in joins:
            query = query.join(DimStatus, FactWorkOrder.status_id == DimStatus.id)
        if "date" in joins:
            query = query.join(DimDate, FactWorkOrder.date_id == DimDate.id)

        # --- Filters ---
        if filters:
            query = query.where(and_(*filters))

        # --- Aggregation ---
        if group_cols:
            query = query.with_only_columns(*group_cols, func.sum(FactWorkOrder.count).label("count"))
            query = query.group_by(*group_cols)
        else:
            query = query.with_only_columns(func.sum(FactWorkOrder.count).label("count"))

        # --- Execute ---
        result = await db.execute(query)
        rows = result.all()

        # --- Format Result ---
        chart_data = []
        for row in rows:
            if group_cols:
                item = {}
                for idx, col in enumerate(group_cols):
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
