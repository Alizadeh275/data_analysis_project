from typing import List, Dict, Any
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import DimLocation, DimDate, DimProjectType, DimStatus, FactWorkOrder


class DimensionLoader:
    """Abstract class for all dimensions"""
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.records = []
        self.map = {}

    def prepare(self):
        """Fill self.records with dicts to insert, and self.map keys initialized"""
        raise NotImplementedError

    async def insert(self, db: AsyncSession):
        """Insert self.records into DB and update self.map"""
        raise NotImplementedError


class LocationLoader(DimensionLoader):
    def prepare(self):
        loc_unique = self.df[['city_name', 'department_code']].drop_duplicates()
        for _, row in loc_unique.iterrows():
            city_name = str(row['city_name']).strip() if not pd.isna(row['city_name']) else None
            try:
                dept_code = str(int(row['department_code'])) if not pd.isna(row['department_code']) else None
            except ValueError:
                dept_code = None
            self.map[(city_name, dept_code)] = None
            self.records.append({"city_name": city_name, "department_code": dept_code})

    async def insert(self, db: AsyncSession):
        if self.records:
            result = await db.execute(
                DimLocation.__table__.insert()
                .returning(DimLocation.id, DimLocation.city_name, DimLocation.department_code),
                self.records
            )
            for row in result.fetchall():
                self.map[(row.city_name, row.department_code)] = row.id


class DateLoader(DimensionLoader):
    def prepare(self):
        date_unique = self.df[['year', 'month']].drop_duplicates()
        for _, row in date_unique.iterrows():
            try:
                year = int(row['year']) if not pd.isna(row['year']) else None
            except ValueError:
                year = None
            try:
                month = int(row['month']) if not pd.isna(row['month']) else None
            except ValueError:
                month = None
            self.map[(year, month)] = None
            self.records.append({"year": year, "month": month})

    async def insert(self, db: AsyncSession):
        if self.records:
            result = await db.execute(
                DimDate.__table__.insert()
                .returning(DimDate.id, DimDate.year, DimDate.month),
                self.records
            )
            for row in result.fetchall():
                self.map[(row.year, row.month)] = row.id


class SimpleLoader(DimensionLoader):
    """Loader for single-column dimensions like project_type or status"""
    def __init__(self, df: pd.DataFrame, column_name: str, model):
        super().__init__(df)
        self.column_name = column_name
        self.model = model

    def prepare(self):
        unique_values = self.df[self.column_name].dropna().drop_duplicates()
        for val in unique_values:
            val = str(val).strip()
            if val:
                self.map[val] = None
                self.records.append({"name": val})

    async def insert(self, db: AsyncSession):
        if self.records:
            result = await db.execute(
                self.model.__table__.insert().returning(self.model.id, self.model.name),
                self.records
            )
            for row in result.fetchall():
                self.map[row.name] = row.id


# ================================
# Fact Loader
# ================================

class FactLoader:
    def __init__(self, df_long: pd.DataFrame, dimension_maps: Dict[str, Dict]):
        """
        df_long: the transformed long-format DataFrame
        dimension_maps: dictionary with keys 'location', 'date', 'project_type', 'status'
                        each value is a mapping dict {dimension_value: id}
        """
        self.df_long = df_long
        self.dimension_maps = dimension_maps
        self.fact_records = []

    def prepare(self):
        """Map dimension IDs and prepare fact records."""
        loc_map = self.dimension_maps['location']
        date_map = self.dimension_maps['date']
        proj_map = self.dimension_maps['project_type']
        status_map = self.dimension_maps['status']

        for _, row in self.df_long.iterrows():
            city_name = str(row['city_name']).strip() if not pd.isna(row['city_name']) else None
            try:
                dept_code = str(int(row['department_code'])) if not pd.isna(row['department_code']) else None
            except ValueError:
                dept_code = None
            loc_id = loc_map.get((city_name, dept_code))
            
            try:
                date_id = date_map.get((int(row['year']), int(row['month'])))
            except ValueError:
                date_id = None

            proj_id = proj_map.get(str(row['project_type']).strip())
            status_id = status_map.get(str(row['status']).strip())
            try:
                count = int(row['count']) if not pd.isna(row['count']) else 0
            except ValueError:
                count = 0

            self.fact_records.append({
                "location_id": loc_id,
                "date_id": date_id,
                "project_type_id": proj_id,
                "status_id": status_id,
                "count": count
            })

    async def insert(self, db: AsyncSession):
        """Bulk insert fact records."""
        if self.fact_records:
            await db.execute(FactWorkOrder.__table__.insert(), self.fact_records)
            await db.commit()


async def load_work_orders_bulk_full(df_long: pd.DataFrame, db: AsyncSession):
    # ----------------------
    # 1. Load Dimensions
    # ----------------------
    loaders = [
        LocationLoader(df_long),
        DateLoader(df_long),
        SimpleLoader(df_long, 'project_type', DimProjectType),
        SimpleLoader(df_long, 'status', DimStatus)
    ]

    for loader in loaders:
        loader.prepare()
        await loader.insert(db)

    # ----------------------
    # 2. Load Facts
    # ----------------------
    dimension_maps = {
        'location': next(l.map for l in loaders if isinstance(l, LocationLoader)),
        'date': next(l.map for l in loaders if isinstance(l, DateLoader)),
        'project_type': next(l.map for l in loaders if isinstance(l, SimpleLoader) and l.column_name=='project_type'),
        'status': next(l.map for l in loaders if isinstance(l, SimpleLoader) and l.column_name=='status')
    }

    fact_loader = FactLoader(df_long, dimension_maps)
    fact_loader.prepare()
    await fact_loader.insert(db)
