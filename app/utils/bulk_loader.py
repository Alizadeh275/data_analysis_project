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
def prepare_fact_records(df_long: pd.DataFrame, loaders: List[DimensionLoader]):
    fact_records = []
    # get mapping dictionaries
    loc_map = next((l.map for l in loaders if isinstance(l, LocationLoader)), {})
    date_map = next((l.map for l in loaders if isinstance(l, DateLoader)), {})
    proj_map = next((l.map for l in loaders if isinstance(l, SimpleLoader) and l.column_name=='project_type'), {})
    status_map = next((l.map for l in loaders if isinstance(l, SimpleLoader) and l.column_name=='status'), {})

    for _, row in df_long.iterrows():
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

        fact_records.append({
            "location_id": loc_id,
            "date_id": date_id,
            "project_type_id": proj_id,
            "status_id": status_id,
            "count": count
        })
    return fact_records


async def load_work_orders_bulk_full(df_long: pd.DataFrame, db: AsyncSession):
    # initialize loaders
    loaders = [
        LocationLoader(df_long),
        DateLoader(df_long),
        SimpleLoader(df_long, 'project_type', DimProjectType),
        SimpleLoader(df_long, 'status', DimStatus)
    ]

    # prepare and insert all dimensions
    for loader in loaders:
        loader.prepare()
        await loader.insert(db)

    # prepare and insert fact records
    fact_records = prepare_fact_records(df_long, loaders)
    if fact_records:
        await db.execute(FactWorkOrder.__table__.insert(), fact_records)
        await db.commit()
