import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import DimLocation, DimDate, DimProjectType, DimStatus, FactWorkOrder

async def load_work_orders_bulk_full(df_long: pd.DataFrame, db: AsyncSession):
    # ================================
    # 1. Prepare and deduplicate dimensions
    # ================================

    # DimLocation
    loc_unique = df_long[['city_name', 'department_code']].drop_duplicates()
    locations_map = {}
    loc_records = []
    for _, row in loc_unique.iterrows():
        city_name = str(row['city_name']).strip() if not pd.isna(row['city_name']) else None
        dept_code = (
            str(int(row['department_code']))
            if not pd.isna(row['department_code'])
            else None
        )
        locations_map[(city_name, dept_code)] = None
        loc_records.append({"city_name": city_name, "department_code": dept_code})

    # DimDate
    date_unique = df_long[['year', 'month']].drop_duplicates()
    dates_map = {}
    date_records = []
    for _, row in date_unique.iterrows():
        year = int(row['year']) if not pd.isna(row['year']) else None
        month = int(row['month']) if not pd.isna(row['month']) else None
        dates_map[(year, month)] = None
        date_records.append({"year": year, "month": month})

    # DimProjectType
    proj_unique = df_long['project_type'].drop_duplicates()
    proj_map = {}
    proj_records = []
    for name in proj_unique:
        name = str(name).strip()
        if name:
            proj_map[name] = None
            proj_records.append({"name": name})

    # DimStatus
    status_unique = df_long['status'].dropna().unique()
    status_map = {}
    status_records = []
    for name in status_unique:
        name = str(name).strip()
        if name:
            status_map[name] = None
            status_records.append({"name": name})

    # ================================
    # 2. Bulk insert dimensions
    # ================================
    # location
    if loc_records:
        result = await db.execute(
            DimLocation.__table__.insert()
            .returning(DimLocation.id, DimLocation.city_name, DimLocation.department_code),
            loc_records
        )
        for row in result.fetchall():
            locations_map[(row.city_name, row.department_code)] = row.id

    # date
    if date_records:
        result = await db.execute(
            DimDate.__table__.insert()
            .returning(DimDate.id, DimDate.year, DimDate.month),
            date_records
        )
        for row in result.fetchall():
            dates_map[(row.year, row.month)] = row.id

    # project
    if proj_records:
        result = await db.execute(
            DimProjectType.__table__.insert()
            .returning(DimProjectType.id, DimProjectType.name),
            proj_records
        )
        for row in result.fetchall():
            proj_map[row.name] = row.id

    # status
    if status_records:
        result = await db.execute(
            DimStatus.__table__.insert()
            .returning(DimStatus.id, DimStatus.name),
            status_records
        )
        for row in result.fetchall():
            status_map[row.name] = row.id

    await db.commit()  # commit dimensions before fact insert

    # ================================
    # 3. Prepare fact records
    # ================================
    fact_records = []
    for _, row in df_long.iterrows():
        city_name = str(row['city_name']).strip() if not pd.isna(row['city_name']) else None
        dept_code = (
            str(int(row['department_code']))
            if not pd.isna(row['department_code'])
            else None
        )
        loc_id = locations_map.get((city_name, dept_code))
        date_id = dates_map.get((int(row['year']), int(row['month'])))
        proj_id = proj_map.get(str(row['project_type']).strip())
        status_id = status_map.get(str(row['status']).strip())
        count = int(row['count']) if not pd.isna(row['count']) else 0

        fact_records.append({
            "location_id": loc_id,
            "date_id": date_id,
            "project_type_id": proj_id,
            "status_id": status_id,
            "count": count
        })

    # ================================
    # 4. Bulk insert facts
    # ================================
    if fact_records:
        await db.execute(FactWorkOrder.__table__.insert(), fact_records)
        await db.commit()
