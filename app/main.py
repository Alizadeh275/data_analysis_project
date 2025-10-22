from fastapi import FastAPI, HTTPException, Depends
from contextlib import asynccontextmanager
import pandas as pd
import re
import os
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete

from app.database import Base, engine, get_async_session
from app.models import DimLocation, DimDate, DimProjectType, DimStatus, FactWorkOrder
from app.utils import load_work_orders_bulk_full


# ================================
# Lifespan for startup/shutdown
# ================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


app = FastAPI(title="Work Orders ETL", lifespan=lifespan)


# ================================
# Config paths
# ================================
DATA_FILE_PATH = "app/static/2016.xlsx"
LONG_EXCEL_PATH = "app/static/data_long.xlsx"
LONG_CSV_PATH = "app/static/data_long.csv"

rename_map = {
    "مدیریت برق شهرستان": "city_name",
    "کد امور": "department_code",
    "سال": "year",
    "ماه": "month",
    "تعداد": "count",
    "تست": "project_type",
    "وضعیت": "status"
}
    

# ================================
# Utility function for cleaning Excel
# ================================
def clean_and_transform_excel(file_path: str) -> pd.DataFrame:
    # --- 1. Read Excel with MultiIndex header ---
    df = pd.read_excel(file_path, header=[1, 2], engine="openpyxl")

    # --- 2. Remove unwanted summary columns in both header levels ---
    mask = ~(
        df.columns.get_level_values(0).str.contains("جمع|مجموع", na=False)
        | df.columns.get_level_values(1).str.contains("جمع|مجموع", na=False)
    )
    df = df.loc[:, mask]

    # --- 3. Remove last summary column if starts with "مجموع" ---
    if df.columns[-1][0].startswith("مجموع") or df.columns[-1][1].startswith("مجموع"):
        df = df.iloc[:, :-1]

    # --- 4. Remove extra rows (totals or NaNs) ---
    df = df[
        ~df.iloc[:, 0].isna()
        & (~df.iloc[:, 0].astype(str).str.startswith("جمع"))
        & (~df.iloc[:, 0].astype(str).str.startswith("مجموع"))
        & (df.iloc[:, 0] != "کل شرکت")
    ]

    # --- 5. Clean level 0 headers ---
    new_lvl0 = []
    for lvl0, lvl1 in df.columns:
        if "تست" in str(lvl0):
            match = re.search(r"(تست\s*\d+)", str(lvl0))
            if match:
                new_lvl0.append(match.group(1))
            else:
                new_lvl0.append(lvl0)
        else:
            new_lvl0.append(lvl0)
    df.columns = pd.MultiIndex.from_tuples(
        [(lvl0, lvl1) for lvl0, lvl1 in zip(new_lvl0, df.columns.get_level_values(1))]
    )

    # --- 6. Flatten MultiIndex to single-level columns ---
    df.columns = [
        f"{lvl0} - {lvl1}" if lvl1 not in ("", None) else lvl0
        for lvl0, lvl1 in df.columns
    ]
    df.columns = [re.sub(r" - Unnamed: \d+_level_\d+", "", col) for col in df.columns]

    # --- 7. Remove duplicate suffixes like '.1', '.2' from column names ---
    df.columns = [re.sub(r"\.\d+$", "", col) for col in df.columns]

    # --- 8. Convert wide → long ---
    id_vars = df.columns[:4].tolist()
    value_vars = df.columns[4:].tolist()
    df_long = df.melt(
        id_vars=id_vars, value_vars=value_vars, var_name="تست_و_وضعیت", value_name="تعداد"
    )

    # --- 9. Split “تست - وضعیت” column ---
    split_cols = df_long["تست_و_وضعیت"].str.split(" - ", n=1, expand=True)
    split_cols.columns = ["تست", "وضعیت"]
    split_cols["وضعیت"] = split_cols["وضعیت"].fillna("")

    # --- 10. Clean status suffixes like '.1', '.2' ---
    split_cols["وضعیت"] = split_cols["وضعیت"].str.replace(r"\.\d+$", "", regex=True).str.strip()

    df_long = df_long.drop(columns=["تست_و_وضعیت"])
    df_long = pd.concat([df_long, split_cols], axis=1)

    # --- 11. Rename columns ---
    df_long = df_long.rename(columns=rename_map)

    return df_long

# ================================
# Main API endpoint
# ================================
@app.post("/refresh-workorders/")
async def refresh_workorders(db: AsyncSession = Depends(get_async_session)):
    # --- 1. Clear all tables ---
    try:
        await db.execute(delete(FactWorkOrder))
        await db.execute(delete(DimLocation))
        await db.execute(delete(DimDate))
        await db.execute(delete(DimProjectType))
        await db.execute(delete(DimStatus))
        await db.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error clearing tables: {e}")

    # --- 2. Transform Excel (clean → long format) ---
    try:
        df_long = clean_and_transform_excel(DATA_FILE_PATH)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error transforming Excel: {e}")

    # --- 3. Save results ---
    os.makedirs(os.path.dirname(LONG_EXCEL_PATH), exist_ok=True)
    df_long.to_excel(LONG_EXCEL_PATH, index=False)
    df_long.to_csv(LONG_CSV_PATH, index=False)

    # --- 4. Bulk insert into DB ---
    try:
        await load_work_orders_bulk_full(df_long, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading data: {e}")

    return {
        "status": "success",
        "message": "Work orders refreshed successfully."
    }
