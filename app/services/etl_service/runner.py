import os
import pandas as pd
from app.core.constants import WIDE_FILE_PATH, LONG_EXCEL_PATH, LONG_CSV_PATH
from app.services.db_cleaner import WorkOrderCleaner
from app.services.etl_service.excel_transformer import ExcelTransformer
from app.services.etl_service.loader import WorkOrderLoader
from sqlalchemy.ext.asyncio import AsyncSession


class WorkOrderETLManager:
    """
    Manages the full ETL workflow for work orders:
    1. Clear DB tables
    2. Transform Excel to long DataFrame
    3. Save to Excel/CSV
    4. Load into DB
    """

    def __init__(self, db: AsyncSession, wide_file_path: str = WIDE_FILE_PATH):
        self.db = db
        self.wide_file_path = wide_file_path
        self.cleaner = WorkOrderCleaner(db)

    async def run(self):
        # ----------------------
        # 1. Clear tables
        # ----------------------
        await self.cleaner.clear_all()

        # ----------------------
        # 2. Transform Excel
        # ----------------------
        transformer = ExcelTransformer(self.wide_file_path)
        long_df: pd.DataFrame = transformer.transform()

        # ----------------------
        # 3. Save transformed data
        # ----------------------
        os.makedirs(os.path.dirname(LONG_EXCEL_PATH), exist_ok=True)
        long_df.to_excel(LONG_EXCEL_PATH, index=False)
        long_df.to_csv(LONG_CSV_PATH, index=False)

        # ----------------------
        # 4. Load into DB
        # ----------------------
        loader = WorkOrderLoader(long_df)
        await loader.load(self.db)

        return {
            "status": "success",
            "message": "Work orders refreshed successfully.",
            "records": len(long_df)
        }
