import os
from app.services.db_cleaner import clear_workorder_tables
from app.services.excel_transformer import clean_and_transform_excel
from app.utils.bulk_loader import load_work_orders_bulk_full
from app.core.constants import WIDE_FILE_PATH, LONG_EXCEL_PATH, LONG_CSV_PATH

async def run_etl(db):
    await clear_workorder_tables(db)

    long_df = clean_and_transform_excel(WIDE_FILE_PATH)

    os.makedirs(os.path.dirname(LONG_EXCEL_PATH), exist_ok=True)
    long_df.to_excel(LONG_EXCEL_PATH, index=False)
    long_df.to_csv(LONG_CSV_PATH, index=False)

    await load_work_orders_bulk_full(long_df, db)

    return {
        "status": "success",
        "message": "Work orders refreshed successfully.",
        "records": len(long_df)
    }
