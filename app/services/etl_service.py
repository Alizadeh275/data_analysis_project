import os
from app.services.db_cleaner import clear_workorder_tables
from app.services.excel_transformer import ExcelTransformer
from app.core.constants import WIDE_FILE_PATH, LONG_EXCEL_PATH, LONG_CSV_PATH
from app.services.load_service import WorkOrderLoader

async def run_etl(db):
    await clear_workorder_tables(db)

    transformer = ExcelTransformer(WIDE_FILE_PATH)
    long_df = transformer.transform()

    os.makedirs(os.path.dirname(LONG_EXCEL_PATH), exist_ok=True)
    long_df.to_excel(LONG_EXCEL_PATH, index=False)
    long_df.to_csv(LONG_CSV_PATH, index=False)


    loader = WorkOrderLoader(long_df)
    await loader.load(db)

    return {
        "status": "success",
        "message": "Work orders refreshed successfully.",
        "records": len(long_df)
    }
