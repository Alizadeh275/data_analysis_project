import pandas as pd
import re
from app.core.constants import RENAME_MAP

def clean_and_transform_excel(file_path: str) -> pd.DataFrame:
    df = pd.read_excel(file_path, header=[1, 2], engine="openpyxl")

    # Remove summary columns (both header levels)
    mask = ~(
        df.columns.get_level_values(0).str.contains("جمع|مجموع", na=False)
        | df.columns.get_level_values(1).str.contains("جمع|مجموع", na=False)
    )
    df = df.loc[:, mask]

    # Drop last column if it’s a summary
    if any(str(x).startswith("مجموع") for x in df.columns[-1]):
        df = df.iloc[:, :-1]

    # Remove total/NaN rows
    df = df[
        ~df.iloc[:, 0].isna()
        & (~df.iloc[:, 0].astype(str).str.startswith(("جمع", "مجموع")))
        & (df.iloc[:, 0] != "کل شرکت")
    ]

    # Normalize headers
    new_lvl0 = []
    for lvl0, lvl1 in df.columns:
        if "تست" in str(lvl0):
            match = re.search(r"(تست\s*\d+)", str(lvl0))
            new_lvl0.append(match.group(1) if match else lvl0)
        else:
            new_lvl0.append(lvl0)
    df.columns = pd.MultiIndex.from_tuples([(lvl0, lvl1) for lvl0, lvl1 in zip(new_lvl0, df.columns.get_level_values(1))])

    # Flatten headers
    df.columns = [
        f"{lvl0} - {lvl1}" if lvl1 else lvl0
        for lvl0, lvl1 in df.columns
    ]
    df.columns = [re.sub(r" - Unnamed: \d+_level_\d+", "", c) for c in df.columns]
    df.columns = [re.sub(r"\.\d+$", "", c) for c in df.columns]

    # Melt to long
    id_vars, value_vars = df.columns[:4].tolist(), df.columns[4:].tolist()
    df_long = df.melt(id_vars=id_vars, value_vars=value_vars, var_name="تست_و_وضعیت", value_name="تعداد")

    # Split column
    split_cols = df_long["تست_و_وضعیت"].str.split(" - ", n=1, expand=True)
    split_cols.columns = ["تست", "وضعیت"]
    split_cols["وضعیت"] = split_cols["وضعیت"].fillna("").str.replace(r"\.\d+$", "", regex=True).str.strip()

    df_long = pd.concat([df_long.drop(columns=["تست_و_وضعیت"]), split_cols], axis=1)
    df_long = df_long.rename(columns=RENAME_MAP)

    return df_long
