import pandas as pd
import re
from app.core.constants import RENAME_MAP

class ExcelTransformer:
    """
    Class to handle cleaning and transforming wide-format Excel files into long-format DataFrame.
    """

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.df_wide = None
        self.df_long = None

    def read_excel(self):
        """Read the Excel file with multi-level headers."""
        self.df_wide = pd.read_excel(self.file_path, header=[1, 2], engine="openpyxl")
        return self

    def remove_summary_columns(self):
        """Remove columns that are summaries (contain جمع/مجموع)."""
        mask = ~(
            self.df_wide.columns.get_level_values(0).str.contains("جمع|مجموع", na=False)
            | self.df_wide.columns.get_level_values(1).str.contains("جمع|مجموع", na=False)
        )
        self.df_wide = self.df_wide.loc[:, mask]

        # Drop last column if it looks like a summary
        if any(str(x).startswith("مجموع") for x in self.df_wide.columns[-1]):
            self.df_wide = self.df_wide.iloc[:, :-1]
        return self

    def remove_total_rows(self):
        """Remove total/NaN rows like 'کل شرکت' or rows starting with جمع/مجموع."""
        self.df_wide = self.df_wide[
            ~self.df_wide.iloc[:, 0].isna()
            & (~self.df_wide.iloc[:, 0].astype(str).str.startswith(("جمع", "مجموع")))
            & (self.df_wide.iloc[:, 0] != "کل شرکت")
        ]
        return self

    def normalize_headers(self):
        """Normalize and flatten multi-level headers."""
        new_lvl0 = []
        for lvl0, lvl1 in self.df_wide.columns:
            if "تست" in str(lvl0):
                match = re.search(r"(تست\s*\d+)", str(lvl0))
                new_lvl0.append(match.group(1) if match else lvl0)
            else:
                new_lvl0.append(lvl0)

        self.df_wide.columns = pd.MultiIndex.from_tuples(
            [(lvl0, lvl1) for lvl0, lvl1 in zip(new_lvl0, self.df_wide.columns.get_level_values(1))]
        )

        # Flatten headers
        self.df_wide.columns = [
            f"{lvl0} - {lvl1}" if lvl1 else lvl0
            for lvl0, lvl1 in self.df_wide.columns
        ]
        self.df_wide.columns = [re.sub(r" - Unnamed: \d+_level_\d+", "", c) for c in self.df_wide.columns]
        self.df_wide.columns = [re.sub(r"\.\d+$", "", c) for c in self.df_wide.columns]
        return self

    def melt_to_long(self):
        """Melt wide DataFrame into long format and split columns."""
        id_vars, value_vars = self.df_wide.columns[:4].tolist(), self.df_wide.columns[4:].tolist()
        df_long = self.df_wide.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name="تست_و_وضعیت",
            value_name="تعداد"
        )

        # Split combined column
        split_cols = df_long["تست_و_وضعیت"].str.split(" - ", n=1, expand=True)
        split_cols.columns = ["تست", "وضعیت"]
        split_cols["وضعیت"] = split_cols["وضعیت"].fillna("").str.replace(r"\.\d+$", "", regex=True).str.strip()

        self.df_long = pd.concat([df_long.drop(columns=["تست_و_وضعیت"]), split_cols], axis=1)
        self.df_long = self.df_long.rename(columns=RENAME_MAP)
        return self

    def transform(self) -> pd.DataFrame:
        """Run full transformation pipeline."""
        return (
            self.read_excel()
                .remove_summary_columns()
                .remove_total_rows()
                .normalize_headers()
                .melt_to_long()
                .df_long
        )
