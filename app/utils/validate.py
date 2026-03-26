import pandas as pd
from .logger import get_logger, log_execution


def validate_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates and returns given dataframe.

    Assumes the data is formatted like the dummy_sales_batch files.
    """

    # quantity column often has "Ten" string instead of 10 int
    if "Quantity" in df.columns:
        df.loc[df["Quantity"] == "Ten", "Quantity"] = "10"
        df['Quantity'] = df['Quantity'].astype(int)

    # columns with some known nulls: UnitPrice, DiscountPercent, TaxAmount, ShippingCost, TotalAmount
    df.dropna(axis=0, inplace=True)

    # convert pandas string dtype to object
    str_cols = [c for c in df.columns if pd.api.types.is_string_dtype(df[c])]
    for col in str_cols:
        df[col] = df[col].astype("object")

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d", errors="coerce")
        # Remove rows with invalid dates so derived year/month are always finite.
        df.dropna(subset=["Date"], inplace=True)
        df["year"] = df["Date"].dt.year
        df["month"] = df["Date"].dt.month
    else:
        df["year"] = 2026
        df["month"] = 3

    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["month"] = pd.to_numeric(df["month"], errors="coerce").astype("Int64")
    df.dropna(subset=["year", "month"], inplace=True)
    df["year"] = df["year"].astype("int64")
    df["month"] = df["month"].astype("int64")
    # df["year"] = pd.DatetimeIndex(df["Date"]).year
    # df["month"] = pd.DatetimeIndex(df["Date"]).month

    # logger.info("Dataframe successfully validated")
    return df
