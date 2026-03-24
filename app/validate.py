import pandas as pd
import logging

logger = logging.getLogger(__name__)


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

    logger.info("Dataframe successfully validated")
    return df
