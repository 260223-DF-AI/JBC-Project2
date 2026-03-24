import pandas as pd



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

    return df
