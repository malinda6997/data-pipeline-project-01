import pandas as pd


def check_products_quality(df: pd.DataFrame) -> bool:
    """Performs data quality validations on the products DataFrame."""
    print("Running Data Quality Checks for Products...")

    # 1. Check if DataFrame is empty
    if df.empty:
        raise ValueError("Data Quality Error: Products DataFrame is completely empty!")

    # 2. Primary Key null validation
    if df['product_id'].isnull().any():
        raise ValueError("Data Quality Error: Null values found in 'product_id'!")

    # 3. Primary Key uniqueness validation
    if df['product_id'].duplicated().any():
        raise ValueError("Data Quality Error: Duplicate 'product_id' values found!")

    # 4. Valid price range validation (price must be greater than 0)
    price_col = 'price' if 'price' in df.columns else 'product_price'
    if price_col in df.columns and (df[price_col] <= 0).any():
        raise ValueError("Data Quality Error: Invalid product prices (<= 0) found!")

    print("Products Data Quality Checks Passed!")
    return True


def check_users_quality(df: pd.DataFrame) -> bool:
    """Performs data quality validations on the users DataFrame."""
    print("Running Data Quality Checks for Users...")

    # 1. Check if DataFrame is empty
    if df.empty:
        raise ValueError("Data Quality Error: Users DataFrame is completely empty!")

    # 2. Primary Key null validation
    if df['user_id'].isnull().any():
        raise ValueError("Data Quality Error: Null values found in 'user_id'!")

    # 3. Primary Key uniqueness validation
    if df['user_id'].duplicated().any():
        raise ValueError("Data Quality Error: Duplicate 'user_id' values found!")

    # 4. Essential field validation (email should not be null)
    email_col = 'email' if 'email' in df.columns else 'user_email'
    if email_col in df.columns and df[email_col].isnull().any():
        raise ValueError(f"Data Quality Error: Null values found in '{email_col}'!")

    print("Users Data Quality Checks Passed!")
    return True