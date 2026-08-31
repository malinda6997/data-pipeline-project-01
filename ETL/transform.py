import pandas as pd

def transform_products(products_df):
    df = products_df.copy()
    df= df.rename(columns={
        'id': 'product_id',
        'title': 'product_name',
        'price': 'product_price',
        'category': 'product_category',
        'description': 'product_description'
    })
    
    df = df[[
        'product_id',
        'product_name',
        'product_price',
        'product_category',
        'product_description'
    ]]
    
    df[['product_price']] = df[['product_price']].astype(float)
    return df

