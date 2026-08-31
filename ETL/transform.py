import pandas as pd

def transform_products(products_df):
    df = products_df.copy().reset_index(drop=True)

    if 'rating' in df.columns:
        rating_df = pd.json_normalize(df['rating'])
        df['rating_rate'] = rating_df['rate']
        df['rating_count'] = rating_df['count']

    df = df.rename(columns={
        'id': 'product_id',
        'title': 'product_name',
        'price': 'product_price',
        'category': 'product_category',
        'description': 'product_description'
    })
    
    df['product_id'] = df['product_id'].astype(int)
    df['product_price'] = df['product_price'].astype(float)
    
    selected_columns = [
        'product_id',
        'product_name',
        'product_price',
        'product_category',
        'product_description',
        'rating_rate',
        'rating_count'
    ]
    
    available_cols = [col for col in selected_columns if col in df.columns]
    
    return df[available_cols]


def transform_users(users_df):
    df = users_df.copy().reset_index(drop=True)

    if 'name' in df.columns:
        name_df = pd.json_normalize(df['name'])
        df['first_name'] = name_df['firstname'].str.capitalize()
        df['last_name'] = name_df['lastname'].str.capitalize()
        df['full_name'] = df['first_name'] + ' ' + df['last_name']

    if 'address' in df.columns:
        address_df = pd.json_normalize(df['address'])
        df['city'] = address_df['city'].str.capitalize()
        df['street'] = address_df['street'].str.title()

    df = df.rename(columns={
        'id': 'user_id',
        'email': 'user_email',
        'username': 'user_username',
        'phone': 'user_phone'
    })

    df['user_id'] = df['user_id'].astype(int)

    selected_columns = [
        'user_id',
        'full_name',
        'first_name',
        'last_name',
        'user_email',
        'user_username',
        'user_phone',
        'street',
        'city'
    ]

    available_cols = [col for col in selected_columns if col in df.columns]

    return df[available_cols]