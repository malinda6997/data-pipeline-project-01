import pandas as pd
import requests
import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("API_URL")

def extract_products():
    
    url = f'{BASE_URL}/products'
    response = requests.get(url)
    response.raise_for_status()
    
    data = response.json()
    products_df = pd.DataFrame(data)
    return products_df

def extract_users():
    
    url = f'{BASE_URL}/users'
    response = requests.get(url)
    response.raise_for_status()
    
    data = response.json()
    users_df = pd.DataFrame(data)
    return users_df
    
    
    


