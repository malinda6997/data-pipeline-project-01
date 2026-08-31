import pandas as pd
import requests
import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("API_URL")

def extract_products():
    if not BASE_URL:
        raise ValueError("Error: API_URL not found in .env file!")

    url = f'{BASE_URL}/products'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except requests.exceptions.RequestException as e:
        print(f"Error extracting products: {e}")
        return pd.DataFrame()


def extract_users():
    if not BASE_URL:
        raise ValueError("Error: API_URL not found in .env file!")

    url = f'{BASE_URL}/users'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except requests.exceptions.RequestException as e:
        print(f"Error extracting users: {e}")
        return pd.DataFrame()
