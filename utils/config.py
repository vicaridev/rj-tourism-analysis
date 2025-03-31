import os


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "airbnb", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "airbnb", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "airbnb", "gold")

os.makedirs(BRONZE_DIR, exist_ok=True)
os.makedirs(SILVER_DIR, exist_ok=True)
os.makedirs(GOLD_DIR, exist_ok=True)