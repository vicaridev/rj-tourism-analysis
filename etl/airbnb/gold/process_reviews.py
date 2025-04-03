#%%
import os
import pandas as pd
from utils.config import SILVER_DIR


reviews_path = os.path.join(SILVER_DIR, 'reviews_clean.parquet')
df_reviews = pd.read_parquet(reviews_path)
