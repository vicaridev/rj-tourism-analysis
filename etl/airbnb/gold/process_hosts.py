#%%
import pandas as pd
import os
from utils.config import SILVER_DIR, GOLD_DIR


hosts_clean_path = os.path.join(SILVER_DIR, 'hosts_clean.parquet')
df = pd.read_parquet(hosts_clean_path)


#%%
df.info()