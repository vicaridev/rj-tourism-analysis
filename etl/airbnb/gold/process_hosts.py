#%%
import os
from utils.config import SILVER_DIR, GOLD_DIR
import pandas as pd


hosts_path = os.path.join(SILVER_DIR, 'hosts_clean.csv')
df = pd.read_csv(hosts_path)

#%%
df.info()
    