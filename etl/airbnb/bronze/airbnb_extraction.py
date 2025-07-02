#%%
import os
import gzip
import pandas as pd
import requests
from utils.config import BRONZE_DIR
from utils.logger import logging


files = {
    'calendar': 'https://data.insideairbnb.com/brazil/rj/rio-de-janeiro/2024-12-27/data/calendar.csv.gz',
    'listings': 'https://data.insideairbnb.com/brazil/rj/rio-de-janeiro/2024-12-27/data/listings.csv.gz',
    'reviews': 'https://data.insideairbnb.com/brazil/rj/rio-de-janeiro/2024-12-27/data/reviews.csv.gz'
}




def extract_airbnb_data():
    
    for filename, url in files.items():
        logging.info(f'Iniciando extração do arquivo {filename}')
        
        gzb_path = os.path.join(BRONZE_DIR, f'{filename}.csv.gz')
        file_path = os.path.join(BRONZE_DIR, f'{filename}.parquet')

        logging.info(f'Baixando arquivo {filename}... {url}')
        response = requests.get(url, stream=True)
        
        if response.status_code == 200:
            logging.info(f'{filename} baixado com sucesso!')
            try:
                with open(gzb_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            file.write(chunk)
                logging.info(f'{filename}.csv.gz salvo com sucesso!')
            except Exception as e:
                logging.info(f'Erro ao salvar o arquivo {filename}: {e}')
                continue
        else:
            logging.info(f'Falha ao baixar {filename}, status code: {response.status_code}')
            continue  
        
        logging.info(f'Convertendo {filename} para dataframe...')
        try:
            with gzip.open(gzb_path, 'rt', encoding='utf-8') as file:
                df = pd.read_csv(file)
            
            logging.info(f'Salvando {filename} como PARQUET...')
            df.to_parquet(file_path, index=False)

            os.remove(gzb_path)
            logging.info(f'{filename} extraído e transformado com sucesso\n')
        except Exception as e:
            logging.info(f'Erro ao processar o arquivo {filename}: {e}')

    logging.info('Extração do Airbnb concluída!')    
    
    pass