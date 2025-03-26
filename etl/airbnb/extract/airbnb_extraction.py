
import os
import gzip
import pandas as pd
import requests



files = {
    'calendar': 'https://data.insideairbnb.com/brazil/rj/rio-de-janeiro/2024-12-27/data/calendar.csv.gz',
    'listings': 'https://data.insideairbnb.com/brazil/rj/rio-de-janeiro/2024-12-27/data/listings.csv.gz',
    'reviews': 'https://data.insideairbnb.com/brazil/rj/rio-de-janeiro/2024-12-27/data/reviews.csv.gz'
}

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "airbnb", "bronze")



def extract_airbnb_data():
    os.makedirs(BRONZE_DIR, exist_ok=True)
    print(BASE_DIR)
    print(BRONZE_DIR)
    for filename, url in files.items():
        print(f'Iniciando extração do arquivo {filename}')
        
        gzb_path = os.path.join(BRONZE_DIR, f'{filename}.csv.gz')
        csv_path = os.path.join(BRONZE_DIR, f'{filename}.csv')

        print(f'Baixando arquivo {filename}... {url}')
        response = requests.get(url, stream=True)
        
        if response.status_code == 200:
            print(f'{filename} baixado com sucesso!')
            try:
                with open(gzb_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            file.write(chunk)
                print(f'{filename}.csv.gz salvo com sucesso!')
            except Exception as e:
                print(f'Erro ao salvar o arquivo {filename}: {e}')
                continue
        else:
            print(f'Falha ao baixar {filename}, status code: {response.status_code}')
            continue  
        
        print(f'Convertendo {filename} para dataframe...')
        try:
            with gzip.open(gzb_path, 'rt', encoding='utf-8') as file:
                df = pd.read_csv(file)
            
            print(f'Salvando {filename} como CSV...')
            df.to_csv(csv_path, index=False)

            os.remove(gzb_path)
            print(f'{filename} extraído e transformado com sucesso\n')
        except Exception as e:
            print(f'Erro ao processar o arquivo {filename}: {e}')

    print('Extração do Airbnb concluída!')    
    
extract_airbnb_data()
