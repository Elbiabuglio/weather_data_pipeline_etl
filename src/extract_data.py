import requests
import json
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def extract_data(url:str) -> list:
    """
    Extracts data from the given URL and returns it as a list of dictionaries.

    Args:
        url (str): The URL to extract data from.

    Returns:
        list: A list of dictionaries containing the extracted data.
    """
    response = requests.get(url)
    data = response.json()
    
    if response.status_code != 200:
        logging.error(f"Erro na requisição {url}. Status code: {response.status_code}")
    
        if not isinstance(data, list):
            logging.warning(f"Nenhum dado do tipo lista foi retornado. Tipo retornado: {type(data)}")
        
    
    output_path = 'data/weather_data.json'
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=4)
    
    logging.info(f"Dados extraídos e salvos em {output_path}")
    
    return data

