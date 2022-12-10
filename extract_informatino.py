import os
from os.path import join
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
import pandas as pd
from datetime import datetime, timedelta

load_dotenv(find_dotenv())

start_date = datetime.today()
end_date = start_date + timedelta(days=7)

start_date = start_date.strftime('%Y-%m-%d')
end_date = end_date.strftime('%Y-%m-%d')

city = 'Boston'
key = os.getenv('KEY')

URL = join(
  'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
  f'{city}/{start_date}/{end_date}?unitGroup=metric&include=days&key={key}&contentType=csv'
)

data = pd.read_csv(URL)

file_path = join(Path(__file__).parents[0], 'temp', f'semana={start_date}')
os.mkdir(file_path)

data.to_csv(join(file_path, 'dados_brutos.csv'))
data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(join(
  file_path,
  'temperaturas.csv'
))
data[['datetime', 'description', 'icon']].to_csv(join(
  file_path,
  'condicoes.csv'
))