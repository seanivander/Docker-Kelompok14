import requests # Untuk melakukan HTTP Request
from bs4 import BeautifulSoup # Untuk pengolahan data HTML atau XML
import pandas as pd #manipulasi data
import psycopg2 # untuk connect ke database postgresql

# Web Scraping

url = 'https://www.petsecure.com.au/pet-care/a-guide-to-worldwide-pet-ownership/'
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')
table = soup.find('table',{'class':'cats'})
headers = []

for th in table.find_all('th'):
  headers.append(th.text.strip())

rows = []

for tr in table.find_all('tr'): #loop komponen tr
  row_data = []
  for td in tr.find_all('td'):
    row_data.append(td.text.strip())

  if len(row_data) > 0:
    rows.append(row_data)

population_df = pd.DataFrame(rows,columns=headers)
population_df = population_df.rename(columns={population_df.columns[0]:'country',population_df.columns[1]:'populations'})


# API
url = 'https://api.opencagedata.com/geocode/v1/json'
api_key = 'a34768ccd55d49cfa29fb5753e2d1486'
countries = population_df['country'].to_list()
countries_list = []

for country in countries:
  params = {'q': country, 'key': api_key}
  response = requests.get(url,params=params)
  json_data = response.json()
  components = json_data['results'][0]['components']
  geometry = json_data['results'][0]['geometry']

  country_components = {
      'country': country,
      'country_code': components.get('country_code',''),
      'latitude': geometry.get('lat'),
      'longitude': geometry.get('lng')
  }

  countries_list.append(country_components)

component_df = pd.DataFrame(countries_list)

# QUERY
# Connect to the database
host = 'ep-silent-lab-755834.ap-southeast-1.aws.neon.tech'
port = 5432
database = 'dibimbing'
username = 'students'
password = 'Jeruk@2023'
connection = psycopg2.connect(host=host, port=port, database=database, user=username, password=password)

query = 'SELECT * FROM public.pet_stores'
pet_stores_df = pd.read_sql(query,connection)

# Transform
# Data Integration
df = pd.merge(pet_stores_df,pd.merge(population_df,component_df,on='country'),on='country')

# Data Cleaning
df['country_code'] = df['country_code'].str.upper()
df['populations'] = df['populations'].str.replace(',','').astype(int)

# Data Enrichment
df['total_cat_per_store'] = df['populations']/df['total_pet_store']

# Data Filtering
# Top 5 countries with highest cat per store ratio
top_5_df = df.sort_values(by='total_cat_per_store',ascending=False).head(5)

# Load
# Import necessary libraries
from sqlalchemy import create_engine

# Connect to the database
host = 'database'
port = 5432
database = 'dibimbing'
username = 'postgres'
password = 'postgres'

# Create database connection
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

# Load the DataFrame into a PostgreSQL table
top_5_df.to_sql(name='top_5_cat_per_store', con=engine, if_exists='replace', index=False)

# Close the database connection
engine.dispose()