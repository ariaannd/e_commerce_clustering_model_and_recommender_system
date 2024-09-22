'''
=================================================
E-Commerce DAG

This program is designed to automate the process of extracting data from PostgreSQL, performing data preprocessing  
to create new cleaned data to analyze 
=================================================

'''

# Import Libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
import pytz

# Connecting to Postgres
from sqlalchemy import create_engine 

def load_data_to_pg():
    '''
    This function is used to load csv data to postgress database

        Params:
            - database (str): the name of the local database
            - username (str): the name of username used to access the local database
            - password (str): the password used to access the local database

        Return: df table loaded to postgres 

        Example of use: load_data_to_postgres('database', 'username', 'password')
    '''
    # Define database, username, password, and host
    database = "e_commerce"
    username = "e_commerce"
    password = "e_commerce"
    host = "postgres"

    # Define URL connecting to PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Use URL connecting to SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    # Fetch data using SQLAlchemy
    df = pd.read_csv('/opt/airflow/dags/data.csv', encoding='latin-1')
    df.to_sql('table_e_commerce', conn, index=False, if_exists='replace')
    

    
def fetch_data_pg():
    '''
    This function is used to fetch data from PostgreSQL
    
    Params:
            - database (str): the name of the local database
            - username (str): the name of username used to access the local database
            - password (str): the password used to access the local database

        Return: df table fetched from postgres 

        Example of use: fetch_data_pg('database', 'username', 'password')
    '''
    # Define database, username, password, and host
    database = "e_commerce"
    username = "e_commerce"
    password = "e_commerce"
    host = "postgres"

    # Make URL Connecting PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Use URL Connecting to SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    # Fetch data using SQLAlchemy
    df = pd.read_sql_query("select * from table_e_commerce", conn) #nama table sesuaikan sama nama table di postgres
    df.to_csv('/opt/airflow/dags/e_commerce_data_new.csv', sep=',', index=False)
    

def preprocessing(): 
    ''' 
    This function is used to preprocess and clean the dataset
    
        Params: -

        Return: -

        Example of use: preprocessing()
    '''
    # Define the Data
    data = pd.read_csv("/opt/airflow/dags/e_commerce_data_new.csv")

    # Data Cleaning and Preprocessing 
    
    # Change Customer ID to String
    data['CustomerID'] = data['CustomerID'].astype(str)
    
    # Change Date to Datetype
    data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'])
    
    # Drop Duplicates
    data.drop_duplicates(inplace=True)
    
    # Drop Missing Values
    data.dropna(inplace=True)
    data.reset_index(drop=True, inplace=True)
    
    # Cancellation Data
    
    # Filter out the rows with InvoiceNo starting with "C" for Cancelled
    data['Transaction_Status'] = np.where(data['InvoiceNo'].astype(str).str.startswith('C'), 'Cancelled', 'Completed')

    # Analyze the characteristics of these rows (considering the new column)
    cancelled_transactions = data[data['Transaction_Status'] == 'Cancelled']

    # Finding the percentage of cancelled transactions
    cancelled_percentage = (cancelled_transactions.shape[0] / data.shape[0]) * 100

    # Printing the percentage of cancelled transactions
    print(f"The percentage of cancelled transactions in the dataset is: {cancelled_percentage:.2f}%")
    
    # Stock Code Anomaly
    unique_stock_codes = data['StockCode'].unique()
    
    # Finding and printing the stock codes with 0 and 1 numeric characters
    anomalous_stock_codes = [code for code in unique_stock_codes if sum(c.isdigit() for c in str(code)) in (0, 1)]

    # Removing rows with anomalous stock codes from the dataset
    data = data[~data['StockCode'].isin(anomalous_stock_codes)]
    
    # Lower Case Product Description
    
    service_related_descriptions = ["Next Day Carriage", "High Resolution Image"]
    
    # Remove rows with service-related information in the description
    data = data[~data['Description'].isin(service_related_descriptions)]

    # Standardize the text to uppercase to maintain uniformity across the dataset
    data['Description'] = data['Description'].str.upper()
    
    # Unit Price Anomaly
    
    # Removing records with a unit price of zero to avoid potential data entry errors
    data = data[data['UnitPrice'] > 0]

    # Resetting the index of the cleaned dataset
    data.reset_index(drop=True, inplace=True)
    
    # Data Type Handling
    
    # Changing the data type of 'CustomerID' to string as it is a unique identifier and not used in mathematical operations
    data['CustomerID'] = data['CustomerID'].astype(str)

    # Change InvoiceDate to datetime datatype
    data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'])
    
    # Breakdown InvoiceDate Into Year and Quarter
    
    # Extract year into a separate column and convert it to string
    data['Year'] = data['InvoiceDate'].dt.year.astype(str)

    # Define a function to get the quarter
    def get_quarter(month):
        if month in range(1, 4):
            return 'Q1'
        elif month in range(4, 7):
            return 'Q2'
        elif month in range(7, 10):
            return 'Q3'
        else:
            return 'Q4'

    # Apply the function to 'Month' column to get the quarter
    data['Quarter'] = data['InvoiceDate'].dt.month.apply(get_quarter)
    
    # Breakdown Countries Into Continents
    
    # Define a dictionary mapping countries to continents
    country_to_continent = {
        "United Kingdom": "Europe", "France": "Europe", "Australia": "Oceania", 
        "Netherlands": "Europe", "Germany": "Europe", "Norway": "Europe", "EIRE": "Europe", 
        "Switzerland": "Europe", "Spain": "Europe", "Poland": "Europe", "Portugal": "Europe", 
        "Italy": "Europe", "Belgium": "Europe", "Lithuania": "Europe", "Japan": "Asia", 
        "Iceland": "Europe", "Channel Islands": "Europe", "Denmark": "Europe", "Cyprus": "Europe", 
        "Sweden": "Europe", "Austria": "Europe", "Israel": "Asia", "Finland": "Europe", 
        "Greece": "Europe", "Singapore": "Asia", "Lebanon": "Asia", "United Arab Emirates": "Asia", 
        "Saudi Arabia": "Asia", "Czech Republic": "Europe", "Canada": "North America", 
        "Brazil": "South America", "USA": "North America", "European Community": "Europe", 
        "Bahrain": "Asia", "Malta": "Europe", "RSA": "Africa"
    }

    # Add new column "Continent" based on the mapping
    data["Continent"] = data["Country"].map(country_to_continent)
    
    # Create Sales Column
    
    data['Sales'] = data['Quantity'] * data['UnitPrice']
    
    # Breakdown Product Description
    
    # Create a list of keywords or categories to search for in the descriptions
    keywords = ['T-LIGHT', 'CAKESTAND', 'ORNAMENT', 'LANTERN', 'COAT HANGER', 'TEA LIGHT', 'BOX', 
                'BOTTLE', 'NAPKIN', 'CUSHION', 'PLATE', 'LUNCH BOX', 'JIGSAW', 'MUG', 'FRAME', 'BAG', 'CUTLERY', 'CAKE CASES', 
                'STORAGE BAG', 'SIGN', 'CANDLE', 'HOOK', 'CANDLE STICK', 'EGG', 'TOYS', 'CARD', 'CANDLE', 'FLOWER', 'ORNAMENT', 
                'APRON', 'CLOCK', 'HOTTIE', 'HAND WARMER', 'DOLL', 'TEA', 'SHOPPER', 'PANTRY', 'CAKE','SPOTTY BUNTING', 'CHRISTMAS', 
                'WICKER', 'CHALKBOARD', 'JAM MAKING', 'PICTURE FRAME', 'BAKING SET', 'GLASS', 'TEACUP', 'SHIRT', 'EAR MUFF', 'TOY', 
                'INFLATABLE', 'COOKIE CUTTER', 'PAPER CHAIN', 'FASHION', 'COAT','LIGHTS', 'KITCHEN SCALES', 'MUG', 'WREATH', 'STORAGE',
                'BOXES', 'PLAYHOUSE','ART', 'WALLET', 'TISSUE', 'GAME', 'DOORMAT', 'DRAWER KNOB', 'CABINET', 'PLASTERS', 'BLOCK WORD',
                'DECORATION', 'STICKER', 'BOWL', 'PENCIL', 'POT', 'CONTAINER', 'HOLDER', 'EASTER', 'RIBBON', 'BOOK', 'TOWEL', 'SCARF', 
                'PURSE']

    # Function to check if a description contains any of the keywords
    def extract_category(description):
        for keyword in keywords:
            if keyword in description:
                return keyword
        # If no keyword is found, categorize as 'OTHER'
        return 'OTHER'  

    # Apply the function to create a new column with product categories
    data['ProductCategory'] = data['Description'].apply(extract_category)

    # Define categories and keywords
    categories = {
        'HOME DECORATION': ['SIGN', 'T-LIGHT', 'CANDLE', 'WICKER', 'CUSHION', 'HOOK', 'LANTERN', 'ORNAMENT', 'TEA LIGHT', 'LIGHTS', 'DRAWER KNOB', 'CABINET'],
        'KITCHENWARE': ['BOTTLE', 'CAKE', 'CAKE CASES', 'MUG', 'NAPKIN', 'PLATE', 'EGG', 'CUTLERY', 'PANTRY', 'CAKESTAND', 'JAM MAKING', 'APRON', 'BAKING SET','COOKIE CUTTER', 'KITCHEN SCALES','MUG', 'BOWL'],
        'UTILITY': ['CLOCK', 'COAT HANGER', 'TISSUE', 'PLASTERS', 'PENCIL', 'BOOK', 'TOWEL'],
        'SMALL DECORATION': ['CHRISTMAS', 'GLASS', 'FRAME', 'FLOWER', 'CHALKBOARD', 'SPOTTY BUNTING', 'PAPER CHAIN', 'WREATH','ART', 'DOORMAT', 'CABINET', 'DECORATION', 'STICKER', 'EASTER', 'RIBBON'],
        'CARDS' : ['CARD'],
        'CONTAINER': ['BAG', 'BOX', 'BOXES','SHOPPER', 'STORAGE', 'POT', 'CONTAINER', 'HOLDER'],
        'TEASET': ['TEA'],
        'TOYS' :['INFLATABLE', 'TOY', 'JIGSAW', 'DOLL','PLAYHOUSE', 'GAME', 'BLOCK WORD'],
        'FASHION': ['HAND WARMER', 'HOTTIE', 'SHIRT', 'EAR MUFF','FASHION','COAT', 'WALLET', 'SCARF', 'PURSE'],
        'OTHER': ['OTHER']
    }

    # Function to match categories using keywords
    def match_category(description):
        for category, keywords in categories.items():
            for keyword in keywords:
                if keyword in description.upper():
                    return category
        return 'OTHER'  # If no category matches, return 'OTHER'

    # Apply the function to create a new column with categories
    data['ProductType'] = data['ProductCategory'].apply(match_category)
    
        
    
        # Convert into clean data
    data.to_csv('/opt/airflow/dags/e_commerce_data_clean.csv', index=False)

def load_clean_data_to_pg():
    '''
    This function is used to load csv data to postgress database

        Params:
            - database (str): the name of the local database
            - username (str): the name of username used to access the local database
            - password (str): the password used to access the local database

        Return: df table loaded to postgres 

        Example of use: load_data_to_postgres('database', 'username', 'password')
    '''
    # Define database, username, password, and host
    database = "e_commerce"
    username = "e_commerce"
    password = "e_commerce"
    host = "postgres"

    # Define URL connecting to PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Use URL connecting to SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    # Fetch data using SQLAlchemy
    df = pd.read_csv('/opt/airflow/dags/e_commerce_data_clean.csv')
    df.to_sql('table_e_commerce_clean', conn, index=False, if_exists='replace')     
        
# Define Airflow DAG with tasks for loading data into PostgreSQL, fetching data from PostgreSQL, preprocessing data, and loading cleaned data scheduled daily at 23.59
local_time = pytz.timezone('Asia/Jakarta')      
default_args = {
    'owner': 'Group 3', 
    'start_date': datetime(2023, 12, 24, 12, 00,tzinfo=local_time) 
}

with DAG(
    "Final_Project_DAG", # Project Name
    description='Milestone_3',
    schedule_interval='59 23 * * *', # Scheduled 23:59.
    default_args=default_args, 
    catchup=False
) as dag:
    # Task : 1
    load_data_to_postgres = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_pg)
    
    # Task: 2
    fetch_data_from_postgres = PythonOperator(
        task_id='fetch_data_from_postgres',
        python_callable=fetch_data_pg) 

    # Task: 3
    data_preprocessing = PythonOperator(
        task_id='data_preprocessing',
        python_callable=preprocessing)

    # Task: 4
    load_clean_data_to_postgres = PythonOperator(
        task_id='load_clean_data_to_postgres',
        python_callable=load_clean_data_to_pg)

    # Airflow Process
    load_data_to_postgres >> fetch_data_from_postgres >> data_preprocessing >> load_clean_data_to_postgres




