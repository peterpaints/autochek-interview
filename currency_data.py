import argparse
import os
import sched
import time
import requests
import sqlite3
import logging
from datetime import datetime, timedelta

from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv

load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Define the currencies to fetch data for
currencies = ["NGN", "GHS", "KES", "UGX", "MAD", "XOF", "EGP"]

# Define the API endpoint and credentials
API_URL = os.getenv('API_URL')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')

s = sched.scheduler(time.time, time.sleep)


def schedule_task(scheduler, task):
    """
    Schedule a task to run at a specific time.

    Parameters
    ----------
    scheduler : sched.scheduler
        The scheduler to use.
    task : callable
        The task to schedule.
    """
    next_run = datetime.now().replace(
        hour=1 if datetime.now().hour < 12 else 23, minute=0, second=0)
    if datetime.now() >= next_run:
        next_run += timedelta(hours=12)
    scheduler.enterabs(time.mktime(next_run.timetuple()), 1, task,
                       (scheduler, ))


class Database:
    """
    A singleton class for managing a SQLite database.

    Parameters
    ----------
    dbname : str, optional
        The name of the database file'
    """

    _instance = None

    def __new__(cls, dbname):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance.connection = sqlite3.connect(dbname)
        return cls._instance

    def create_table(self, table_name, schema):
        """
        Create a new table in the database.

        Parameters
        ----------
        table_name : str
            The name of the table to create.
        schema : dict
            A dictionary where the keys are column names and the values are the data types for the columns.
        """
        cursor = self.connection.cursor()
        columns = ', '.join(f'{name} {type}' for name, type in schema.items())
        cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {columns}
                )
            ''')

    def create_unique_index(self, table_name, index_name, columns):
        """
        Create a unique index on a table for the specified columns.

        Parameters
        ----------
        table_name : str
            The name of the table on which to create the index.
        index_name : str
            The name of the index to create.
        columns : list of str
            A list of column names to include in the index.
        """
        cursor = self.connection.cursor()
        columns_str = ', '.join(columns)
        cursor.execute(
            f'CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns_str})'
        )
        self.connection.commit()

    def insert_data(self, table_name, data, schema):
        """
        Insert data into a table in the database.

        Parameters
        ----------
        table_name : str
            The name of the table to insert data into.
        data : iterable
            An iterable where each item is a tuple that corresponds to a row to be inserted into the table.
        schema : dict
            A dictionary representing the table schema where keys are column names and values are their respective data types.
        """
        cursor = self.connection.cursor()
        placeholders = ', '.join('?' * len(schema))
        cursor.executemany(
            f'''
            INSERT OR REPLACE INTO {table_name} VALUES ({placeholders})
        ''', data)
        self.connection.commit()

    def close(self):
        """
        Close the connection to the database.
        """
        self.connection.close()


def _fetch(params, api_url=API_URL, auth=(USERNAME, PASSWORD)):
    """
    Fetch data from the API.

    Parameters
    ----------
    params : dict
        The parameters to send with the API request.
    api_url : str, optional
        The URL of the API endpoint, by default API_URL
    auth : tuple, optional
        A tuple containing the username and password for API authentication, by default (USERNAME, PASSWORD)

    Returns
    -------
    dict
        The JSON response from the API.
    """
    response = requests.get(api_url, params=params, auth=auth)
    return response.json()


def fetch_data_for_currency(currency):
    """
    Fetch exchange rate data for a specific currency.

    Parameters
    ----------
    currency : str
        The currency to fetch data for.

    Returns
    -------
    tuple
        A tuple containing the timestamp, the base currency, the exchange rate from USD to the specified currency,
        the exchange rate from the specified currency to USD, and the specified currency.
    """
    usd_to_curr = _fetch({"from": "USD", "to": currency})
    USD_to_currency_rate = usd_to_curr["to"][0]["mid"]

    curr_to_usd = _fetch({"from": currency, "to": "USD"})
    currency_to_USD_rate = curr_to_usd["to"][0]["mid"]
    timestamp = curr_to_usd["timestamp"]

    return (timestamp, "USD", USD_to_currency_rate, currency_to_USD_rate,
            currency)


def fetch_and_save_data(sc=None):
    """
    Fetch currency data and save it to the database.

    Parameters
    ----------
    sc : sched.scheduler, optional
        The scheduler to use for scheduling the next run of this function.
    """
    logging.info("Starting the data fetch and save process.")
    db = Database('currency_data.db')
    schema = {
        'timestamp': 'TEXT',
        'currency_from': 'TEXT',
        'USD_to_currency_rate': 'REAL',
        'currency_to_USD_rate': 'REAL',
        'currency_to': 'TEXT'
    }
    db.create_table('currency_data', schema)
    logging.info("Database table 'currency_data' created or exists already.")

    db.create_unique_index('currency_data', 'idx_timestamp_currency',
                           ['timestamp', 'currency_to'])

    with ThreadPoolExecutor() as executor:
        logging.info("Fetching data for currencies ...")
        results = executor.map(fetch_data_for_currency, currencies)

    logging.info("Data fetched. Inserting into the database.")
    db.insert_data('currency_data', results, schema)
    db.close()
    logging.info("Data insertion complete and database connection closed.")

    if sc is not None:
        schedule_task(sc, fetch_and_save_data)
        logging.info("Scheduled the next run of fetch_and_save_data.")


parser = argparse.ArgumentParser()
parser.add_argument(
    '--manual',
    action='store_true',
    help='Run the script manually without waiting for the scheduler')
args = parser.parse_args()


if args.manual:
    fetch_and_save_data()
else:
    # Schedule the first run
    schedule_task(s, fetch_and_save_data)
    # Start the scheduler
    s.run()
