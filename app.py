import datetime
import requests
import sqlite3
import json

connection = sqlite3.connect("SmallExchangeRate.sqlite")
cursor = connection.cursor()

datetime_mask = '%d-%m-%Y %H:%M'


def get_datetime(format_date):
    datetime_now = datetime.datetime.now()
    return datetime_now.strftime(format_date)


def extract(method):
    response = requests.get('https://api.exchangeratesapi.io/latest?base=USD')
    if method == 'get_status':
        return response.status_code
    elif method == 'get_json':
        return json.loads(response.text)


def transform(method):
    raw_json = extract('get_json')
    currency_list = []
    if method == 'get_currency_name':
        for currency in raw_json['rates'].keys():
            currency_list.append(currency)
        return currency_list
    elif method == 'get_currency_value':
        for currency in raw_json['rates'].items():
            currency_list.append(str(currency[1]))
        return currency_list


def create_table():
    currency_name = []
    currency_name_list = transform('get_currency_name')

    for name in currency_name_list:
        currency_name.append(name + ' REAL')

    query_rates = f'CREATE TABLE IF NOT EXISTS RATES (LOAD_DATE CURRENT_TIMESTAMP, {", ".join(currency_name)});'
    cursor.execute(query_rates)
    connection.commit()


def checking():
    status_api = extract('get_status')
    query = "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'RATES';"

    if tuple(check[0] for check in cursor.execute(query))[0] == 1 and status_api == 200:
        return True
    elif tuple(check[0] for check in cursor.execute(query))[0] == 0:
        create_table()
        return True
    elif tuple(check[0] for check in cursor.execute(query))[0] == 1 and status_api != 200:
        return False


def load():
    currency_value = [get_datetime(datetime_mask)]
    currency_value_list = transform('get_currency_value')

    for value in currency_value_list:
        currency_value.append(value)

    query = f'INSERT INTO RATES VALUES ({", ".join("?" * (len(currency_value_list) + 1))})'
    cursor.execute(query, currency_value)
    connection.commit()


def data_moving():
    if checking():
        load()
