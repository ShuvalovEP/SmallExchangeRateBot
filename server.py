import requests
import configparser

from app import get_all_rates
from os import path

import os

environment = path.join(os.getcwd(), 'environment', 'config.ini')
config = configparser.ConfigParser()
config.read(environment)

chat_id = config['TelegramBot']['chat_id']
TOKEN = config['TelegramBot']['token']
PROXY = {'https': f"socks5h://{config['Proxy']['user']}:{config['Proxy']['password']}@{config['Proxy']['url']}:1080"}
URL = f'https://api.telegram.org/bot{TOKEN}/'

message_id_list = []


def send_message(text):
    params = {'chat_id': chat_id, 'text': text}
    return requests.post(URL + 'sendMessage', data=params, proxies=PROXY)


def ping_pong():
    response = requests.post(URL + 'getUpdates', proxies=PROXY)
    result_json = response.json()['result']
    message_id = result_json[-1]['message'].get('message_id')
    if result_json[-1]['message']['text'] == '/list' and message_id not in message_id_list:
        send_message(get_all_rates())
        message_id_list.append(result_json[-1]['message'].get('message_id'))
    elif message_id not in message_id_list:
        send_message(result_json[-1]['message']['text'])
        message_id_list.append(result_json[-1]['message'].get('message_id'))


if __name__ == '__main__':
    send_message(f'Watch Dog обнаружил цель')
    while True:
        ping_pong()
