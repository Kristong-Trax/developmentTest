import requests
import pandas as pd
import json


def add(x, y):
    """Add Function"""
    return x + y


def divide(x, y):
    """Divide Function"""
    if y == 0:
        raise ValueError('Can not divide by zero!')
    return x / y


def get_api_content(url):
    response = requests.get(url)
    response_content = response.json()
    return response_content


def get_emojis_api_url():
    response_content = get_api_content('https://api.github.com')
    response_series = pd.Series(response_content)
    if response_series.empty:
        return None
    else:
        return response_series['emojis_url'].split()


if __name__ == '__main__':
    x = get_emojis_api_url()
    print x