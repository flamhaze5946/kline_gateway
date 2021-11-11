import time


def get_kline_key_name(namespace: str, interval: str, symbol: str):
    return str.join(':', [namespace, interval, symbol])


def timestamp():
    return int(time.time() * 1000)
