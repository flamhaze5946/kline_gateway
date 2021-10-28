import time


interval_millseconds_map = {
    '1m': 1000 * 60 * 1,
    '3m': 1000 * 60 * 3,
    '5m': 1000 * 60 * 5,
    '15m': 1000 * 60 * 15,
    '30m': 1000 * 60 * 30,
    '1h': 1000 * 60 * 60 * 1,
    '2h': 1000 * 60 * 60 * 2,
    '4h': 1000 * 60 * 60 * 4,
}


def get_kline_key_name(namespace: str, interval: str, symbol: str):
    return str.join(':', [namespace, interval, symbol])


def timestamp():
    return int(time.time() * 1000)
