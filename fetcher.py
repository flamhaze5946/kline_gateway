import ccxt

"""
K线格式
[
    开盘时间,
    开盘价,
    最高价,
    最低价,
    收盘价,
    成交量,
    收盘时间,
    成交额,
    成交笔数,
    主动买入成交量,
    主动买入成交额,
    忽略项
]
"""
class Fetcher(object):
    def __init__(self, config: dict):
        self._config = config

    def get_symbols(self):
        pass

    def get_klines(self, interval: str, symbol: str, start_time: int = None, end_time: int = None, bar_count: int = 99):
        pass


class RedisFetcher(Fetcher):
    pass


class MysqlFetcher(Fetcher):
    pass


class MongoFetcher(Fetcher):
    pass


class CcxtFetcher(Fetcher):
    def __init__(self, config: dict):
        super().__init__(config)
        self._binance = ccxt.binance({'enableRateLimit': True})

    def get_symbols(self):
        exchange_info = self._binance.fapiPublicGetExchangeInfo()
        return [symbol_info['symbol'] for symbol_info in exchange_info['symbols']]

    def get_klines(self, interval: str, symbol: str, start_time: int = None, end_time: int = None, bar_count: int = 99):
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': bar_count
        }
        if start_time is not None:
            params['startTime'] = start_time
        if end_time is not None:
            params['endTime'] = end_time
        return self._format(self._binance.fapiPublicGetKlines(params))

    def _format(self, klines: list) -> list:
        format_klines = []
        for kline in klines:
            format_kline = [
                int(kline[0]),
                kline[1],
                kline[2],
                kline[3],
                kline[4],
                kline[5],
                int(kline[6]),
                kline[7],
                int(kline[8]),
                kline[9],
                kline[10],
                kline[11]
            ]
            format_klines.append(format_kline)
        return format_klines

