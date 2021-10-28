import ccxt


class Fetcher(object):
    def __init__(self, config: dict):
        self._config = config

    def get_symbols(self):
        pass

    def get_klines(self, interval: str, symbol: str, bar_count: int):
        pass


class CcxtFetcher(Fetcher):
    def __init__(self, config: dict):
        super().__init__(config)
        self._binance = ccxt.binance()

    def get_symbols(self):
        exchange_info = self._binance.fapiPublicGetExchangeInfo()
        return [symbol_info['symbol'] for symbol_info in exchange_info['symbols']]

    def get_klines(self, interval: str, symbol: str, bar_count: int):
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': bar_count
        }
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

