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

    def get_symbols(self):
        super().get_symbols()

    def get_klines(self, interval: str, symbol: str, bar_count: int):
        super().get_klines(interval, symbol, bar_count)

