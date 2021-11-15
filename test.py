from fetcher import CcxtFetcher


def action(interval, symbol, klines):
    print(interval)
    print(symbol)
    print(len(klines))


if __name__ == '__main__':
    fetcher = CcxtFetcher({})
    klines = fetcher.get_klines('1m', 'BTCUSDT', 1630736160000, 1636736700000, 5000,
                                return_klines=False, with_action=action)
