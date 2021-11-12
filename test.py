from fetcher import CcxtFetcher

if __name__ == '__main__':
    fetcher = CcxtFetcher(None)
    klines = fetcher.get_klines('1m', 'BTCUSDT', 1636736160000, 1636736700000, 2000)
    for kline in klines:
        print(kline)
    time_set = []
    for kline in klines:
        time_set.append(kline[0])
    print(len(time_set))
    print(len(set(time_set)))
