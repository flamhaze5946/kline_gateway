import _thread
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import List

from apscheduler.schedulers.blocking import BlockingScheduler
from redis import StrictRedis, ConnectionPool

from KlineFetchWebSocketSubscriber import SubscriberSymbolsBody, KlineFetchWebSocketSubscriber
from KlinePush import channel_count_per_ws
from KlineUtils import timestamp, get_kline_key_name, interval_millseconds_map
from config import klines_web_fetch_worker, timezone, save_buffer_millseconds
from fetcher import CcxtFetcher

max_workers = klines_web_fetch_worker
scheduler = BlockingScheduler(timezone=timezone)

last_interval_time = {}

ws_url = 'wss://fstream.binance.com/ws'


class Pusher(object):
    def __init__(self, config: dict):
        self._config = config
        fetcher_type = self._config['fetcher_type']
        if fetcher_type == 'ccxt':
            self.fetcher = CcxtFetcher(self._config)
        else:
            raise Exception(f'not support fetcher type: {fetcher_type}')
        self.symbols = self.fetcher.get_symbols()
        self.save_klines_thread_pool = ThreadPoolExecutor(max_workers=max_workers)

    def _push(self, klines: list):
        pass

    def _clean(self, interval: str, symbols: List[str], save_days: int):
        pass

    def query_klines_by_start_time(self, interval: str, symbol: str, start_time: int):
        pass

    def set_kline(self, interval: str, symbol: str, kline, kline_time: int, remove: bool, insert: bool):
        pass

    def _save_klines(self, interval: str, symbol: str, klines: list):
        pass

    def _stream_update_with_start(self, symbols_body: SubscriberSymbolsBody):
        for interval, symbols in symbols_body.interval_symbols_map.items():
            self.save_klines(interval, symbols)

    def save_klines(self, interval: str, symbols: List[str], bar_count: int = 99):
        futures = []
        for symbol in symbols:
            future = self.save_klines_thread_pool.submit(self.save_symbol_klines, symbol, interval, bar_count)
            futures.append(future)
        [future.result() for future in futures]

    def save_symbol_klines(self, symbol: str, interval: str, bar_count: int):
        klines = self.fetcher.get_klines(symbol, interval, bar_count)
        last_kline_time = klines[-1][0]
        last_interval_time[interval] = last_kline_time

        self._save_klines(interval, symbol, klines)
        print(f'save {bar_count} klines success, symbol: {symbol}, interval: {interval}')

    def start_stream_update(self):
        interval_symbols_maps = []
        current_map = defaultdict(list)
        map_channel_count = 0
        for interval in interval_millseconds_map.keys():
            for symbol in self.symbols:
                if map_channel_count >= channel_count_per_ws:
                    interval_symbols_maps.append(current_map)
                    current_map = defaultdict(list)
                    map_channel_count = 0
                current_map[interval].append(symbol)
                map_channel_count = map_channel_count + 1
        interval_symbols_maps.append(current_map)

        subscribers = []
        for interval_symbols_map in interval_symbols_maps:
            symbols_body = SubscriberSymbolsBody(interval_symbols_map)
            subscriber = KlineFetchWebSocketSubscriber(ws_url, self, symbols_body,
                                                       with_start=self._stream_update_with_start,
                                                       save_buffer_millseconds=save_buffer_millseconds)
            subscribers.append(subscriber)
        for subscriber in subscribers:
            _thread.start_new_thread(subscriber.start, ())


class RedisPusher(Pusher):
    def __init__(self, config: dict):
        super().__init__(config)
        redis_config = self._config['redis']
        redis_pool = ConnectionPool(host=redis_config['host'], port=redis_config['port'],
                                    db=redis_config['db_index'], password=redis_config['password'])
        self._redisc = StrictRedis(connection_pool=redis_pool)

    def _push(self, klines: list):
        super()._push(klines)

    def _clean(self, interval: str, symbols: List[str], save_days: int):
        now = timestamp()
        with self._redisc.pipeline(transaction=False) as pipeline:
            for symbol in symbols:
                key = get_kline_key_name(interval, symbol)
                pipeline.zremrangebyscore(key, 0, now - (save_days * 1000 * 60 * 60 * 24))
            pipeline.execute()

    def set_kline(self, interval: str, symbol: str, kline, kline_time: int, remove: bool, insert: bool):
        key = get_kline_key_name(interval, symbol)
        with self._redisc.pipeline(transaction=True) as pipeline:
            if remove:
                pipeline.zremrangebyscore(key, kline[0], kline_time)
            if insert:
                pipeline.zadd(key, {json.dumps(kline): kline[0]})
            pipeline.execute()

    def query_klines_by_start_time(self, interval: str, symbol: str, start_time: int):
        key = get_kline_key_name(interval, symbol)
        self._redisc.zrangebyscore(key, start_time, start_time)

    def _save_klines(self, interval: str, symbol: str, klines: list):
        key = get_kline_key_name(interval, symbol)
        kline_score_mapping = {json.dumps(kline): kline[0] for kline in klines}

        with self._redisc.pipeline(transaction=True) as pipeline:
            start_time = min(kline_score_mapping.values())
            end_time = max(kline_score_mapping.values())
            pipeline.zremrangebyscore(key, start_time, end_time)
            pipeline.zadd(key, kline_score_mapping)
            pipeline.execute()

