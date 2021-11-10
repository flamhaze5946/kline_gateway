import _thread
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import List

from apscheduler.schedulers.blocking import BlockingScheduler
from redis import StrictRedis, ConnectionPool

from subscriber import SubscriberSymbolsBody, KlineFetchWebSocketSubscriber
from utils import timestamp, get_kline_key_name, interval_millseconds_map
from config import klines_web_fetch_worker, timezone, save_buffer_millseconds
from fetcher import CcxtFetcher

max_workers = klines_web_fetch_worker


class Pusher(object):
    def __init__(self, config: dict):
        self._config = config
        fetcher_type = self._config['fetcher_type']
        if fetcher_type == 'ccxt':
            self.fetcher = CcxtFetcher(self._config)
        else:
            raise Exception(f'not support fetcher type: {fetcher_type}')
        self._namespace = self._config['namespace']
        self._ws_url = self._config['ws_url']
        self._clean_klines = self._config['clean_klines']
        self._klines_save_days = self._config['klines_save_days']
        self._channel_count_per_ws = self._config['channel_count_per_ws']
        self._symbols = self.fetcher.get_symbols()
        self._scheduler = BlockingScheduler(timezone=timezone)
        self._save_klines_thread_pool = ThreadPoolExecutor(max_workers=max_workers)

    def _push(self, klines: List[list]):
        pass

    def _clean(self, interval: str, symbols: List[str], save_days: int):
        pass

    def _query_klines_by_start_time(self, interval: str, symbol: str, start_time: int):
        pass

    def _set_kline(self, interval: str, symbol: str, kline: list, kline_time: int, remove: bool, insert: bool):
        pass

    def _save_klines(self, interval: str, symbols: List[str], bar_count: int = 99):
        futures = []
        for symbol in symbols:
            future = self._save_klines_thread_pool.submit(self._save_latest_symbol_klines, symbol, interval, bar_count)
            futures.append(future)
        [future.result() for future in futures]

    def _save_klines0(self, interval: str, symbol: str, klines: List[list]):
        pass

    def _stream_update_with_start(self, symbols_body: SubscriberSymbolsBody):
        for interval, symbols in symbols_body.interval_symbols_map.items():
            self._save_klines(interval, symbols)

    def _register_clean_jobs(self, save_days: int):
        pass

    def _save_latest_symbol_klines(self, symbol: str, interval: str, bar_count: int):
        klines = self.fetcher.get_klines(interval, symbol, bar_count)

        self._save_klines0(interval, symbol, klines)
        print(f'save {bar_count} klines success, symbol: {symbol}, interval: {interval}')

    def _start_stream_update(self):
        interval_symbols_maps = []
        current_map = defaultdict(list)
        map_channel_count = 0
        for interval in interval_millseconds_map.keys():
            for symbol in self._symbols:
                if map_channel_count >= self._channel_count_per_ws:
                    interval_symbols_maps.append(current_map)
                    current_map = defaultdict(list)
                    map_channel_count = 0
                current_map[interval].append(symbol)
                map_channel_count = map_channel_count + 1
        interval_symbols_maps.append(current_map)

        subscribers = []
        for interval_symbols_map in interval_symbols_maps:
            symbols_body = SubscriberSymbolsBody(interval_symbols_map)
            subscriber = KlineFetchWebSocketSubscriber(self._ws_url, symbols_body,
                                                       kline_from_start_time_supplier=self._query_klines_by_start_time,
                                                       kline_setter=self._set_kline,
                                                       with_start=self._stream_update_with_start,
                                                       save_buffer_millseconds=save_buffer_millseconds)
            subscribers.append(subscriber)
        for subscriber in subscribers:
            _thread.start_new_thread(subscriber.start, ())

    def start(self):
        self._start_stream_update()
        if self._clean_klines:
            self._register_clean_jobs(self._klines_save_days)
        self._scheduler.start()


class RedisPusher(Pusher):
    def __init__(self, config: dict):
        super().__init__(config)
        redis_config = self._config['redis']
        redis_pool = ConnectionPool(host=redis_config['host'], port=redis_config['port'],
                                    db=redis_config['db_index'], password=redis_config['password'])
        self._redisc = StrictRedis(connection_pool=redis_pool)

    def _push(self, klines: List[list]):
        super()._push(klines)

    def _clean(self, interval: str, symbols: List[str], save_days: int):
        now = timestamp()
        with self._redisc.pipeline(transaction=False) as pipeline:
            for symbol in symbols:
                key = get_kline_key_name(self._namespace, interval, symbol)
                pipeline.zremrangebyscore(key, 0, now - (save_days * 1000 * 60 * 60 * 24))
            pipeline.execute()

    def _set_kline(self, interval: str, symbol: str, kline: list, kline_time: int, remove: bool, insert: bool):
        key = get_kline_key_name(self._namespace, interval, symbol)
        with self._redisc.pipeline(transaction=True) as pipeline:
            if remove:
                pipeline.zremrangebyscore(key, kline[0], kline_time)
            if insert:
                pipeline.zadd(key, {json.dumps(kline): kline[0]})
            pipeline.execute()

    def _query_klines_by_start_time(self, interval: str, symbol: str, start_time: int):
        key = get_kline_key_name(self._namespace, interval, symbol)
        return self._redisc.zrangebyscore(key, start_time, start_time)

    def _save_klines0(self, interval: str, symbol: str, klines: List[list]):
        key = get_kline_key_name(self._namespace, interval, symbol)
        kline_score_mapping = {json.dumps(kline): kline[0] for kline in klines}

        with self._redisc.pipeline(transaction=True) as pipeline:
            start_time = min(kline_score_mapping.values())
            end_time = max(kline_score_mapping.values())
            pipeline.zremrangebyscore(key, start_time, end_time)
            pipeline.zadd(key, kline_score_mapping)
            pipeline.execute()

    def _register_clean_jobs(self, save_days: int):
        self._register_clean_redis_jobs(save_days)

    def _register_clean_redis_jobs(self, save_days: int):
        self._scheduler.add_job(self._clean, id='clean_redis_4h', args=('4h', self._symbols, save_days),
                                trigger='cron', hour='*/4')
        self._scheduler.add_job(self._clean, id='clean_redis_2h', args=('2h', self._symbols, save_days),
                                trigger='cron', hour='*/2')
        self._scheduler.add_job(self._clean, id='clean_redis_1h', args=('1h', self._symbols, save_days),
                                trigger='cron', hour='*')
        self._scheduler.add_job(self._clean, id='clean_redis_30m', args=('30m', self._symbols, save_days),
                                trigger='cron', minute='*/30')
        self._scheduler.add_job(self._clean, id='clean_redis_15m', args=('15m', self._symbols, save_days),
                                trigger='cron', minute='*/15')
        self._scheduler.add_job(self._clean, id='clean_redis_5m', args=('5m', self._symbols, save_days),
                                trigger='cron', minute='*/5')
        self._scheduler.add_job(self._clean, id='clean_redis_3m', args=('3m', self._symbols, save_days),
                                trigger='cron', minute='*/3')
        self._scheduler.add_job(self._clean, id='clean_redis_1m', args=('1m', self._symbols, save_days),
                                trigger='cron', minute='*')
