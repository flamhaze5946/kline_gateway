import _thread

import pandas
import pandas as pd
import json
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import List

from apscheduler.schedulers.blocking import BlockingScheduler
from arctic.date import DateRange
from redis import StrictRedis, ConnectionPool

from subscriber import SubscriberSymbolsBody, KlineFetchWebSocketSubscriber
from utils import timestamp, get_kline_key_name
from config import klines_web_fetch_worker, timezone, save_buffer_millseconds
from fetcher import CcxtFetcher
import dolphindb as ddb

max_workers = klines_web_fetch_worker

fetcher_constructor_map = {
    'ccxt': CcxtFetcher
}


class Pusher(object):
    def __init__(self, config: dict):
        self._config = config

        fetcher_type = self._config['fetcher_type']
        if fetcher_type not in fetcher_constructor_map.keys():
            raise Exception(f'not support fetcher type: {fetcher_type}')
        self.fetcher = fetcher_constructor_map[fetcher_type](self._config)

        self._stream_update = self._config['stream_update']
        self.intervals = self._config['intervals']
        self._namespace = self._config['namespace']
        self._ws_url = self._config['ws_url']
        self._clean_klines = self._config['clean_klines']
        self._klines_save_days = self._config['klines_save_days']
        self._channel_count_per_ws = self._config['channel_count_per_ws']
        self._init_bar_count = self._config['init_bar_count']
        self._symbols = self.fetcher.get_symbols()
        self._scheduler = BlockingScheduler(timezone=timezone)
        self._save_klines_thread_pool = ThreadPoolExecutor(max_workers=max_workers)

    def _clean(self, interval: str, symbols: List[str], save_days: int):
        pass

    def _query_klines_by_start_time(self, interval: str, symbol: str, start_time: int):
        pass

    def _save_klines(self, interval: str, symbols: List[str],
                     start_time: int = None, end_time: int = None, bar_count: int = 99):
        futures = []
        for symbol in symbols:
            future = self._save_klines_thread_pool.submit(self._save_latest_symbol_klines,
                                                          symbol, interval, start_time, end_time, bar_count)
            futures.append(future)
        [future.result() for future in futures]

    def _save_klines0(self, interval: str, symbol: str, klines: List[list]):
        pass

    def _fetch_and_save_klines(self, symbols_body: SubscriberSymbolsBody):
        for interval, symbols in symbols_body.interval_symbols_map.items():
            self._save_klines(interval, symbols, bar_count=self._init_bar_count)

    def _register_clean_jobs(self, save_days: int):
        self._scheduler.add_job(self._clean, id='clean_4h', args=('4h', self._symbols, save_days),
                                trigger='cron', hour='*/4')
        self._scheduler.add_job(self._clean, id='clean_2h', args=('2h', self._symbols, save_days),
                                trigger='cron', hour='*/2')
        self._scheduler.add_job(self._clean, id='clean_1h', args=('1h', self._symbols, save_days),
                                trigger='cron', hour='*')
        self._scheduler.add_job(self._clean, id='clean_30m', args=('30m', self._symbols, save_days),
                                trigger='cron', minute='*/30')
        self._scheduler.add_job(self._clean, id='clean_15m', args=('15m', self._symbols, save_days),
                                trigger='cron', minute='*/15')
        self._scheduler.add_job(self._clean, id='clean_5m', args=('5m', self._symbols, save_days),
                                trigger='cron', minute='*/5')
        self._scheduler.add_job(self._clean, id='clean_3m', args=('3m', self._symbols, save_days),
                                trigger='cron', minute='*/3')
        self._scheduler.add_job(self._clean, id='clean_1m', args=('1m', self._symbols, save_days),
                                trigger='cron', minute='*')

    def _save_latest_symbol_klines(self, symbol: str, interval: str,
                                   start_time: int = None, end_time: int = None, bar_count: int = 99):
        self.fetcher.get_klines(interval, symbol, start_time, end_time,
                                         bar_count=bar_count, return_klines=False, with_action=self._save_klines0)
        print(f'save {bar_count} klines success, symbol: {symbol}, interval: {interval}')

    def _start0(self):
        interval_symbols_maps = []
        current_map = defaultdict(list)
        map_channel_count = 0
        for interval in self.intervals:
            for symbol in self._symbols:
                if map_channel_count >= self._channel_count_per_ws:
                    interval_symbols_maps.append(current_map)
                    current_map = defaultdict(list)
                    map_channel_count = 0
                current_map[interval].append(symbol)
                map_channel_count = map_channel_count + 1
        interval_symbols_maps.append(current_map)

        symbols_bodys = []
        for interval_symbols_map in interval_symbols_maps:
            symbols_body = SubscriberSymbolsBody(interval_symbols_map)
            symbols_bodys.append(symbols_body)
        if self._stream_update:
            for symbols_body in symbols_bodys:
                if self._stream_update:
                    subscriber = KlineFetchWebSocketSubscriber(
                        self._ws_url, symbols_body,
                        kline_from_start_time_supplier=self._query_klines_by_start_time,
                        kline_setter=self._save_klines0,
                        with_start=self._fetch_and_save_klines,
                        save_buffer_millseconds=save_buffer_millseconds)
                    _thread.start_new_thread(subscriber.start, ())
            if self._clean_klines:
                self._register_clean_jobs(self._klines_save_days)
            self._scheduler.start()
        else:
            for symbols_body in symbols_bodys:
                self._fetch_and_save_klines(symbols_body)

    def start(self):
        self._start0()


class MongoPusher(Pusher):
    def __init__(self, config: dict):
        super().__init__(config)


class DolphinPusher(Pusher):
    def __init__(self, config: dict):
        super().__init__(config)

    def _clean(self, interval: str, symbols: List[str], save_days: int):
        super()._clean(interval, symbols, save_days)

    def _query_klines_by_start_time(self, interval: str, symbol: str, start_time: int):
        super()._query_klines_by_start_time(interval, symbol, start_time)

    def _save_klines0(self, interval: str, symbol: str, klines: List[list]):
        super()._save_klines0(interval, symbol, klines)


class ArcticPusher(Pusher):
    """
    没法用, 以后知道怎么用再看看
    """

    interval_collection_map = {}

    def __init__(self, config: dict):
        super().__init__(config)
        self.host = self._config['mongodb']['host']
        self.port = self._config['mongodb']['port']
        self.db_name = self._config['mongodb']['db_name']
        self.collection_prefix = self._config['mongodb']['collection_prefix']
        self.connection = Arctic(f'{self.host}:{self.port}', self.db_name)

    def _clean(self, interval: str, symbols: List[str], save_days: int):
        super()._clean(interval, symbols, save_days)

    def _query_klines_by_start_time(self, interval: str, symbol: str, start_time: int):
        db = self._get_database(interval)
        kline_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time / 1000))
        try:
            kline_df = db.read(symbol, date_range=DateRange(kline_time, kline_time))
            kline_item = kline_df.iloc[0]
            kline = [
                kline_item['start_time'],
                kline_item['open_price'],
                kline_item['high_price'],
                kline_item['low_price'],
                kline_item['close_price'],
                kline_item['volume'],
                kline_item['end_time'],
                kline_item['amount'],
                kline_item['trade_num'],
                kline_item['positive_volume'],
                kline_item['positive_amount'],
                kline_item['ignore_0'],
            ]
            klines = [kline]
        except:
            klines = []
        return klines

    def _save_klines0(self, interval: str, symbol: str, klines: List[list]):
        db = self._get_database(interval)
        kline_maps = defaultdict(list)
        for kline in klines:
            kline_maps['start_time'].append(kline[0])
            kline_maps['open_price'].append(kline[1])
            kline_maps['high_price'].append(kline[2])
            kline_maps['low_price'].append(kline[3])
            kline_maps['close_price'].append(kline[4])
            kline_maps['volume'].append(kline[5])
            kline_maps['end_time'].append(kline[6])
            kline_maps['amount'].append(kline[7])
            kline_maps['trade_num'].append(kline[8])
            kline_maps['positive_volume'].append(kline[9])
            kline_maps['positive_amount'].append(kline[10])
            kline_maps['ignore_0'].append(kline[11])
        df = pd.DataFrame(data=kline_maps)
        df['index'] = pandas.to_datetime(df['start_time'], unit='ms')
        df.set_index('index', inplace=True, drop=True)

        db.write(symbol, df)

    def _get_database(self, interval: str):
        if interval in self.interval_collection_map:
            return self.interval_collection_map[interval]
        collection_names = self.connection.list_libraries()
        interval_collection_name = f'{self.collection_prefix}-{interval}'
        if interval_collection_name not in collection_names:
            self.connection.initialize_library(interval_collection_name, lib_type=VERSION_STORE)
        collection = self.connection[interval_collection_name]
        self.interval_collection_map[interval] = collection
        return collection


class RedisPusher(Pusher):
    def __init__(self, config: dict):
        super().__init__(config)
        redis_config = self._config['redis']
        redis_pool = ConnectionPool(host=redis_config['host'], port=redis_config['port'],
                                    db=redis_config['db_index'], password=redis_config['password'])
        self._redisc = StrictRedis(connection_pool=redis_pool)

    def _clean(self, interval: str, symbols: List[str], save_days: int):
        now = timestamp()
        with self._redisc.pipeline(transaction=False) as pipeline:
            for symbol in symbols:
                key = get_kline_key_name(self._namespace, interval, symbol)
                pipeline.zremrangebyscore(key, 0, now - (save_days * 1000 * 60 * 60 * 24))
            pipeline.execute()

    def _query_klines_by_start_time(self, interval: str, symbol: str, start_time: int):
        key = get_kline_key_name(self._namespace, interval, symbol)
        return self._redisc.zrangebyscore(key, start_time, start_time)

    def _save_klines0(self, interval: str, symbol: str, klines: List[list]):
        if klines is None or len(klines) <= 0:
            return
        key = get_kline_key_name(self._namespace, interval, symbol)
        kline_score_mapping = {json.dumps(kline): kline[0] for kline in klines}

        with self._redisc.pipeline(transaction=True) as pipeline:
            start_time = min(kline_score_mapping.values())
            end_time = max(kline_score_mapping.values())
            pipeline.zremrangebyscore(key, start_time, end_time)
            pipeline.zadd(key, kline_score_mapping)
            pipeline.execute()
