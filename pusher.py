import _thread
import math
import os
import threading

import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import List

import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler
from dbutils.pooled_db import PooledDB
from redis import StrictRedis, ConnectionPool

from subscriber import SubscriberSymbolsBody, KlineFetchWebSocketSubscriber
from utils import timestamp, get_kline_key_name
from config import klines_web_fetch_worker, timezone, save_buffer_millseconds
from fetcher import CcxtFetcher
import sqlite3

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
        now = timestamp()
        start_time = 0
        end_time = now - (save_days * 1000 * 60 * 60 * 24)
        self._clean0(interval, symbols, start_time, end_time)

    def _clean0(self, interval: str, symbols: List[str], start_time: int, end_time: int):
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


class MySQLPusher(Pusher):
    def __init__(self, config: dict):
        super().__init__(config)
        mysql_config = self._config['mysql']
        self.host = mysql_config['host']
        self.port = mysql_config['port']
        self.username = mysql_config['username']
        self.password = mysql_config['password']
        self.database = mysql_config['database']
        self.insert_batch_size = mysql_config['insert_batch_size']
        init_conn = pymysql.connect(host=self.host, user=self.username, password=self.password, charset='utf8mb4')
        init_conn.ping(reconnect=True)
        self.available_tables = set()
        database_create_sql = f'CREATE DATABASE IF NOT EXISTS {self.database}'
        cursor = init_conn.cursor()
        cursor.execute(database_create_sql)
        init_conn.close()
        self.connection_pool = PooledDB(
            creator=pymysql,
            maxconnections=40,
            mincached=2,
            maxcached=10,
            blocking=True,
            setsession=[],
            ping=5,
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.database,
            charset='utf8mb4')

    def _get_connection(self):
        return self.connection_pool.connection()

    def _clean0(self, interval: str, symbols: List[str], start_time: int, end_time: int):
        connection = self._get_connection()
        for symbol in symbols:
            table_name = self._generate_table_name(symbol, interval)
            cursor = connection.cursor()
            cursor.execute(
                f'''
                delete from {table_name} where start_time between {start_time} and {end_time};
                '''
            )
            connection.commit()

    def _query_klines_by_start_time(self, interval: str, symbol: str, start_time: int):
        connection = self._get_connection()
        klines = []
        table_name = self._generate_table_name(symbol, interval)
        cursor = connection.cursor()
        cursor.execute(
            f'''
            select id, start_time, open_price, high_price, low_price, close_price, 
            volume, end_time, amount, trade_num, positive_volume, positive_amount, ignore_0 from {table_name}
            where start_time = {start_time} order by end_time asc 
            '''
        )
        connection.commit()
        for row in cursor:
            kline = [
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[9],
                row[10],
                row[11],
                row[12],
            ]
            klines.append(kline)
        return klines

    def _save_klines0(self, interval: str, symbol: str, klines: List[list]):
        connection = self._get_connection()
        batchs = []
        batch_count = math.ceil(len(klines) / self.insert_batch_size)
        table_name = self._generate_table_name(symbol, interval)
        for i in range(batch_count):
            batchs.append([])
        for kline_index in range(len(klines)):
            kline = klines[kline_index]
            batch = batchs[kline_index % len(batchs)]
            batch.append(kline)
        for batch in batchs:
            if len(batch) <= 0:
                continue
            insert_sql_prefix = f'\
                            insert into {table_name} (start_time, open_price, high_price, low_price, close_price, \
                            volume, end_time, amount, trade_num, positive_volume, positive_amount, ignore_0) values \
                            '
            for kline in batch:
                insert_sql_prefix = insert_sql_prefix + f'\
                                ({kline[0]}, {kline[1]}, {kline[2]}, {kline[3]}, {kline[4]}, {kline[5]}, {kline[6]},\
                                 {kline[7]}, {kline[8]}, {kline[9]}, {kline[10]}, "{kline[11]}"), '
            insert_sql = insert_sql_prefix[:-2]

            kline_start_time_mapping = {json.dumps(kline): kline[0] for kline in klines}
            start_time = min(kline_start_time_mapping.values())
            end_time = max(kline_start_time_mapping.values())
            delete_sql = f'delete from {table_name} where start_time >= {start_time} and end_time <= {end_time}'

            cursor = connection.cursor()
            cursor.execute(delete_sql)
            cursor.execute(insert_sql)
            connection.commit()

    def _create_table(self, table_name: str):
        connection = self._get_connection()
        cursor = connection.cursor()
        cursor.execute(
            f'''
            create table if not exists {table_name}
            (
                id              int auto_increment,
                start_time      bigint       not null,
                open_price      varchar(255) not null,
                high_price      varchar(255) not null,
                low_price       varchar(255) not null,
                close_price     varchar(255) not null,
                volume          varchar(255) not null,
                end_time        bigint       not null,
                amount          varchar(255) not null,
                trade_num       int          not null,
                positive_volume varchar(255) null,
                positive_amount varchar(255) not null,
                ignore_0        text         null,
                constraint symbol_interval_pk
                    primary key (id)
            );
            '''
        )
        cursor.execute(
            f'''
            create unique index symbol_interval_end_time_index
                on {table_name} (end_time);
            '''
        )
        cursor.execute(
            f'''
            create unique index symbol_interval_start_time_index
                on {table_name} (start_time);
            '''
        )
        connection.commit()
        self.available_tables.add(table_name)

    def _table_ensure(self, table_name: str):
        if table_name in self.available_tables:
            return
        connection = self._get_connection()
        cursor = connection.cursor()
        cursor.execute(
            f'''
            select count(*)
            from information_schema.TABLES 
            where TABLE_NAME = '{table_name}';
            '''
        )
        connection.commit()
        count = 0
        rows = cursor.fetchall()
        for row in rows:
            count = row[0]
            break

        if count > 0:
            return
        self._create_table(table_name)

    def _generate_table_name(self, symbol: str, interval: str):
        table_name = f'{symbol}_{interval}'
        self._table_ensure(table_name)
        return table_name


class SqlitePusher(Pusher):
    table_name = 'kline'
    insert_batch_size = 1000
    sqlite_connection_map = defaultdict(dict)
    conn_lock_map = defaultdict(dict)
    operate_lock_map = defaultdict(dict)

    def __init__(self, config: dict):
        super().__init__(config)
        sqlite_config = self._config['sqlite']
        self.sqlite_dir = sqlite_config['dir']

    def _clean0(self, interval: str, symbols: List[str], start_time: int, end_time: int):
        for symbol in symbols:
            conn = self._get_connection(interval, symbol)
            cursor = conn.cursor()
            cursor.execute(
                f'''
                delete from {self.table_name} where start_time between {start_time} and {end_time};
                '''
            )
            conn.commit()

    def _query_klines_by_start_time(self, interval: str, symbol: str, start_time: int):
        klines = []
        conn = self._get_connection(interval, symbol)
        cursor = conn.cursor()
        cursor.execute(
            f'''
                    select id, start_time, open_price, high_price, low_price, close_price, 
                    volume, end_time, amount, trade_num, positive_volume, positive_amount, ignore_0 from {self.table_name}
                    where start_time = {start_time} order by end_time asc 
                    '''
        )
        conn.commit()
        for row in cursor:
            kline = [
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[9],
                row[10],
                row[11],
                row[12],
            ]
            klines.append(kline)
        return klines

    def _save_klines0(self, interval: str, symbol: str, klines: List[list]):
        batchs = []
        batch_count = math.ceil(len(klines) / self.insert_batch_size)
        for i in range(batch_count):
            batchs.append([])
        for kline_index in range(len(klines)):
            kline = klines[kline_index]
            batch = batchs[kline_index % len(batchs)]
            batch.append(kline)
        for batch in batchs:
            if len(batch) <= 0:
                continue
            insert_sql_prefix = f'\
                    insert into {self.table_name} (start_time, open_price, high_price, low_price, close_price, \
                    volume, end_time, amount, trade_num, positive_volume, positive_amount, ignore_0) values \
                    '
            for kline in batch:
                insert_sql_prefix = insert_sql_prefix + f'\
                        ({kline[0]}, {kline[1]}, {kline[2]}, {kline[3]}, {kline[4]}, {kline[5]}, {kline[6]},\
                         {kline[7]}, {kline[8]}, {kline[9]}, {kline[10]}, "{kline[11]}"), '
            insert_sql = insert_sql_prefix[:-2]

            kline_start_time_mapping = {json.dumps(kline): kline[0] for kline in klines}
            start_time = min(kline_start_time_mapping.values())
            end_time = max(kline_start_time_mapping.values())
            delete_sql = f'delete from {self.table_name} where start_time <= {start_time} and {end_time} <= end_time'

            conn = self._get_connection(interval, symbol)
            cursor = conn.cursor()
            cursor.execute(delete_sql)
            cursor.execute(insert_sql)
            conn.commit()

    def _table_ensure(self, connection):
        cursor = connection.cursor()
        cursor.execute(
            f'''
            SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = '{self.table_name}' 
            '''
        )
        connection.commit()
        count = 0
        for row in cursor:
            count = row[0]
            break

        if count > 0:
            return
        self._create_table(connection)

    def _create_table(self, connection):
        cursor = connection.cursor()
        cursor.executescript(
            f'''
            create table "{self.table_name}"
            (
                id integer not null
                    constraint symbol_pk
                        primary key autoincrement,
                start_time integer not null,
                open_price real not null,
                high_price real not null,
                low_price real not null,
                close_price real not null,
                volume real not null,
                end_time integer not null,
                amount real not null,
                trade_num integer not null,
                positive_volume real not null,
                positive_amount real not null,
                ignore_0 text
            );

            create index end_time_index
                on "{self.table_name}" (end_time);

            create index start_time_index
                on "{self.table_name}" (start_time);
            '''
        )
        connection.commit()

    def _get_connection(self, interval, symbol):
        interval_map = self.sqlite_connection_map[interval]
        current_thread = threading.current_thread()
        if symbol not in interval_map:
            interval_map[symbol] = {}

        thread_conn_map = interval_map[symbol]
        if current_thread in thread_conn_map:
            return thread_conn_map[current_thread]
        else:
            sqlite_path = os.path.join(self.sqlite_dir, self._get_sqlite_filename(interval, symbol))
            conn = sqlite3.connect(sqlite_path)
            self._table_ensure(conn)
            thread_conn_map[current_thread] = conn
            return conn

    @staticmethod
    def _get_sqlite_filename(interval: str, symbol: str):
        return f'{symbol}-{interval}.db'


class MongoPusher(Pusher):
    def __init__(self, config: dict):
        super().__init__(config)

    def _clean0(self, interval: str, symbols: List[str], start_time: int, end_time: int):
        super()._clean0(interval, symbols, start_time, end_time)

    def _query_klines_by_start_time(self, interval: str, symbol: str, start_time: int):
        super()._query_klines_by_start_time(interval, symbol, start_time)

    def _save_klines0(self, interval: str, symbol: str, klines: List[list]):
        super()._save_klines0(interval, symbol, klines)


class DolphinPusher(Pusher):
    def __init__(self, config: dict):
        super().__init__(config)

    def _clean0(self, interval: str, symbols: List[str], start_time: int, end_time: int):
        super()._clean0(interval, symbols, start_time, end_time)

    def _query_klines_by_start_time(self, interval: str, symbol: str, start_time: int):
        super()._query_klines_by_start_time(interval, symbol, start_time)

    def _save_klines0(self, interval: str, symbol: str, klines: List[list]):
        super()._save_klines0(interval, symbol, klines)


class RedisPusher(Pusher):
    def __init__(self, config: dict):
        super().__init__(config)
        redis_config = self._config['redis']
        redis_pool = ConnectionPool(host=redis_config['host'], port=redis_config['port'],
                                    db=redis_config['db_index'], password=redis_config['password'])
        self._redisc = StrictRedis(connection_pool=redis_pool)

    def _clean0(self, interval: str, symbols: List[str], start_time: int, end_time: int):
        with self._redisc.pipeline(transaction=False) as pipeline:
            for symbol in symbols:
                key = get_kline_key_name(self._namespace, interval, symbol)
                pipeline.zremrangebyscore(key, start_time, end_time)
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
