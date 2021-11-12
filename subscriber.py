import _thread
import json
from collections import defaultdict
from threading import Lock
from typing import List, Dict

from websocket import WebSocketApp

from utils import timestamp


class SubscriberSymbolsBody(object):
    def __init__(self, interval_symbols_map: Dict[str, List[str]]):
        self.interval_symbols_map = interval_symbols_map


class KlineBuffer(object):
    def __init__(self, kline):
        self.lock = Lock()
        self.kline = kline
        self.last_save_time = None


class KlineFetchWebSocketSubscriber(object):
    interval_symbol_kline_buffer_map: Dict[str, Dict[str, KlineBuffer]] = defaultdict(dict)

    def __init__(self, host: str, symbols_body: SubscriberSymbolsBody,
                 kline_from_start_time_supplier, kline_setter,
                 with_start=None, save_buffer_millseconds: int = 1000 * 10):
        """
        :param host: websocket host
        :param symbols_body: subscribe config
        :param kline_from_start_time_supplier: (interval: str, symbol: str, start_time: int), query klines
        :param kline_setter: (interval: str, symbol: str, klines)
        :param with_start: function with on start
        :param save_buffer_millseconds: the buffer's scope
        """
        self.host = host
        self._ws = WebSocketApp(self.host, on_open=self._on_open, on_close=self._on_close, on_error=self._on_error,
                                on_message=self._on_message)
        self._with_start = with_start
        self.save_buffer_millseconds = save_buffer_millseconds
        self._symbols_body = symbols_body
        self._interval_symbols_map = self._symbols_body.interval_symbols_map
        self._kline_from_start_time_supplier = kline_from_start_time_supplier
        self._kline_setter = kline_setter
        self._subscribe_params = []
        for interval, symbols in self._interval_symbols_map.items():
            for symbol in symbols:
                subscribe_key = f'{symbol.lower()}@kline_{interval}'
                self._subscribe_params.append(subscribe_key)
                self.interval_symbol_kline_buffer_map[interval][symbol] = KlineBuffer(None)

    def _with_start0(self):
        if self._with_start is not None:
            try:
                self._with_start(self._symbols_body)
            except Exception as e:
                self._on_error(self._ws, e)

    def start(self):
        _thread.start_new_thread(self._with_start0, ())
        self._ws.run_forever(ping_interval=15)

    def _restart(self):
        self.start()

    def _on_open(self, ws: WebSocketApp):
        print(f'websocket connection opened, klines: {self._subscribe_params} subscribing...')
        subscribe_data = {
            "method": "SUBSCRIBE",
            "params": self._subscribe_params,
            "id": 1
        }
        ws.send(json.dumps(subscribe_data))
        print(f'klines: {self._subscribe_params} subscribe success.')

    def _on_close(self, ws: WebSocketApp):
        print(f'subscriber: {self._subscribe_params} closed.')

    def _on_error(self, ws: WebSocketApp, error):
        print(f'subscriber: {self._subscribe_params} error occured: {error}, restart.')
        try:
            ws.close()
        except Exception as e:
            print(f'close websocket error, {e}')
        self._ws = WebSocketApp(self.host, on_open=self._on_open, on_close=self._on_close, on_error=self._on_error,
                                on_message=self._on_message)
        self._restart()

    def _on_message(self, ws: WebSocketApp, message):
        fire_message = False
        try:
            body = None
            if type(message) is str:
                body = json.loads(message)
            if type(body) is not dict:
                fire_message = True
        except Exception as e:
            print(f'message: {message} load failed: {e}, return')
            return
        if 'e' not in body or 'kline' != body['e']:
            fire_message = True
        if fire_message:
            print(f'kline webSocket message not match, fire. message: {message}')
            return
        symbol = body['s']
        kline_time = body['E']
        kline_info = body['k']
        kline_start_time = kline_info['t']
        kline_end_time = min(int(kline_time), kline_info['T'])
        interval = kline_info['i']

        kline = [
          kline_start_time,   # kline start time
          kline_info['o'],    # kline open price
          kline_info['h'],    # kline high price
          kline_info['l'],    # kline low price
          kline_info['c'],    # kline close price
          kline_info['v'],    # kline volume
          kline_end_time,     # kline end time
          kline_info['q'],    # kline deal amount
          kline_info['n'],    # kline deal count
          kline_info['V'],    # kline positive deal count
          kline_info['Q'],    # kline positive deal amount
          "0",
          kline_time
        ]

        save_klines = []
        if self.save_buffer_millseconds is not None and self.save_buffer_millseconds > 0:
            kline_buffer = self.interval_symbol_kline_buffer_map[interval][symbol]
            now = timestamp()
            with kline_buffer.lock:
                if kline_buffer.last_save_time is None:
                    save_klines.append(kline)
                else:
                    if kline_buffer.kline is None:
                        kline_buffer.kline = kline
                        return
                    else:
                        if kline_buffer.kline[0] == kline[0]:
                            if kline[-1] > kline_buffer.kline[-1]:
                                kline_buffer.kline = kline
                            if kline_buffer.kline[-1] - kline_buffer.last_save_time > self.save_buffer_millseconds:
                                save_klines.append(kline_buffer.kline)
                                kline_buffer.kline = None
                        elif kline_buffer.kline[0] < kline[0]:
                            save_klines.append(kline_buffer.kline)
                            save_klines.append(kline)
                            kline_buffer.kline = None
                if len(save_klines) <= 0:
                    return
                else:
                    kline_buffer.last_save_time = now
        else:
            save_klines.append(kline)
        save_klines = [save_kline[:-1] for save_kline in save_klines]
        self._kline_setter(interval, symbol, save_klines)
