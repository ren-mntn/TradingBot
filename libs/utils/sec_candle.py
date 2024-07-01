# coding: utf-8
#!/usr/bin/python3

import asyncio
import time
from collections import deque
from threading import Thread, Event
from datetime import datetime, timedelta, timezone
import pandas as pd
import traceback
import numpy as np
pd.set_option('display.expand_frame_repr', False)

from libs.database import auth_db, candle_db

class CandleGenerator(object):

    def __init__( self,logger, exchange, timescale, num_of_candle=500, update_current=False, callback=None, args=(), server=('',)  ):
        self._logger = logger
        self._exchange = exchange
        self._timescale = timescale
        self._num_of_candle = num_of_candle
        self._callback = callback
        self._args = args
        self._update_current = update_current
        self._server = server

        # 約定をためるキュー
        self._execution_buffer = deque(maxlen=timescale*10000)
        self._executions = deque(maxlen=timescale*10000)

        # ローソク足データ
        self._candle = pd.DataFrame(index=[], columns=['date', 'open', 'high', 'low', 'close', 'volume', 'buy_volume', 'sell_volume',
                                                      'count', 'buy_count', 'sell_count', 'value', 'buy_value', 'sell_value']).set_index('date')
        self._current_ohlc = {'date':0, 'open': 0, 'high': 0, 'low': 0, 'close': 0, 'volume': 0, 'buy_volume': 0, 'sell_volume': 0,
                              'count': 0, 'buy_count': 0, 'sell_count': 0, 'value': 0, 'buy_value': 0, 'sell_value': 0}

        # ローソク足サーバーが指定されていれば取得を試みる
        self._db_candle = []
        if self.use_server :
            self._connected = False
            # 初回の取得タイミングはローソク足が3本溜まってからになるように調整
            self._last_fetch_time = time.time()-30+timescale*3
        else:
            self._connected = True

        self._lastcandle = datetime.now(timezone(timedelta(hours=9), 'JST'))-timedelta(minutes=10)
        self._last_conveted = 0

        self._candle_generate = asyncio.Event()

        asyncio.create_task(self._loop(), name=f"sec_candle_generator_{exchange.exchange_name}")

        # 10秒ごとに実行
        self._logger.call_every1sec.append({'name':'reduce_execution_buffer', 'handler':self.reduce_execution_buffer, 'interval':10, 'counter':0})

        self._exchange.execution_info.add_handler( exec_que=self._execution_buffer, handler=self._on_execution )

    @property
    def use_server(self) :
        return self._server[0] and self._server[0]!=''

    def _update_current_candle( self, i ):

        # 現在足のデータ生成
        price = float(i['price'])
        size = i['size']
        c = self._current_ohlc
        if c['open'] == 0:
            c['open'] = c['high'] = c['low'] = price
        c['high'] = max(price, c['high'])
        c['low'] = min(price, c['low'])
        c['close'] = price
        c['volume'] += size
        c['count'] += 1
        c['value'] += price * size

        if i['side'] == 'BUY':
            c['buy_volume'] += size
            c['buy_count'] += 1
            c['buy_value'] += price * size
        else:
            c['sell_volume'] += size
            c['sell_count'] += 1
            c['sell_value'] += price * size

    async def _loop(self):
        while self._logger.running:
            await self._candle_generate.wait()
            self._candle_generate.clear()
            await self._updatecandle()

    async def _on_execution( self ):
        while len(self._execution_buffer)>0:
            i = self._execution_buffer.popleft()

            self._executions.append(i)
            if self._update_current :
                self._update_current_candle(i)

        # 送られてきたexecutionの時間��前回ローソク足更新時の最後の足よりもtimescale進んでいればローソク足の更新作業を行う
        if self._executions[-1]['exec_date'].timestamp() - self._lastcandle.timestamp() >= self._timescale:
            self._candle_generate.set()

    # executionリストをもとに指定秒足のローソクを生成
    async def _updatecandle(self):
        try:
            tmpExecutions = list(self._executions)
            self.raw = pd.DataFrame([[
                    tick['exec_date'], tick['price'],
                    tick['size'],
                    tick['size']if tick['side'] == 'BUY'else 0,
                    tick['size']if tick['side'] == 'SELL'else 0,
                    1 if tick['size'] != 0 else 0,
                    1 if tick['side'] == 'BUY' else 0,
                    1 if tick['side'] == 'SELL' else 0,
                    tick['price'] * tick['size'],
                    tick['price'] * tick['size'] if tick['side'] == 'BUY' else 0,
                    tick['price'] * tick['size'] if tick['side'] == 'SELL' else 0
                    ] for tick in tmpExecutions],
                    columns=['date', 'price', 'volume', 'buy_volume', 'sell_volume', 'count', 'buy_count', 'sell_count',
                             'value', 'buy_value', 'sell_value'])
            tmpcandle = self.raw.set_index('date').resample(str(self._timescale)+'s').agg({
                    'price': 'ohlc', 'volume': 'sum', 'buy_volume': 'sum', 'sell_volume': 'sum',
                     'count': 'sum', 'buy_count': 'sum', 'sell_count': 'sum',
                    'value': 'sum', 'buy_value': 'sum', 'sell_value': 'sum'})
            tmpcandle.columns = tmpcandle.columns.droplevel()
            self._logger.debug( "tmpcandle\n{}".format(tmpcandle) )

            if len(self._candle)<2 :
                self._candle = tmpcandle
            else:
                # 前回変換済みのところを検索
                last_index = np.where(tmpcandle.index.values>=self._last_conveted)[0][0]-len(tmpcandle) if self._last_conveted!=0 else -len(tmpcandle)
                self._candle = pd.concat([self._candle[:-1], tmpcandle[last_index:]]).sort_index().groupby(level=0).last()
            self._last_conveted = self._candle.tail(2).index.values[0]
            self._logger.debug( "self._last_conveted : {}".format(self._last_conveted) )

            self._candle['close'] = self._candle['close'].ffill()
            self._candle['open'] = self._candle['open'].fillna(self._candle['close'])
            self._candle['high'] = self._candle['high'].fillna(self._candle['close'])
            self._candle['low'] = self._candle['low'].fillna(self._candle['close'])

            # 必要な本数だけにカット
            self._candle = self._candle.tail(self._num_of_candle+1)

            # 現在足データの更新
            if self._update_current :
                self._current_ohlc = {'date':self._candle.index[-1],
                                      'open': self._candle['open'][-1],
                                      'high': self._candle['high'][-1],
                                      'low': self._candle['low'][-1],
                                      'close': self._candle['close'][-1],
                                      'volume': self._candle['volume'][-1],
                                      'buy_volume': self._candle['buy_volume'][-1],
                                      'sell_volume': self._candle['sell_volume'][-1],
                                      'count': self._candle['count'][-1],
                                      'buy_count': self._candle['buy_count'][-1],
                                      'sell_count': self._candle['sell_count'][-1],
                                      'value': self._candle['value'][-1],
                                      'buy_value': self._candle['buy_value'][-1],
                                      'sell_value': self._candle['sell_value'][-1] }

            if self.use_server and not self._connected :
                self._logger.info( "{} {} candles : {}, {}".format(self._exchange.exchange_name, self._exchange.symbol, len(self.candle),len(self._candle)) )

            if self._lastcandle!=self._candle.index[-1] and self._callback!=None and self._connected and self._logger.running:
                # ローソク足更新時のロジックを呼び出す
                await self._callback(*self._args)

            self._lastcandle = self._candle.index[-1]

        except Exception as e:
            self._logger.error("Error occured at _updatecandle: {}".format(e))
            self._logger.info(traceback.format_exc())

    # 負荷軽減のため、ローソク足に変換済みの約定履歴を破棄
    async def reduce_execution_buffer(self):
        if len(self._executions) == 0: return
        if self._last_conveted == 0:  return

        while len(self._executions)>0:
            i = self._executions.popleft()
            if int(self._last_conveted)/1000000000 <= i['exec_date'].timestamp():
                self._executions.appendleft(i)
                break

    # 確定��ーソク足の時刻
    @property
    def date(self):
        return self._candle.index[-2] if len(self._candle)>1 else 0

    @property
    def candle(self):
        if self.use_server :
            if not self._connected :

                # ローソク足サーバーからの取得は30秒に1回
                if self._last_fetch_time+30 < time.time() :
                    try:
                        self.fetch_from_candle_server(self._timescale, self._num_of_candle )
                    except Exception as e:
                        self._logger.error("Error occured at fetch_from_candle_server: {}".format(e))

                    self._last_fetch_time = time.time()

                # ローソク足が溜まるまではなにもしない
                if len(self._candle)<3 or len(self._db_candle)==0 :
                    return self._db_candle

                hit = np.where(self._db_candle.index.values[-1]>=self._candle.index.values)

                # まだ接合できない
                if len(hit[0])==0 :
                    return self._db_candle

                self._candle = pd.concat([self._db_candle[:-1],self._candle[hit[0][-1]:]]).groupby(level=0).last()
                self._connected = True

        if not self._update_current :
            return self._candle

        # 最後に未確定ローソクを追加
        current_candle = pd.DataFrame.from_dict(self._current_ohlc, orient='index').T.set_index('date')
        candle = pd.concat([self._candle[:-1],current_candle])
        candle[['open','high','low','close']] = candle[['open','high','low','close']].applymap("{:.1f}".format)
        return candle

    @property
    def num_of_candle(self):
        return self._num_of_candle

    @num_of_candle.setter
    def num_of_candle(self, value):
        self._num_of_candle = value

    def fetch_from_candle_server( self, timescale, num_of_candle ):

        host=self._server[0]
        key = auth_db(host).generate_key(username=self._server[1], password=self._server[2])
        db = candle_db( host, key )

        # 取引所とシンボルを検索
        exchange_list = db.query_exchanges()
        matched_exchange = [e for e in exchange_list if e.lower()==self._exchange.exchange_name.lower()]
        if len(matched_exchange)==0 :
            self._server=('','','')
            self._connected = True
            raise ValueError("Can't fetch candle data for "+self._exchange.exchange_name)
        exchange_name = matched_exchange[0]
        symbols = db.query_symbols( exchange_name )
        matched_symbol = [s for s in symbols if s.lower()==self._exchange.symbol.lower()]
        if len(matched_symbol)==0 :
            self._server=('','','')
            self._connected = True
            raise ValueError("Can't fetch candle data for "+self._exchange.symbol)
        symbol_name = matched_symbol[0]

        try:
            # ローソク足取得
            self._db_candle = db.query_candles( exchange=exchange_name, symbol=symbol_name, timescale=timescale, num_of_candle=num_of_candle )
        except Exception as e:
            self._server=('','','')
            self._connected = True
            self._logger.info(traceback.format_exc())
            return

        self._logger.info( "fetch {} candles from {}[{},{}]".format(len(self._db_candle),host,self._exchange.exchange_name,self._exchange.symbol) )
