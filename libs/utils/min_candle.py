# coding: utf-8
#!/usr/bin/python3

import asyncio
from datetime import datetime, timedelta, timezone
import traceback
import pandas as pd
import numpy as np
from pybotters.store import DataStore

class CandleCollector(object):

    def __init__(self, logger):
        self._logger = logger

        self._min_candles = {}         # 取引所の1分足用 DataStore登録用辞書
        self._candle_id = 0            # ターゲット足の DataStoreを作成する際に重複しないようにID
        self._target_candle_list = []  # 生成するターゲットのローソク足リスト

    # ロジック部から呼び出される関数
    def CreateCandleCollectTask(self, exchange, symbol, timeframe, num_of_candle=500, callback=None, args=()):

        # 対象の取引所&シンボルを表すキー
        exchange_symbol = exchange.__class__.__name__+symbol

        # ターゲットの足を入れるDataStoreクラスを作成 (idは重複しない連番で作る）
        id = f'candle{self._candle_id}'
        exchange.create_candle_class(id)
        self._candle_id += 1

        # 生成するターゲットのローソク足データを作成
        candle_dict = {'exchange':exchange,               # 取引所クラス
                       'symbol':symbol,                   # シンボル            (str)
                       'exchange_symbol':exchange_symbol, # 取引所名 + シンボル (str)
                       'timeframe':timeframe*60,          # 作成する足の長さ
                       'pool':[],                         # 生成した足を一時的に入れるリスト
                       'current_candle':0,                # 生成した足の最新足時刻
                       'last_candle':0,                   # 前回ハンドラを呼び出した際の最新足時刻
                       'limit_time':0,                    # 生成後でも残しておく足の時刻（この足以前は変換済みなので削除してOK）
                       'target_candle':exchange._stores.get(id), # 変換後のデータを入れるデータストア
                       'callback':callback,               # イベント発生時に呼び出すコールバック関数
                       'args':args,}

        # ターゲット足のデータストアからpandas形式で取り出すためのクラスを指定しておく
        candle_dict['acces_handler'] = Candle_Access( logger=self._logger, kline=candle_dict['target_candle'], exchange=exchange, symbol=symbol,
                                                      timeframe=timeframe, num_of_candle=num_of_candle, callback=callback)

        # 生成するターゲットのローソク足リストに登録
        self._target_candle_list.append(candle_dict)

        # 初期ローソクの取得
        asyncio.create_task(exchange.get_candles(timeframe, num_of_candle=num_of_candle, symbol=symbol, need_result=False), name="get_initial_candle")

        # ターゲットローソク足イベント呼び出しタスクの起動（ターゲットのローソク足が更新されたら指定されたハンドルを呼び出すタスク）
        asyncio.create_task(self._wait_candle_event(candle_dict), name="wait_update_candle")

        return candle_dict['acces_handler']

    # ターゲットのローソク足が更新されたら登録されたハンドラーを呼びだす
    async def _wait_candle_event(self,task):
        while self._logger.running:
            # ターゲットのローソク足が更新するまで待機
            await task['target_candle'].wait()

            # APIで取得中の場合には終了まで待機
            while task['exchange']._candle_api_lock.locked():
                await asyncio.sleep(1)

            # 最新足が更新されていたら登録されたハンドラーを呼び出す
            if task['last_candle']!=task['current_candle'] :
                task['last_candle']=task['current_candle']
                if task['callback'] and self._logger.running:
                    try:
                        await task['callback'](*task['args'])
                    except Exception:
                        self._logger.error( traceback.format_exc() )

    # 各exchangeクラスから呼び出されて１分足のDataStoreが登録される (引数candle: 1分足のDatastore)
    def set_min_candle(self, exchange_symbol, candle):
        if exchange_symbol not in self._min_candles :
            self._min_candles[exchange_symbol] = {'datastore':candle, 'limit_time':0, 'last_time':0}

    # 最終１分足の時刻を取得
    def get_last_min_candle(self, exchange_symbol):
        return self._min_candles[exchange_symbol]['last_time']

    # 最終１分足の時刻を保管
    def set_last_min_candle(self, exchange_symbol, value):
        self._min_candles[exchange_symbol]['last_time'] = value

    # 指定されたシンボルの分足からリサンプルしてリストに登録されたターゲット足を作る
    def resample(self, symbol=None):
        if not self._target_candle_list : return

        # 分足格納用辞書
        df_min={}

        for candle_store in self._target_candle_list:
            # 生成するように登録されたシンボル以外はスキップ
            if candle_store['symbol']!=(symbol or candle_store['symbol']) : continue

            # 対象の取引所&シンボルを表すキー
            exchange_symbol = candle_store['exchange_symbol']
            exchange_name = candle_store['exchange'].exchange_name

            # まだ分足取得していなかったら分足のDataStoreから取得してpandas形式で df_min へ保存
            if exchange_symbol not in df_min:

                # 分足を取得生成（各取引所クラスで生成された分足ローソクデータが self._min_candles[xxx]]['candle'] に保存されている）
                if exchange_symbol not in self._min_candles : continue

                if exchange_name!='bitget':
                    symbol_key = 'symbol'
                else:
                    symbol_key = 'instId'
                data = self._min_candles[exchange_symbol]['datastore'].find({symbol_key:candle_store['symbol']})
                if not data : continue

                # DataStoreから取得したデータをもとにpandasを生成
                df_min[exchange_symbol] = pd.DataFrame(data)

                # 時刻の変換
                if exchange_name=='bybit':
                    df_min[exchange_symbol]['timestamp'] = pd.to_datetime(df_min[exchange_symbol]['start'], unit='s', utc=True)

                elif exchange_name=='bitget':
                    df_min[exchange_symbol]['timestamp'] = pd.to_datetime(df_min[exchange_symbol]['ts'], unit='ms', utc=True)
                    df_min[exchange_symbol].columns=['symbol','interval','ts','open','high','low','close','volume','timestamp']

                else:
                    df_min[exchange_symbol]['timestamp'] = pd.to_datetime(df_min[exchange_symbol]['timestamp'], unit='s', utc=True)

                df_min[exchange_symbol] = df_min[exchange_symbol].set_index('timestamp').sort_index().groupby(level=0).last()

                self._min_candles[exchange_symbol]['limit_time'] = df_min[exchange_symbol].index[-1].to_pydatetime().timestamp()
                self._min_candles[exchange_symbol]['symbol'] = candle_store['symbol']

            # カラムの指定 (取引所ごとの違い)
            if exchange_name=='bitflyer' :
                agg_param= {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum', 'ask_volume': 'last', 'bid_volume': 'last', 'sell_volume': 'sum', 'buy_volume': 'sum' }
                columns = ['timestamp','open','high','low','close','volume','ask_volume','bid_volume','sell_volume','buy_volume']

            elif exchange_name=='phemex' or exchange_name=='bybit':
                agg_param = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum', 'turnover': 'sum'}
                columns = ['timestamp','open','high','low','close','volume','turnover']

            else:
                self._logger.error( "Unknown exchange : {}".format(exchange_name) )
                continue
            df_min[exchange_symbol] = df_min[exchange_symbol].astype(dict([(c,'float') for c in columns if c!='timestamp']))

            # ターゲットの長さのデータを生成してpoolに入れる (DataStoreへの登録はまだ行わない）
            interval = candle_store['timeframe']
            df = df_min[exchange_symbol].resample(f'{interval}S').agg(agg_param)

            if len(df)!=0:
                df['close'] = df['close'].fillna(method='ffill')
                df['open'] = df['open'].fillna(df['close'])
                df['high'] = df['high'].fillna(df['close'])
                df['low'] = df['low'].fillna(df['close'])
                df['volume'] = df['volume'].fillna(0)
                df=df.dropna()
                if len(df)!=0:
                    df['timestamp'] = df.index.values.astype(np.int64)
                    df['timestamp'] = df['timestamp']/1000000
                    candle_store['pool'].append({'symbol': candle_store['symbol'], 'interval': interval, 'kline': df[columns].values.tolist()[1:]})
                    candle_store['limit_time'] = df.index[max(-2,-len(df))].to_pydatetime().timestamp()
                    candle_store['current_candle'] = df.index[-1].to_pydatetime().timestamp()
                    self._min_candles[exchange_symbol]['limit_time'] = min(self._min_candles[exchange_symbol]['limit_time'],candle_store['limit_time'])
            del df

        # 不要になったDataStoreの1分足を削除
        for exchange_symbol,value in self._min_candles.items():
            if 'symbol' in self._min_candles[exchange_symbol]:
                symbol = self._min_candles[exchange_symbol]['symbol']
                limit_time = self._min_candles[exchange_symbol]['limit_time']
                all_data = self._min_candles[exchange_symbol]['datastore'].find({'symbol':symbol})
                filtered = [d for d in all_data if ('start' in d and d['start']<limit_time)or('timestamp' in d and d['timestamp']<limit_time)]
                self._min_candles[exchange_symbol]['datastore']._delete(filtered)

        # 一時的に作成した1分足のDataFrameを削除
        for d in df_min.values():
            del d

# ターゲット足のデータストアからpandas形式で取り出すためのクラス
class Candle_Access(DataStore):
    def __init__(self, logger, kline, exchange, symbol, timeframe, num_of_candle, callback):
        self._logger = logger
        self._kline = kline
        self._exchange = exchange
        self._symbol = symbol
        self._timeframe = timeframe
        self._num_of_candle = num_of_candle
        self._callback = callback

    def __len__(self):
        return len(self._kline)

    def find(self):
        return self._kline.find()

    async def wait(self):
        return await self._kline.wait()

    # pandasのDataFrameを生成して返す
    @property
    def candle(self):
        if len(self._kline)==0 : return pd.DataFrame()
        data = self._kline.find()
        df = pd.DataFrame(data)
        if 'start' in df.columns: index='start'     # bybit
        else:                     index='timestamp' # phemex/bitflyer/GMO
        df[index] = pd.to_datetime(df[index], unit='s', utc=True)
        result = df.set_index(index).sort_index().groupby(level=0).last().tz_convert(timezone(timedelta(hours=9), 'JST')).tail(self._num_of_candle)
#        self._logger.info( "fetch {}datas to {}candles".format(len(data),len(result)))

        # 不要になった分足を削除
        limit_time = result.index[0].to_pydatetime().timestamp()
        filtered = [d for d in data if ('start' in d and d['start']<limit_time)or('timestamp' in d and d['timestamp']<limit_time)]
#        self._logger.info( [d['timestamp'] for d in filtered] )
        self._kline._delete(filtered)

        return result
