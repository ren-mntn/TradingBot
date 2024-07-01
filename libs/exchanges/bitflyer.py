# coding: utf-8
#!/usr/bin/python3

import asyncio
from datetime import datetime, timedelta, timezone
import time
import traceback
import pandas as pd

import pybotters
from pybotters.store import DataStore

from libs.utils import TimeConv, Scheduler, LockCounter
from libs.market import *
from libs.exchanges.base_module import MultiProc_WS, RestAPIExchange, WebsocketExchange
from libs.account import AccountInfo, OpenPositionFIFO

# https://lightning.bitflyer.com/docs?lang=ja

class Kline(DataStore):
    _KEYS = ['symbol', 'interval', 'timestamp']
    _MAXLEN = 5000000

    def _onmessage(self, message):
        symbol= message.get('symbol')
        interval= message.get('interval')
        self._insert([{'symbol':symbol, 'interval':interval,
                       'timestamp':int(item[0]/1000), 'open':item[1], 'high':item[2], 'low':item[3], 'close':item[4],
                       'volume':item[5], 'ask_volume':item[6], 'bid_volume':item[7], 'sell_volume':item[8], 'buy_volume':item[9]}
                       for item in message.get('kline',[])])

class Bitflyer(pybotters.bitFlyerDataStore, TimeConv, RestAPIExchange, WebsocketExchange):

    # シンボルごとの価格呼び値（BTCなど指定のない物は1）
    _price_unit_dict = {'XRP_JPY':0.01, 'XLM_JPY':0.001, 'MONA_JPY':0.001, 'ETH_BTC':0.00001, 'BCH_BTC':0.00001 }
    def round_order_price(self, price, symbol=None):
        price_unit = self._price_unit_dict.get((symbol or self.symbol),1)
        return round(round(price/price_unit)*price_unit, 8)

    # 最小取引単位（現物や先物やETC,BCHなど指定のない物は0.01）
    _minimum_order_size_dict = {'BTC_JPY':0.001, 'XRP_JPY':0.1, 'XLM_JPY':0.1, 'MONA_JPY':0.1}
    def minimum_order_size(self, symbol=None):
        return self._minimum_order_size_dict.get(symbol or self.symbol, 0.01)

    # 表示フォーマットの指定
    def units(self,value=0):
        return {'unitrate' :1,                          # 損益額をプロフィットグラフに表示する単位に変換する係数
                'profit_currency' : 1,                  # 利益額の単位 0:USD 1:JPY
                'title':"JPY {:+,.0f}".format(value),   # 表示フォーマット
                'pos_rate':1}                           # ポジションをcurrency単位に変換する係数

    def __init__(self, logger, candle_store, apikey=('',''), testnet=False):
        self._logger = logger
        self.candle_store = candle_store   # 分ローソク足変換クラス
        self.auth = (apikey[0]!='' and apikey[1]!='')
        self._apikey = apikey

        self._api_url = ""
        self._endpoint = "wss://ws.lightstream.bitflyer.com/json-rpc"
        self.exchange_name = "bitflyer"

        self._client = pybotters.Client(apis={self.exchange_name: [apikey[0],apikey[1]]})
        self._candle_api_lock = asyncio.Lock()
        self._datastore_lock = LockCounter(self._logger, self.__class__.__name__)
        self._order_dict_lock = LockCounter(self._logger, "order_dict")
        self._candle_subscribed = {}
        super().__init__()
        self.create_candle_class('candle')

        self.PendingUntil = time.time()
        self.RateLimitRemaining = 500
        self.OrderRequestRateLimitRemaining = 300
        self.XRateLimitReset = time.time()
        self.OrderRequestRateLimitReset = time.time()

        RestAPIExchange.__init__(self)
        WebsocketExchange.__init__(self)

        # 定期的にpublucAPIへアクセスしてIPごとのアクセス制限をチェック
        self.server_health='NONE'
        self._logger.call_every1sec.append({'name':'check_server_status', 'handler':self._check_server_status, 'interval':10, 'counter':0})

    # APIリミットの管理
    def _update_private_limit(self,r):
        if r.status != 200 :
            self._logger.error( "{}: {}".format(r.status, r.reason) )
        self.RateLimitRemaining = int(r.headers.get('X-RateLimit-Remaining',self.RateLimitRemaining))
        self.XRateLimitReset = int(r.headers.get('X-RateLimit-Reset',self.XRateLimitReset))
        self.OrderRequestRateLimitRemaining = int(r.headers.get('X-OrderRequest-RateLimit-Remaining',self.OrderRequestRateLimitRemaining))
        self.OrderRequestRateLimitReset = int(r.headers.get('X-OrderRequest-RateLimit-Reset',self.OrderRequestRateLimitReset))

    def _update_public_limit(self,r):
        if r.status != 200 :
            self._logger.error( "{}: {}".format(r.status, r.reason) )
        self.RateLimitRemainingPerIP = int(r.headers.get('X-RateLimit-Remaining',self.RateLimitRemaining))
        self.XRateLimitResetPerIP = int(r.headers.get('X-RateLimit-Reset',self.XRateLimitReset))

    async def _check_server_status(self):
        try:
            res = await self._client.get('https://api.bitflyer.com/v1/gethealth')
            self._update_public_limit(res)
            data = await res.json()
            self.server_health = data.get('status','FAIL')
            self._logger.trace(self.server_health)
        except:
            self.server_health = "FAIL"

    @property
    def api_remain1(self):
        return self.RateLimitRemaining if time.time()<self.XRateLimitReset else 500

    @property
    def api_remain2(self):
        return self.RateLimitRemainingPerIP if time.time()<self.XRateLimitResetPerIP else 500

    @property
    def api_remain3(self):
        return self.OrderRequestRateLimitRemaining if time.time()<self.OrderRequestRateLimitReset else 300

    def create_candle_class(self,id):
        self.create(id, datastore_class=Kline)

    # データストアへの接続を共通名に変換
    @property
    def ticker(self):
        return self._stores.get('ticker')

    @property
    def board(self):
        return self._stores.get('board')

    @property
    def candle(self):
        return self._stores.get('candle')

    @property
    def positions(self):
        return self._stores.get('positions')

    @property
    def orders(self):
        return self._stores.get('childorders')

    # 初期設定
    async def start(self, subscribe={}, symbol='FX_BTC_JPY'):
        self._param = subscribe
        self.symbol = symbol

        # ポジション管理クラス
        self.my = AccountInfo(self._logger, OpenPositionFIFO)

        # データ保存用クラス
        self.board_info = BoardInfo( self._logger )
        self.execution_info = ExecuionInfo( self._logger )
        self.execution_info.spot_last = 0
        self.ticker_info = TickerInfo()
        self.ticker_info.best_ask = 0
        self.ticker_info.best_bid = 0

        # 指定のチャンネルの購読
        self._handler = {'None':None}
        self._ws_args = []
        if self._param.get('execution', True) :
            self._subscribe(topic=f"lightning_executions_{self.symbol}", handler=self._on_executions)
            if self.symbol=='FX_BTC_JPY':  # SFD算出用に現物も購読する
                self._subscribe(topic="lightning_executions_BTC_JPY", handler=self._on_spot_executions)

        if self._param.get('ticker', False) :
            self._subscribe(topic=f"lightning_ticker_{self.symbol}", handler=self._on_ticker)

        if self._param.get('board', False) :
            self._subscribe(topic=f"lightning_board_snapshot_{self.symbol}", handler=self._on_board_snapshot)
            self._subscribe(topic=f"lightning_board_{self.symbol}", handler=self._on_board)

        if self.auth :
            self._subscribe(topic=f"child_order_events", handler=self._on_child_order_events)

        # WebSocketタスクを開始
        options = {} if self.auth else {'auth': None}
        self.ws = MultiProc_WS( logger = self._logger, exchange_name = self.__class__.__name__,
                      handler = self.onmessage,
                      endpoint = self._endpoint,
                      apis = {self.exchange_name: [self._apikey[0],self._apikey[1]]},
                      send_json = self._ws_args,
                      lock = self._datastore_lock,
                      disconnect_handler = self._disconnected,
                      **options )
        asyncio.create_task(self.ws._start_websocket(logging=False), name="bitflyer_websocket")

        # 自動キャンセルタスクを開始
        asyncio.create_task(self._cancel_task(), name="bitflyer_cancel")

        # DataStoreの初期化
        if self.auth :

            while not (self.ws.pid) :
                await asyncio.sleep(1)
            self._logger.info("Websocket connected")

            await self.initialize(
                self._client.get('https://api.bitflyer.com/v1/me/getchildorders', params={'product_code': symbol}),
                self._client.get('https://api.bitflyer.com/v1/me/getparentorders', params={'product_code': symbol}),
                self._client.get('https://api.bitflyer.com/v1/me/getpositions', params={'product_code': symbol}),
            )

        return self

    def _subscribe(self, topic, handler):
        self._logger.debug( "subscribe : {}".format(topic) )
        self._handler[topic]=handler
        self._ws_args.append({"method": "subscribe", "params": {"channel": topic}})

    # メッセージハンドラ
    def _onmessage(self, msg, ws):
        channel = msg.get('params', {}).get('channel','None')

        self._logger.debug("recv bitFlyer websocket [{}]".format(channel))
        self._logger.trace("recv data [{}]".format(msg))

        channel_handler = self._handler.get(channel)
        if channel_handler != None:
            channel_handler(msg.get('params'))

            if channel_handler==self._on_ticker:
                super()._onmessage(msg,ws)

        else:
            super()._onmessage(msg,ws)

    def _on_executions(self, params):
        message = params.get("message")
        self.execution_info.time = self._utcstr_to_dt(message[-1]['exec_date'])
        self.execution_info.append_latency((self._jst_now_dt().timestamp() - self.execution_info.time.timestamp())*1000)
        for i in message:
            self.execution_info.append_execution(i['price'],i['size'],i['side'],self._utcstr_to_dt(i['exec_date']),i["buy_child_order_acceptance_id"]+'='+i["sell_child_order_acceptance_id"])
        self.my.position.ref_ltp = self.execution_info.last
        self.execution_info._event.set()

    def _on_spot_executions(self, params):
        i = params.get("message",[{}])[-1]
        self.execution_info.spot_last = i.get('price',self.execution_info.spot_last)

    @property
    def sfd(self):
        if self.execution_info.last >= self.execution_info.spot_last:
            return round(self.execution_info.last/self.execution_info.spot_last*100-100, 3) if self.execution_info.spot_last != 0 else 0
        else:
            return -round(self.execution_info.spot_last/self.execution_info.last*100-100, 3) if self.execution_info.last != 0 else 0

    def _on_ticker(self,params):
        message = params.get("message")
        self.ticker_info.time = self._utcstr_to_dt(message['timestamp'])
        self.ticker_info.last = message.get('ltp',self.ticker_info.last)
        self.ticker_info.best_ask = message.get('best_ask',self.ticker_info.best_ask)
        self.ticker_info.best_bid = message.get('best_bid',self.ticker_info.best_bid)
        self.my.position.ref_ltp = self.ticker_info.last

    def _on_board_snapshot(self,params):
        self.board_info.initialize_dict()
        self._on_board(params)

    def _on_board(self,params):
        self.board_info.time = self._jst_now_dt()
        message = params.get("message")
        self.board_info.update_asks(message.get('asks',[]))
        self.board_info.update_bids(message.get('bids',[]))
        self.board_info.event.set()
        for event in self.board._events:
            event.set()

    # https://bf-lightning-api.readme.io/docs/realtime-child-order-events
    def _on_child_order_events(self,message):
        """
        {'channel': 'child_order_events', 'message': [{'product_code': 'FX_BTC_JPY', 'child_order_id': 'JFX20210226-070310-350246F', 'child_order_acceptance_id': 'JRF20210226-070300-065874', 'event_date': '2021-02-26T07:04:19.0558047Z', 'event_type': 'EXECUTION', 'exec_id': 2184727760, 'side': 'SELL', 'price': 5119848, 'size': 0.00994883, 'commission': 0, 'sfd': 0, 'outstanding_size': 0.15324826}, {'product_code': 'FX_BTC_JPY', 'child_order_id': 'JFX20210226-070310-350246F', 'child_order_acceptance_id': 'JRF20210226-070300-065874', 'event_date': '2021-02-26T07:04:19.0870571Z', 'event_type': 'EXECUTION', 'exec_id': 2184727761, 'side': 'SELL', 'price': 5119848, 'size': 0.15324826, 'commission': 0, 'sfd': 0, 'outstanding_size': 0}]}
        {'channel': 'child_order_events', 'message': [{'product_code': 'FX_BTC_JPY', 'child_order_id': 'JFX20210226-070433-408889F', 'child_order_acceptance_id': 'JRF20210226-070431-025837', 'event_date': '2021-02-26T07:04:33.9484605Z', 'event_type': 'ORDER', 'child_order_type': 'LIMIT', 'side': 'BUY', 'price': 5108402, 'size': 0.2, 'expire_date': '2021-02-26T07:05:31'}]}
        """
        self._logger.debug("_on_my_orders={}".format(message))

        self._order_dict_lock.wait()
        datas = message.get('message',[])
        for d in datas:
            self._logger.trace( self.__class__.__name__ + " : "+d['event_type']+" : " + str(d) )
            id = d['child_order_acceptance_id']

            if d['event_type']=='ORDER' :
                if self.my.order.update_order( id, side=d['side'], price=d['price'], size=d['size'] ) :
                    self._logger.debug( self.__class__.__name__ + " : ORDER : " + str(d) )

            elif d['event_type']=='EXECUTION' :
                self._logger.debug( self.__class__.__name__ + " : EXECUTION : " + str(d) )
                if self.my.order.executed( id, side=d['side'], price=d['price'], size=d['size'], remain=d['outstanding_size'] ) :
                    self.my.position.executed( id=id, side=d['side'], price=d['price'], size=d['size'], commission=d['sfd'] )

            elif d['event_type']=='EXPIRE' :
                if self.my.order.remove_order( id ) :
                    self._logger.debug( self.__class__.__name__ + " : EXPIRE : " + str(d) )

            elif d['event_type']=='CANCEL' :
                if self.my.order.remove_order( id ) :
                    self._logger.debug( self.__class__.__name__ + " : CANCEL : " + str(d) )

            elif d['event_type']=='CANCEL_FAILED' :
                if self.my.order.is_myorder( id ) :
                    self._logger.debug( self.__class__.__name__ + " : CANCEL_FAILED : " + str(d) )

            elif d['event_type']=='ORDER_FAILED' :
                if self.my.order.is_myorder( id ) :
                    self._logger.debug( self.__class__.__name__ + " : ORDER_FAILED : " + str(d) )
            else:
                self._logger.debug( self.__class__.__name__ + " : UNKNOWN : " + str(d) )

    # APIからのローソク足取得
    async def get_candles( self, timeframe, num_of_candle=500, symbol=None, since=None, need_result=True ):

        if need_result:
            self.create_candle_class('temp_candle')

        target_symbol = symbol or self.symbol

        # 初めて取得するシンボルであれば定期的な読込スケジュールを登録
        if target_symbol not in self._candle_subscribed :
            self._candle_subscribed[target_symbol] = Scheduler(self._logger, interval=60, basetime=0, callback=self.get_latest_candles,args=(target_symbol,))

        await self._candle_api_lock.acquire()

        # 基本の分足を入れるDataStoreクラスをローソク足変換クラスへ登録
        exchange_symbol = self.__class__.__name__+target_symbol
        self.candle_store.set_min_candle(exchange_symbol,self.candle)

        api_timeframe = 60
        timeframe_sec = timeframe*api_timeframe

        endtime = int(datetime.now(tz=timezone.utc).timestamp()//api_timeframe*api_timeframe)
        starttime = since if since else (endtime//timeframe_sec*timeframe_sec)-timeframe_sec*num_of_candle-api_timeframe
        keep_running = True

        while keep_running and starttime<=endtime and num_of_candle!=0:

            self._logger.debug( "starttime: {}".format(datetime.fromtimestamp(starttime,tz=timezone(timedelta(hours=9),'JST'))) )
            self._logger.debug( "endtime  : {}".format(datetime.fromtimestamp(endtime,tz=timezone(timedelta(hours=9),'JST'))) )

            try:
                responce = ''
                lasttime = int(min(starttime//43200*43200+43200 , datetime.now(tz=timezone.utc).timestamp()//api_timeframe*api_timeframe))
                self._logger.debug( "lasttime : {}".format(datetime.fromtimestamp(lasttime,tz=timezone(timedelta(hours=9),'JST'))) )
                res = await self._client.get(f"https://lightchart.bitflyer.com/api/ohlc?symbol={target_symbol}&period=m&before={lasttime}000")
                self._update_public_limit(res)
                responce = await res.json()
                rows = responce[::-1]

                # 得られたデータが空っぽなら終了
                if len(rows)==0 :
                    keep_running = False
                    self._logger.debug( "{} : Fetch candles from API (request before {}) [empty]".format(target_symbol,
                        datetime.fromtimestamp(lasttime,tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M')) )
                    break

                # 得られたデータが指定されたスタート時刻まで含まれていなかったらまだその時刻のローソクはできていないので終了
                if starttime*1000>rows[-1][0] :
                    keep_running = False

                    # 未確定足を追加で取得
                    res = await self._client.get(f"https://lightchart.bitflyer.com/api/ohlc?symbol={target_symbol}&period=m")
                    self._update_public_limit(res)
                    responce = await res.json()
                    if len(responce)!=0 :
                        data = responce[::-1]
                        self._logger.debug( "{} : Fetch additional latest {} candles timeframe{} from API [{}~{}]".format(target_symbol,len(data),api_timeframe,
                            datetime.fromtimestamp(data[0][0]/1000,tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M'),
                            datetime.fromtimestamp(data[-1][0]/1000,tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M')) )
                    self.candle_store.set_last_min_candle(exchange_symbol, data[-1][0]/1000)
                    self.candle._onmessage({'symbol': target_symbol, 'interval': api_timeframe, 'kline': data})

                    break

                data = [c for c in rows if starttime*1000<=int(c[0])<=endtime*1000]
                self._logger.info( "{} : Fetch {} candles timeframe{} from API [{}~{}]".format(target_symbol,len(data),api_timeframe,
                    datetime.fromtimestamp(data[0][0]/1000,tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M'),
                    datetime.fromtimestamp(data[-1][0]/1000,tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M')) )

                self.candle_store.set_last_min_candle(exchange_symbol, data[-1][0]/1000)

                # 分足データストアに保管
                self.candle._onmessage({'symbol': target_symbol, 'interval': api_timeframe, 'kline': data})

                # データ取得用一時ストア
                if need_result:
                    self._stores.get('temp_candle')._onmessage({'symbol': target_symbol, 'interval': timeframe_sec, 'kline': data})

                # 取得した分足が8000本を超えるようなら一度リサンプルを行う
                if len(self.candle)>8000:
                    with self._datastore_lock :
                        self.candle_store.resample(symbol=target_symbol)

                # 次回の取得開始時
                starttime = int(rows[-1][0]/1000+api_timeframe)

                # 得られたデータが目的のendtimeを超えていたら終了
                if endtime*1000 < rows[-1][0]:
                    keep_running = False

            except Exception:
                if responce :
                    self._logger.debug( responce )
                self._logger.error( traceback.format_exc() )
                self._candle_api_lock.release()
                return None

        if not since:
            # 未確定足を追加で取得
            res = await self._client.get(f"https://lightchart.bitflyer.com/api/ohlc?symbol={target_symbol}&period=m")
            self._update_public_limit(res)
            responce = await res.json()
            if len(responce)!=0 :
                data = responce[::-1]
                self._logger.info( "{} : Fetch latest {} candles timeframe{} from API [{}~{}]".format(target_symbol,len(data),api_timeframe,
                    datetime.fromtimestamp(data[0][0]/1000,tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M'),
                    datetime.fromtimestamp(data[-1][0]/1000,tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M')) )
            self.candle_store.set_last_min_candle(exchange_symbol, data[-1][0]/1000)
            self.candle._onmessage({'symbol': target_symbol, 'interval': api_timeframe, 'kline': data})

        # 取得した分足からリサンプルしてターゲットの足を生成
        with self._datastore_lock :
            self.candle_store.resample(symbol=target_symbol)

        # イベントが都度発生しないようにまとめてローソク足のデータストアへ入れる
        for c in self.candle_store._target_candle_list:
            pool = c.pop('pool')
            c['pool']=[]
            for p in pool:
                c['target_candle']._onmessage(p)

        # データを返す必要が無ければ何も生成せずにリターン
        if not need_result:
            self._candle_api_lock.release()
            return

        candles = self._stores.get('temp_candle').find({'symbol':target_symbol,  'interval':timeframe_sec})
        df = pd.DataFrame(candles)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        df = df.set_index('timestamp').sort_index()

        agg_param= {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum', 'ask_volume': 'last', 'bid_volume': 'last', 'sell_volume': 'sum', 'buy_volume': 'sum' }
        df = df.resample(f'{timeframe*60}S').agg(agg_param)
        df['close'].fillna(method='ffill', inplace=True)
        df['open'].fillna(df['close'], inplace=True)
        df['high'].fillna(df['close'], inplace=True)
        df['low'].fillna(df['close'], inplace=True)
        df['ask_volume'].fillna(method='ffill', inplace=True)
        df['bid_volume'].fillna(method='ffill', inplace=True)
        df['volume'].fillna(0, inplace=True)
        df['sell_volume'].fillna(0, inplace=True)
        df['buy_volume'].fillna(0, inplace=True)

        self._candle_api_lock.release()
        return df.tz_convert(timezone(timedelta(hours=9), 'JST')).tail(num_of_candle)

    # 1分に一度読込を行う（更新されるまで）
    async def get_latest_candles(self,target_symbol):
        start = time.time()

        # 基本の分足を入れるDataStoreクラス
        exchange_symbol = self.__class__.__name__+target_symbol
        self.candle_store.set_min_candle(exchange_symbol,self.candle)

        last_time = self.candle_store.get_last_min_candle(exchange_symbol)
        while last_time==self.candle_store.get_last_min_candle(exchange_symbol) and start+55>time.time():
            if time.time()-last_time<100 :
                await self.get_candles(timeframe=1, symbol=target_symbol, num_of_candle=0, need_result=False)
            else:
                await self.get_candles(timeframe=1, symbol=target_symbol, since=last_time, need_result=False)
            await asyncio.sleep(3)

    # APIからTickerを取得
    async def ticker_api(self, **kwrgs):
        try:
            res = await self._client.get('https://api.bitflyer.com/v1/ticker', params={'product_code': kwrgs.get('symbol', self.symbol)})
            self._update_public_limit(res)
            data = await res.json()
            self.my.position.ref_ltp = data['ltp']
            return {'stat':0, 'ltp':data['ltp'], 'msg':data}
        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

    # APIから現在ポジを取得
    async def getpositions(self, **kwrgs):
        try:
            symbol = kwrgs.get('symbol', self.symbol)
            if symbol == 'FX_BTC_JPY' or symbol.startswith('BTCJPY'):
                # 証拠金取引の場合（FXかまたは先物の場合）
                res = await self._client.get('https://api.bitflyer.com/v1/me/getpositions', params={'product_code': symbol})
                self._update_private_limit(res)
                data = await res.json()
                return data
            else:
                # 現物の場合には現物ポジを返す
                res = await self.getbalance()
                for c in res['msg']:
                    if symbol.startswith(c['currency_code']) :
                        return [{'product_code': symbol, 'side': 'BUY', 'size': c['amount'], 'available': c['available']}]
                return []
        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

    # 現物口座の残高 (JPY)
    async def getbalance(self):
        try:
            res = await self._client.get('https://api.bitflyer.com/v1/me/getbalance')
            self._update_private_limit(res)
            data = await res.json()

            balance = 0
            for r in data :
                try:
                    if r['currency_code']=='JPY' :   balance = float(r['amount'])
                except:
                    pass
            return {'stat': 0, 'balance': balance, 'msg': data}

        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

    # 証拠金口座の証拠金額 (JPY)
    async def getcollateral(self, coin=None):
        try:
            if self.symbol == 'FX_BTC_JPY' or self.symbol.startswith('BTCJPY'):
                if coin=='ALL' :
                    r = await self._client.get('https://api.bitflyer.com/v1/me/getcollateralaccounts')
                    self._update_private_limit(r)
                    res = await r.json()
                    collateral = float(res[0]['amount'])+float(res[1]['amount'])*0.5*self.ticker(symbol='BTC_JPY')['ltp']
                    return {'stat': 0, 'collateral': collateral, 'msg': res}
                else:
                    r = await self._client.get('https://api.bitflyer.com/v1/me/getcollateral')
                    self._update_private_limit(r)
                    res = await r.json()
                    return {'stat': 0, 'collateral': res.get('collateral',0), 'msg': res}

            else:
                # 現物の場合には現物送料を返す
                res = await self.getbalance()
                total = 0
                for c in res['msg']:
                    if c['currency_code']=='JPY' :
                        total += c['amount']
                    else:
                        try:
                            total += c['amount']*self.ticker(symbol=c['currency_code']+"_JPY")['ltp']
                        except:
                            pass
                return {'stat': 0, 'collateral': total, 'msg': res}

        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

    # 発注
    async def sendorder(self, order_type, side, size, price=0, auto_cancel_after=2592000, **kwargs ):
        """
        sendorder( order_type='LIMIT',       # 指値注文の場合は "LIMIT", 成行注文の場合は "MARKET" を指定します。
                   side='BUY',               # 買い注文の場合は "BUY", 売り注文の場合は "SELL" を指定します。
                   size=0.01,                # 注文数量を指定します。
                   price=4800000,            # 価格を指定します。order_type に "LIMIT" を指定した場合は必須です。
                   auto_cancel_after,        # 0 以外を指定すると指定秒数経過後キャンセルを発行します
                   product_code="FX_BTC_JPY" # 省略時には起動時に指定した symbol が選択されます
                   time_in_force="GTC",      # 執行数量条件 を "GTC", "IOC", "FOK" のいずれかで指定します。省略した場合の値は "GTC" です。
                   minute_to_expire=1,       # 期限切れまでの時間を分で指定します。省略した場合の値は 43200 (30 日間) です。
                  ) )

        return: { 'stat': エラーコード,            # オーダー成功時 0
                  'msg':  エラーメッセージ,
                  'ids' : オーダーIDのリスト,      # オーダー成功時 [id,]  オーダー失敗時 []
                }
        """

        pos_side = self.my.position.side
        if self.noTrade and (pos_side==side or pos_side=='NONE'):
            self._logger.info("No trade period" )
            return {'stat': -999, 'msg': "No trade period", 'ids': []}

        if self.RateLimitRemaining<50 and time.time()<self.XRateLimitReset:
            self._logger.info("RateLimitRemaining : {}/500".format(self.RateLimitRemaining) )
            return {'stat': -999, 'msg': "RateLimitRemaining "+str(self.RateLimitRemaining)+"/500", 'ids': []}

        if self.OrderRequestRateLimitRemaining<50 and time.time()<self.OrderRequestRateLimitReset:
            self._logger.info("OrderRequestRateLimitRemaining : {}/300".format(self.OrderRequestRateLimitRemaining) )
            return {'stat': -999, 'msg': "OrderRequestRateLimitRemaining "+str(self.OrderRequestRateLimitRemaining)+"/300", 'ids': []}

        if self.PendingUntil > time.time() :
            self._logger.info("Order pending : {:.1f}sec".format(self.PendingUntil-time.time()) )
            return {'stat': -999, 'msg': "Order pending : {:.1f}sec".format(self.PendingUntil-time.time()), 'ids': []}

        adjust_flag = kwargs.get('adjust_flag', False)

        # 対応しているオプションだけにする
        params={}
        for k,v in kwargs.items():
            if k=='time_in_force' :
                if v in ["GTC", "IOC", "FOK"] :
                    params[k]=v
            elif k in ['product_code','minute_to_expire']:
                params[k]=v

        params['product_code']=params.get('product_code', self.symbol)
        params['child_order_type']=order_type
        params['side']=side
        params['size']=round(size,8)
        params['price']=self.round_order_price(price=price,symbol=params['product_code'])

        # ノートレード期間はポジ以上のクローズは行わない
        if self.noTrade :
            size = min(abs(self.my.position.size),size)
            self._logger.info("No trade period (change order size to {})".format(params['size']) )

        # 最低取引量のチェック（無駄なAPIを叩かないように事前にチェック）
        if params['size']<self.minimum_order_size(symbol=params['product_code']) :
            return {'stat': -153, 'msg': '最低取引数量を満たしていません', 'ids': []}

        self._logger.debug("[sendorder] : {}".format(params) )

        with self._order_dict_lock :
            try :
                r = await self._client.post('https://api.bitflyer.com/v1/me/sendchildorder', data=params)
                self._update_private_limit(r)
                res = await r.json()
            except Exception as e:
                self._logger.error(traceback.format_exc())
                return {'stat': -999, 'msg': str(e), 'ids': []}

            if res and "JRF" in str(res) : 
                if self.my and not adjust_flag:
                    invalidate = min(auto_cancel_after, params.get('minute_to_expire',43200)*60)+120
                    self.my.order.new_order( symbol=params['product_code'], id=res['child_order_acceptance_id'] ,
                                             side=side, price=price, size=params['size'], expire=time.time()+auto_cancel_after, invalidate=time.time()+invalidate )
                return {'stat':0 , 'msg':"", 'ids':[res['child_order_acceptance_id']]}
            else: 
                # オーダー失敗
                self._logger.error("Send order param : {}".format(params) )
                self._logger.error("Error response [sendorder] : {}".format(res) )
                return {'stat':res.get('status') , 'msg':res.get('error_message'), 'ids':[]}

    # キャンセル
    async def cancelorder(self, id, **kwargs ):
        """
        cancelorder( id                          # キャンセルする注文の ID です。
               #-------------以下 bitFlyer独自パラメータ
                   product_code="FX_BTC_JPY"     # 省略時には起動時に指定した symbol が選択されます
                    )

        return: { 'stat': エラーコード,      # キャンセル成功時 0
                  'msg':  エラーメッセージ,
                }
        """

#        if id not in self.my.order.order_dict :
#            self._logger.info("ID:{} is already filled or canceld or expired".format(id) )
#            return {'stat':-999 , 'msg':"Order is already filled or canceld or expired"}

        if self.api_remain1<50 :
            self._logger.info("LimitRemaining : {}".format(self.api_remain1) )
            return {'stat': -999, 'msg': 'LimitRemaining(api_remain1) '+str(self.api_remain1)+'/500'}

        if self.api_remain3<50 :
            self._logger.info("OrderLimitRemaining(api_remain3) : {}/500".format(self.api_remain3) )
            return {'stat': -999, 'msg': "LimitRemaining(api_remain3) "+str(self.api_remain3)+"/300"}

        kwargs['child_order_acceptance_id']=id

        self._logger.debug("[cancelorder] : {}".format(kwargs) )
        kwargs['product_code']=kwargs.get("product_code", self.my.order.order_dict[id]['symbol'] )

        self.my.order.mark_as_invalidate( id )

        try :
            r = await self._client.post('https://api.bitflyer.com/v1/me/cancelchildorder', data=kwargs)
            self._update_private_limit(r)
            res = await r.text()
        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e), 'ids': []}

        if res :
            res = await r.json()
            self._logger.error("Error response [cancelorder] : {}".format(res) )
            return {'stat':res.get('status') , 'msg':res.get('error_message')}

        return {'stat': 0, 'msg': ""}
