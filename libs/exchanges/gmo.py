# coding: utf-8
#!/usr/bin/python3

import asyncio
from collections import deque
from datetime import datetime, timedelta, timezone
import time
import traceback
import pandas as pd

import pybotters
from pybotters.store import DataStore

from libs.utils import TimeConv, Scheduler, LockCounter
from libs.market import *
from libs.exchanges.base_module import MultiProc_WS, RestAPIExchange, WebsocketExchange
from libs.account import AccountInfo, OpenPositionGross, OpenPositionKeepAveLinear

# https://api.coin.z.com/docs/#private-api
# https://api.coin.z.com/docs/#references

class Kline(DataStore):
    _KEYS = ['symbol', 'interval', 'timestamp']
    _MAXLEN = 1000000

    def _onmessage(self, message):
        symbol= message.get('symbol')
        interval= message.get('interval')
        self._insert([{'symbol':symbol, 'interval':interval,
                       'timestamp':int(item[0]/1000), 'open':float(item[1]), 'high':float(item[2]), 'low':float(item[3]), 'close':float(item[4]), 'volume':float(item[5])}
                       for item in message.get('kline',[])])

class DoubleConnection(object):
    def __init__(self, logger, generate_token, connect_to_private, auth=True):
        self._logger = logger
        self.generate_token = generate_token
        self.connect_to_private = connect_to_private
        self.public = None
        self.private = None
        self.auth = auth

    # private が切断された場合に呼び出される
    async def re_connect(self):

        self._logger.info( "exec disconnect handler" )

        # publicも切断されるのを60秒待つ (60秒経ってもpublicが切断されていなければ切断されたのはprivateのみと判断）
        retry = 60
        while retry>0 :
            await asyncio.sleep(1)
            self._logger.info( "waiting public ws disconnect count:{} stat:{}".format(retry, self.public.connected) )
            retry -= 1
            if not self.public.connected :
                break

        # public が接続していたらサーバーは生きていると判断してトークンの再生成とprivateへの再接続を行う
        # (publicが切断していたら、再接続されるまで待つ)
        while not self.public.connected :
            await asyncio.sleep(10)
            self._logger.info( "waiting public ws connection" )

        try:
            self._logger.info( "stop private ws process" )
            await self.private.stop()
            await self.generate_token()
            await self.connect_to_private()
        except Exception:
            self._logger.error( traceback.format_exc() )

    @property
    def time_lag(self):
        return self.public.time_lag

    @property
    def connected(self):
        return (self.public.connected and self.private.connected)


class GMOCoin(pybotters.GMOCoinDataStore, TimeConv, RestAPIExchange, WebsocketExchange):

    def units(self,value=0):
        return {'unitrate' :1,                         # 損益額をプロフィットグラフに表示する単位に変換する係数
                'profit_currency' : 1,                 # 利益額の単位 0:USD 1:JPY
                'title':"JPY {:+,.0f}".format(value),  # 表示フォーマット
                'pos_rate':1}                          # ポジションをcurrency単位に変換する係数

    # シンボルごとの価格呼び値
    _price_unit_dict = {'BTC_JPY':1, 'ETH_JPY':1, 'BCH_JPY':1, 'LTC_JPY':1, 'XRP_JPY':0.001,
                        'BTC':1, 'ETH':1, 'BCH':1, 'LTC':1, 'XRP':0.001, }

    def round_order_price(self, price, symbol=None):
        price_unit = self._price_unit_dict[(symbol or self.symbol)]
        if price_unit==1 :
            return int(price)
        return round(round(price/price_unit)*price_unit, 8)

    _minimum_order_size_dict = {'BTC_JPY':0.01, 'ETH_JPY':0.1, 'BCH_JPY':0.1, 'LTC_JPY':1, 'XRP_JPY':10,
                                'BTC':0.0001, 'ETH':0.01, 'BCH':0.01, 'LTC':0.1, 'XRP':1, }
    def minimum_order_size(self, symbol=None):
        return self._minimum_order_size_dict.get(symbol or self.symbol)

    def _round_order_size(self, value):
        size_unit = self.minimum_order_size()
        if size_unit >= 1:
            return int(value)
        else :
            return round(round(value/size_unit)*size_unit, 8)

    def __init__(self, logger, candle_store, apikey=('',''), testnet=False):
        self._logger = logger
        self.candle_store = candle_store   # 分ローソク足変換クラス
        self.auth = (apikey[0]!='' and apikey[1]!='')

        self._api_url = "https://api.coin.z.com"
        self._endpoint = "wss://api.coin.z.com/ws"
        self.exchange_name = "gmo"

        self._apikey = apikey
        self._client = pybotters.Client(apis={'gmocoin': [apikey[0],apikey[1]]}, base_url=self._api_url)
        self._candle_api_lock = asyncio.Lock()
        self._datastore_lock = LockCounter(self._logger, self.__class__.__name__)
        self._order_dict_lock = LockCounter(self._logger, "order_dict")
        self._candle_subscribed = {}
        super().__init__()
        self.create_candle_class('candle')
        self.PendingUntil = time.time()
        self._get_request = deque(maxlen=3)
        self._post_request = deque(maxlen=3)

        RestAPIExchange.__init__(self)
        WebsocketExchange.__init__(self)

    # APIリミットの管理 (GMOにはリミットカウンタが無い）
    async def check_get_request_limit(self):
        self._get_request.append(time.time())
        if len(self._get_request)<3 :            # 3回
            return
        t = self._get_request.popleft()
        w = t+1-time.time()                      # 1秒あたり
        if w>0 :
            self._logger.info( "get request sleep : {:.3f}".format(w) )
            await asyncio.sleep(w)
        return

    async def check_post_request_limit(self):
        self._post_request.append(time.time())
        if len(self._post_request)<3 :           # 3回
            return
        t = self._post_request.popleft()
        w = t+2-time.time()                      # 2秒あたり
        if w>0 :
            self._logger.info( "post request sleep : {:.3f}".format(w) )
            await asyncio.sleep(w)
        return

    def _check_api_responce(self,r):
        if r.status != 200 :
            self._logger.error( "{}: {}".format(r.status, r.reason) )

    @property
    def api_remain1(self):
        return 500

    @property
    def api_remain2(self):
        return 500

    @property
    def api_remain3(self):
        return 300

    # データストアへの接続を共通名に変換
    @property
    def ticker(self):
        return self._stores.get('ticker')

    @property
    def board(self):
        return self._stores.get('orderbooks')

    @property
    def candle(self):
        return self._stores.get('candle')

    @property
    def positions(self):
        return self._stores.get('positions')

    @property
    def orders(self):
        return self._stores.get('orders')

    async def generate_token(self):
        await self.initialize(
            self._client.post('/private/v1/ws-auth'),
        )
        self._logger.info( "Generate ws token : {}".format(self.token) )

    # public ws が切断された場合に呼び出される
    async def _public_disconnected(self):
        self._disconnected_event.set()
        
        # private wsが稼働していなければ再作成
        if not self.private.connected :
            try:
                self._logger.info( "re-generate private ws process" )
                await self.private.stop()
                await self.generate_token()
                await self.connect_to_private()
            except Exception:
                self._logger.error( traceback.format_exc() )

    # private ws が切断された場合に呼び出される
    async def _private_disconnected(self):
        self._disconnected_event.set()
        await self.ws.re_connect()

    async def connect_to_private(self):
        self.ws.private = MultiProc_WS( logger = self._logger, exchange_name = self.__class__.__name__+'Private',
                          handler = self.onmessage,
                          endpoint = self._endpoint+'/private/v1/'+self.token,
                          apis = {self.exchange_name: [self._apikey[0],self._apikey[1]]},
                          send_json = self._ws_args_private,
                          lock = self._datastore_lock,
                          disconnect_handler = self._private_disconnected )
        asyncio.create_task(self.ws.private._start_websocket(logging=False), name="gmo_private_websocket")
        while not (self.ws.private.pid) :
            await asyncio.sleep(1)
        self._logger.info("Private Websocket connected")

    async def start(self, subscribe={}, symbol='BTC_JPY'):
        self._param = subscribe
        self.symbol = symbol

        self._spot = ('_JPY' not in symbol)
        # ポジション管理クラス
        if self._spot :
            self.my = AccountInfo(self._logger, OpenPositionKeepAveLinear) # 現物
        else :
            self.my = AccountInfo(self._logger, OpenPositionGross)         # デリバティブ

        # データ保存用クラス
        self.board_info = BoardInfo( self._logger )
        self.execution_info = ExecuionInfo( self._logger )
        self.ticker_info = TickerInfo()
        self.ticker_info.best_ask = 0
        self.ticker_info.best_bid = 0

        self._handler = {'None':None}

        # private/public のws管理クラス
        self.ws = DoubleConnection(logger = self._logger,
                                   generate_token = self.generate_token,
                                   connect_to_private = self.connect_to_private,
                                   auth = self.auth)
        if self.auth:
            # DataStoreの初期化
            await self.initialize(
                self._client.get(f'/private/v1/activeOrders?symbol={self.symbol}'),
            )
            if not self._spot :
                await self.initialize(
                    self._client.get(f'/private/v1/openPositions?symbol={self.symbol}'),
                    self._client.get(f'/private/v1/positionSummary?symbol={self.symbol}'),
                )
                # 定期的なポジション情報の取得（更新）を行うタスクを開始
                asyncio.create_task(self._update_position_task(), name="gmo_position")

            # private ws への接続
            await self.generate_token()
            self._ws_args = []
            self._subscribe(handler=self._on_my_execution, channel="executionEvents" )
            self._subscribe(handler=self._on_my_orders, channel="orderEvents" )
            self._subscribe(handler=self._on_my_positions, channel="positionEvents" )

            # WebSocket(private)タスクを開始
            self._ws_args_private = self._ws_args
            await self.connect_to_private()

        # 指定のチャンネルの購読
        self._ws_args = []
        if self._param.get('execution', True) : self._subscribe(handler=self._on_executions, channel="trades", symbol=self.symbol, option="TAKER_ONLY")
        if self._param.get('ticker', False) :   self._subscribe(handler=self._on_ticker, symbol=self.symbol, channel="ticker" )
        if self._param.get('board', False) :    self._subscribe(handler=self._on_board, symbol=self.symbol, channel="orderbooks")

        # WebSocket(public)タスクを開始
        self.ws.public = MultiProc_WS( logger = self._logger, exchange_name = self.__class__.__name__,
                      handler = self.onmessage,
                      endpoint = self._endpoint+'/public/v1',
                      apis = {self.exchange_name: [self._apikey[0],self._apikey[1]]},
                      send_json = self._ws_args,
                      lock = self._datastore_lock,
                      disconnect_handler = self._public_disconnected )
        asyncio.create_task(self.ws.public._start_websocket(logging=False), name="gmo_public_websocket")

        # 自動キャンセルタスクを開始
        asyncio.create_task(self._cancel_task(), name="gmo_cancel")

        while not (self.ws.public.pid) :
            await asyncio.sleep(1)
        self._logger.info("Public Websocket connected")

        return self

    # 定期的なポジション情報の取得のためのタスク
    async def _update_position_task(self):
        while self._logger.running:
            try:
                await asyncio.sleep(60)

                before = self.positions.find()
                self._logger.debug("positions before initialize : {}".format(before) )

                await self.initialize(
                    self._client.get(f'/private/v1/openPositions?symbol={self.symbol}'),
                    self._client.get(f'/private/v1/positionSummary?symbol={self.symbol}'),
                )

                after = self.positions.find()
                self._logger.debug("positions after initialize : {}".format(after) )

                if before==after :
                    self._logger.info( "position dict is not changed" )
                else:
                    for i in before:
                        position_id = i['position_id']
                        hit = [d for d in after if d['position_id']==position_id]
                        if hit :
                            if hit[0]!=i :
                                for key,items in i.items():
                                    if key not in hit[0]:
                                        self._logger.info( "position dict {} {} {} {}".format(position_id, key, items, '-->deleted') )
                                    elif items!=hit[0][key] :
                                        self._logger.info( "position dict {} {} {} {} {} {}".format(position_id, '[changed]', key, items, 'to', hit[0][key])  )
                                for key,items in hit[0].items() :
                                    if key not in i:
                                        self._logger.info( "position dict {} {} {} {}".format(position_id, '[new key]', key, hit[0][key])  )
                        else:
                            self._logger.info( "position dict {} {}".format(position_id, ' is deleted') )
                    for i in after:
                        position_id = i['position_id']
                        hit = [d for d in before if d['position_id']==position_id]
                        if not hit :
                            self._logger.info( "position dict {} {} {}".format(position_id, '[new item]', i) )
            except Exception:
                self._logger.error( traceback.format_exc() )

    def _subscribe(self, handler, **argv):
        self._logger.debug( "subscribe : {}".format(argv) )
        self._handler[argv['channel']]=handler
        argv['command']='subscribe'
        self._ws_args.append(argv)

    def _onmessage(self, msg, ws):
        self._logger.debug("recv gmo websocket [{}]".format(msg.get('channel', 'None')))
        self._logger.trace("recv data [{}]".format(msg))

        topic_handler = self._handler.get(msg.get('channel', 'None'))
        if topic_handler != None:
            topic_handler(msg)

            if topic_handler in [self._on_ticker, self._on_my_positions, self._on_my_orders ] :
                super()._onmessage(msg,ws)

        else:
            super()._onmessage(msg,ws)

    def _on_executions(self,msg):
        self.execution_info.time = self._utcstr_to_dt(msg['timestamp'])
        self.execution_info.append_latency((self._jst_now_dt().timestamp() - self.execution_info.time.timestamp())*1000)
        self.execution_info.append_execution(float(msg['price']),float(msg['size']),msg['side'],self.execution_info.time)
        self.my.position.ref_ltp = self.execution_info.last
        self.execution_info._event.set()

    def _on_ticker(self,msg):
        self.ticker_info.time = self._utcstr_to_dt(msg['timestamp'])
        self.ticker_info.last = float(msg.get('last',self.ticker_info.last))
        self.ticker_info.best_ask = float(msg.get('ask',self.ticker_info.best_ask))
        self.ticker_info.best_bid = float(msg.get('bid',self.ticker_info.best_bid))
        self.my.position.ref_ltp = self.ticker_info.last

    def _on_board(self,msg):
        self.board_info.initialize_dict()
        self.board_info.time = self._utcstr_to_dt(msg['timestamp'])
        self.board_info.update_asks(msg.get('asks',[]))
        self.board_info.update_bids(msg.get('bids',[]))
        self.board_info.event.set()
        for event in self.board._events:
            event.set()

    def _on_my_positions(self,msg):
        """
        {'channel': 'positionEvents', 'leverage': '4', 'lossGain': '0', 'losscutPrice': '5972680', 'msgType': 'OPR', 'orderdSize': '0', 'positionId': 86266891, 'price': '5309049', 'side': 'SELL', 'size': '0.01', 'symbol': 'BTC_JPY', 'timestamp': '2021-03-06T07:26:35.030Z'}
        """
        self._logger.debug( self.__class__.__name__ + " : Position : " + str(msg) )

    def _on_my_execution(self,msg):

        """
        {'channel': 'executionEvents', 'executionId': 278209163, 'executionPrice': '5292138', 'executionSize': '0.01', 'executionTimestamp': '2021-03-06T07:10:32.004Z', 'executionType': 'LIMIT', 'fee': '0', 'lossGain': '0', 'msgType': 'ER', 'orderExecutedSize': '0.01', 'orderId': 1201641525, 'orderPrice': '5292138', 'orderSize': '0.01', 'orderTimestamp': '2021-03-06T07:10:10.795Z', 'positionId': 86266824, 'settleType': 'OPEN', 'side': 'SELL', 'symbol': 'BTC_JPY', 'timeInForce': 'FAS'}

        {'channel': 'executionEvents', 'executionId': 278233470, 'executionPrice': '5309049', 'executionSize': '0.01', 'executionTimestamp': '2021-03-06T07:26:35.030Z', 'executionType': 'LIMIT', 'fee': '0', 'lossGain': '0', 'msgType': 'ER', 'orderExecutedSize': '0.01', 'orderId': 1201663589, 'orderPrice': '5309049', 'orderSize': '0.01', 'orderTimestamp': '2021-03-06T07:25:21.866Z', 'positionId': 86266891, 'settleType': 'OPEN', 'side': 'SELL', 'symbol': 'BTC_JPY', 'timeInForce': 'FAS'}
        """
        self._logger.trace( self.__class__.__name__ + " : EXECUTION : " + str(msg) )

        self._order_dict_lock.wait()
        # オーダーリストから削除
        order_info = self.my.order.executed( id=str(msg['orderId']), side=msg['side'], price=float(msg['executionPrice']), size=float(msg['executionSize']) )

        if not self._spot :
            if 'positionId' in msg and (order_info or self.my.position.is_myorder(id=str(msg['positionId']))) :
                self.my.position.executed( posid=str(msg['positionId']), side=msg['side'], price=float(msg['executionPrice']), size=float(msg['executionSize']), orderid=msg['orderId'] , commission=-float(msg['fee']), settleType=msg['settleType'])

        else:
            if order_info :
                self.my.position.executed( id=str(msg['orderId']), side=msg['side'], price=float(msg['executionPrice']), size=float(msg['executionSize']), commission=-float(msg['fee']) )

    def _on_my_orders(self,message):
        """
        new_order
        {'channel': 'orderEvents', 'executionType': 'LIMIT', 'losscutPrice': '0', 'msgType': 'NOR', 'orderExecutedSize': '0', 'orderId': 1201501249, 'orderPrice': '5300000', 'orderSize': '0.01', 'orderStatus': 'ORDERED', 'orderTimestamp': '2021-03-06T05:29:53.188Z', 'settleType': 'OPEN', 'side': 'SELL', 'symbol': 'BTC_JPY', 'timeInForce': 'FAS'}

        change
        {'channel': 'orderEvents', 'executionType': 'LIMIT', 'losscutPrice': '0', 'msgType': 'ROR', 'orderExecutedSize': '0', 'orderId': 1201501249, 'orderPrice': '5299900', 'orderSize': '0.01', 'orderStatus': 'ORDERED', 'orderTimestamp': '2021-03-06T05:29:53.188Z', 'settleType': 'OPEN', 'side': 'SELL', 'symbol': 'BTC_JPY', 'timeInForce': 'FAS'}

        cancel
        {'cancelType': 'USER', 'channel': 'orderEvents', 'executionType': 'LIMIT', 'losscutPrice': '0', 'msgType': 'COR', 'orderExecutedSize': '0', 'orderId': 1201501249, 'orderPrice': '5299900', 'orderSize': '0.01', 'orderStatus': 'CANCELED', 'orderTimestamp': '2021-03-06T05:29:53.188Z', 'settleType': 'OPEN', 'side': 'SELL', 'symbol': 'BTC_JPY', 'timeInForce': 'FAS'}
        {'cancelType': 'USER', 'channel': 'orderEvents', 'executionType': 'LIMIT', 'losscutPrice': '0', 'msgType': 'COR', 'orderExecutedSize': '0', 'orderId': 1201683420, 'orderPrice': '5253214', 'orderSize': '0.02', 'orderStatus': 'CANCELED', 'orderTimestamp': '2021-03-06T07:37:10.764Z', 'settleType': 'OPEN', 'side': 'BUY', 'symbol': 'BTC_JPY', 'timeInForce': 'FAS'}
        """
        self._logger.debug( "_on_my_orders= " + message['orderStatus'] + " : " + str(message) )

        self._order_dict_lock.wait()
        if message['orderStatus']=='ORDERED' :
            if self.my.order.update_order( id=str(message['orderId']), side=message['side'], price=float(message['orderPrice']), size=float(message['orderSize']) ) :
                self._logger.debug( 'updated order dict: {}'.format(self.my.order.order_dict) )
                if not self._spot :
                    self._logger.debug( 'long_pos: {}'.format(self.my.position._long_position) )
                    self._logger.debug( 'short_pos: {}'.format(self.my.position._short_position) )
                    self._logger.debug( 'size, ave, unreal: {}'.format(self.my.position._calc_position) )
                self._logger.debug( 'profit: {}'.format(self.my.position.realized) )

        elif message['orderStatus']=='CANCELED' :
            order_info = self.my.order.remove_order( id=str(message['orderId']) )
            if order_info :
                self._logger.debug( 'deleted order: {}'.format(order_info) )
                if not self._spot :
                    self._logger.debug( 'long_pos: {}'.format(self.my.position._long_position) )
                    self._logger.debug( 'short_pos: {}'.format(self.my.position._short_position) )
                    self._logger.debug( 'size, ave, unreal: {}'.format(self.my.position._calc_position) )
                self._logger.debug( 'profit: {}'.format(self.my.position.realized) )

        elif message['orderStatus']=='EXPIRED' :
            order_info = self.my.order.remove_order( id=str(message['orderId']) )
            if order_info :
                self._logger.debug( 'deleted order: {}'.format(order_info) )
                if not self._spot :
                    self._logger.debug( 'long_pos: {}'.format(self.my.position._long_position) )
                    self._logger.debug( 'short_pos: {}'.format(self.my.position._short_position) )
                    self._logger.debug( 'size, ave, unreal: {}'.format(self.my.position._calc_position) )
                self._logger.debug( 'profit: {}'.format(self.my.position.realized) )

    # 1分に一度読込を行う（更新されるまで）
    async def get_latest_candles(self,target_symbol):
        start = time.time()

        # 基本の分足を入れるDataStoreクラス
        exchange_symbol = self.__class__.__name__+target_symbol
        self.candle_store.set_min_candle(exchange_symbol,self.candle)

        last_time = self.candle_store.get_last_min_candle(exchange_symbol)
        while last_time==self.candle_store.get_last_min_candle(exchange_symbol) and start+50>time.time():
            try:
                await self.get_candles(timeframe=1, symbol=target_symbol, since=last_time, need_result=False)
            except Exception:
                self._logger.error( traceback.format_exc() )
            await asyncio.sleep(3)


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

        while keep_running and starttime<=endtime:

            # 1時間足以下は 2021/04/15 06:00以降
            starttime = max(1618434000,starttime)
            # なぜか6:00AM区切りなのでtimedelta=3で計算
            date_str = datetime.fromtimestamp(starttime, tz=timezone(timedelta(hours=3))).strftime("%Y%m%d")

            self._logger.debug( "starttime: {}".format(datetime.fromtimestamp(starttime,tz=timezone(timedelta(hours=9),'JST'))) )
            self._logger.debug( "endtime  : {}".format(datetime.fromtimestamp(endtime,tz=timezone(timedelta(hours=9),'JST'))) )

            try:
                responce = ''
                params = {'symbol': target_symbol, 'interval': "1min", 'date':date_str}
                await self.check_get_request_limit()
                res = await self._client.get("/public/v1/klines", params=params)
                self._check_api_responce(res)
                responce = await res.json()
                if 'status' not in responce :
                    raise RuntimeError

                if responce['status']==0 :
                    rows = responce['data']

                    # 得られたデータが空っぽなら終了
                    if len(rows)==0 :
                        keep_running = False
                        self._logger.debug( "{} : Fetch candles from API (request {}) [empty]".format(target_symbol, date_str) )
                        break

                    self._logger.info( "{} : Fetch {} candles timeframe{} from API [{}~{}]".format(target_symbol,len(rows),api_timeframe,
                        datetime.fromtimestamp(int(int(rows[0]['openTime'])/1000),tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M'),
                        datetime.fromtimestamp(int(int(rows[-1]['openTime'])/1000),tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M')) )

                    # 得られたデータが指定されたスタート時刻まで含まれていなかったらまだその時刻のローソクはできていないので終了
                    if starttime*1000>int(rows[-1]['openTime']) :
                        break

                    data = [[int(c['openTime']),c['open'],c['high'],c['low'],c['close'],c['volume']] for c in rows if starttime*1000<=int(c['openTime'])<=endtime*1000]
                    self.candle_store.set_last_min_candle(exchange_symbol, data[-1][0]/1000)

                    # 分足データストアに保管
                    self.candle._onmessage({'symbol': target_symbol, 'interval': api_timeframe, 'kline': data})

                    # データ取得用一時ストア
                    if need_result:
                        self._stores.get('temp_candle')._onmessage({'symbol': target_symbol, 'interval': timeframe_sec, 'kline':data})

                    # 取得した分足が8000本を超えるようなら一度リサンプルを行う
                    if len(self.candle)>8000:
                        with self._datastore_lock :
                            self.candle_store.resample(symbol=target_symbol)

                    # 次回の取得開始時
                    day = 24*60*60
                    starttime = (int(rows[-1]['openTime'])/1000+3*60*60)//day*day+api_timeframe+21*60*60

                    # 得られたデータが目的のendtimeを超えていたら終了
                    if endtime*1000 < int(rows[-1]['openTime']):
                        keep_running = False

                else:
                    if responce :
                        self._logger.error( responce )
                    self._candle_api_lock.release()
                    return None

            except Exception:
                if responce :
                    self._logger.error( responce )
                self._logger.error( traceback.format_exc() )
                self._candle_api_lock.release()
                return None

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

        agg_param = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
        df = df.resample(f'{timeframe*60}S').agg(agg_param)
        df['close'].fillna(method='ffill', inplace=True)
        df['open'].fillna(df['close'], inplace=True)
        df['high'].fillna(df['close'], inplace=True)
        df['low'].fillna(df['close'], inplace=True)
        df['volume'].fillna(0, inplace=True)

        self._candle_api_lock.release()
        return df.tz_convert(timezone(timedelta(hours=9), 'JST')).tail(num_of_candle)

    def create_candle_class(self,id):
        self.create(id, datastore_class=Kline)


    # APIからTickerを取得
    async def ticker_api(self, **kwrgs):
        try:
            await self.check_get_request_limit()
            res = await self._client.get('/public/v1/ticker', params={'symbol': kwrgs.get('symbol', self.symbol)})
            self._check_api_responce(res)
            data = await res.json()
            self.my.position.ref_ltp = float(data['data'][0]['last'])
            return {'stat':data['status'] , 'ltp':float(data['data'][0]['last']), 'msg':data}
        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

    # APIから現在ポジを取得
    async def getpositions(self, **kwrgs):
        try:
            symbol = kwrgs.get('symbol', self.symbol)

            if ('_JPY' not in symbol) :
                await self.check_get_request_limit()
                r = await self._client.get('/private/v1/account/assets')
                self._check_api_responce(r)
                res = await r.json()
                return[{'side':'BUY', 'size': float([d for d in res.get('data',[]) if d['symbol']==symbol][0]['amount'])}]

            else:
                await self.check_get_request_limit()
                r = await self._client.get('/private/v1/openPositions', params={'symbol':symbol})
                self._check_api_responce(r)
                res = await r.json()
                return res.get('data',{}).get('list',[])

        except Exception:
            self._logger.error(traceback.format_exc())
            return []


    # 証拠金口座の証拠金額 (JPY)
    async def getcollateral(self, coin=None):
        try:
            await self.check_get_request_limit()
            r = await self._client.get('/private/v1/account/margin')
            self._check_api_responce(r)
            res = await r.json()
            if res.get('status',-1)!=0 :
                return {'stat': res.get('status',-1), 'msg': res}
            return {'stat': 0, 'collateral': res['data']['actualProfitLoss'], 'msg': res}

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
                   symbol="BTC_JPY"          # 省略時には起動時に指定した symbol が選択されます
                   timeInForce="GTC",        # FAK ( MARKET STOPの場合のみ設定可能 ) FAS FOK ((Post-onlyの場合はSOK) LIMITの場合のみ設定可能 ) *指定がない場合は成行と逆指値はFAK、指値はFASで注文されます。
                   losscutPrice,             # レバレッジ取引で、executionTypeが LIMIT または STOP の場合のみ設定可能。
                   cancelBefore,             # 
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

        if self.PendingUntil > time.time() :
            self._logger.info("Order pending : {:.1f}sec".format(self.PendingUntil-time.time()) )
            return {'stat': -999, 'msg': "Order pending : {:.1f}sec".format(self.PendingUntil-time.time()), 'ids': []}

        adjust_flag = kwargs.get('adjust_flag', False)

        # 対応しているオプションだけにする
        params={}
        for k,v in kwargs.items():
            if k=='timeInForce' :
                if v in ["FAK", "FAS", "FOK", "SOK", ] :
                    params[k]=v
            elif k in ['symbol','losscutPrice','cancelBefore']:
                params[k]=v

        params['symbol']=params.get('symbol', self.symbol)
        params['executionType']=order_type
        params['side']=side
        if price!=0 :
            params['price']=self.round_order_price(price)

        # ノートレード期間はポジ以上のクローズは行わない
        if self.noTrade :
            size = min(abs(self.my.position.size),size)
            self._logger.info("No trade period (change order size to {})".format(size) )

        # 最低取引量のチェック（無駄なAPIを叩かないように事前にチェック）
        if size<self.minimum_order_size(symbol=params['symbol']) :
            return {'stat': -1, 'msg': '最低取引数量を満たしていません', 'ids': []}

        remain = self._round_order_size(size)
        ordered_id_list = []
        if not self._spot :
            # 決済オーダーを出していないポジション数を検索（サイズの大きい順に並べて大きいものから消化する）
            positions = self.positions.find()
            self._logger.debug("positions : {}".format(positions) )
            orders = sorted([o for o in positions
                             if o['orderd_size']<o['size'] and  # 決済オーダーがまだ出せるもの
                             o['side'].name!=side.upper() and   # 売買方向が逆のポジ
                             ((str(o['position_id']) in self.my.position._long_position) or (str(o['position_id']) in self.my.position._short_position))], # 自分のポジリストにあるもの（自分のオーダーだけに決済オーダーを出す）
                             key=lambda o: float(o['size']-o['orderd_size']), reverse=True)
            self._logger.debug("long position : {}".format(self.my.position._long_position) )
            self._logger.debug("short position : {}".format(self.my.position._short_position) )
            self._logger.debug("settle orders : {}".format(orders) )

            for o in orders :
                positionsize = float(o['size']-o['orderd_size'])

                # 大きすぎるポジションなので決済には使えない
                if positionsize>remain :
                    continue

                params['settlePosition'] = [{'positionId':int(o['position_id']), 'size':str(o['size'])}]
                self._logger.debug("[sendorder(close)] : {}".format(params) )
                await self.check_post_request_limit()
                with self._order_dict_lock :
                    try:
                        r = await self._client.post('/private/v1/closeOrder', data=params)
                        self._check_api_responce(r)
                        res = await r.json()

                        ret_code = res.get('status')
                        if ret_code==0 :
                            if self.my and not adjust_flag:
                                self.my.order.new_order( symbol=params['symbol'], id=res['data'] , side=params['side'], price=self.round_order_price(price),
                                                         size=float(o['size']), expire=time.time()+auto_cancel_after, invalidate=time.time()+2592000, closeid=str(o['position_id']) )
                            # オーダーに成功
                            ordered_id_list.append(res['data'])
                            remain -= positionsize

                            # 決済だけでオーダー出し終わった場合
                            if round(remain,8)==0 :
                                return {'stat':0 , 'msg':"", 'ids':ordered_id_list}

                        # {'status': 4, 'messages': [{'message_code': 'ERR-5003', 'message_string': 'Requests are too many.'}]
                        elif res.get('messages',[{}])[0].get('message_code')=='ERR-5003' :
                            # APIリミットエラーの場合には次のアクセスを2秒後まで禁止
                            await asyncio.sleep(2)

                    except Exception:
                        self._logger.error(traceback.format_exc())

        if 'settlePosition' in params :
            del params['settlePosition']
        params['size']=self._round_order_size(remain)
        self._logger.debug("[sendorder] : {}".format(params) )

        await self.check_post_request_limit()
        with self._order_dict_lock :
            try:
                r = await self._client.post('/private/v1/order', data=params)
                self._check_api_responce(r)
                res = await r.json()

                ret_code = res.get('status',-999)
                if ret_code==0 :
                    if self.my and not adjust_flag:
                        self.my.order.new_order( symbol=params['symbol'], id=res['data'] , side=params['side'], price=self.round_order_price(price),
                                                 size=params['size'], expire=time.time()+auto_cancel_after, invalidate=time.time()+2592000 )

                    # オーダーに成功
                    ordered_id_list.append(res['data'])
                    return {'stat':0 , 'msg':"", 'ids':ordered_id_list}

                # {'status': 4, 'messages': [{'message_code': 'ERR-5003', 'message_string': 'Requests are too many.'}]
                elif res.get('messages',[{}])[0].get('message_code')=='ERR-5003' :
                    # APIリミットエラーの場合には次のアクセスを2秒後まで禁止
                    self.PendingUntil = max(self.PendingUntil, time.time()+2)

                self._logger.error("Send order param : {}".format(params) )
                self._logger.error("Error response [sendorder] : {}".format(res) )
                return {'stat':ret_code , 'msg':res.get('data'), 'ids':ordered_id_list}

            except Exception as e:
                self._logger.error(traceback.format_exc())
                return {'stat': -999, 'msg': str(e), 'ids': ordered_id_list}

    # キャンセル
    async def cancelorder(self, id, **kwargs ):
        """
        cancelorder( id                          # キャンセルする注文の ID です。
               #-------------以下 bitFlyer独自パラメータ
                   product_code="FX_BTC_JPY"     # 省略時には起動時に指定した symbol が選択されます
                  ) )

        return: { 'stat': エラーコード,      # キャンセル成功時 0
                  'msg':  エラーメッセージ,
                }
        """

        if id not in self.my.order.order_dict :
            self._logger.info("ID:{} is already filled or canceld or expired".format(id) )
            return {'stat':-999 , 'msg':"Order is already filled or canceld or expired"}

        kwargs['orderId']=id

        self._logger.debug("[cancelorder] : {}".format(kwargs) )

        self.my.order.mark_as_invalidate( id )

        try:
            await self.check_post_request_limit()
            r = await self._client.post('/private/v1/cancelOrder', data=kwargs)
            self._check_api_responce(r)
            res = await r.json()

        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

        if res.get('status')!=0 : 
            self._logger.error("Error response [cancelorder id:{}] : {}".format(id,res) )

            # すでにオーダーがなくなっている
            # [{'message_code': 'ERR-5122', 'message_string': 'The request is invalid due to the status of the specified order.'}]
            if res.get('messages',[{}])[0].get('message_code')=='ERR-5122' :
                order_info = self.my.order.mark_as_invalidate( id, timeout=60 ) # 60秒後にオーダーリストから削除(キャンセルと約定が同時になったときには約定を優先させるため)

            # {'status': 4, 'messages': [{'message_code': 'ERR-5003', 'message_string': 'Requests are too many.'}]
            elif res.get('messages',[{}])[0].get('message_code')=='ERR-5003' :
                # APIリミットエラーの場合には次のアクセスを2秒後まで禁止
                self.PendingUntil = max(self.PendingUntil, time.time()+2)

            return {'stat':res.get('status') , 'msg':res.get('messages')}

        return {'stat': 0, 'msg': ""}

