# coding: utf-8
#!/usr/bin/python3

import asyncio
from collections import deque
from datetime import datetime, timedelta, timezone
import time
import traceback
import pandas as pd
import random


import pybotters
from pybotters.store import DataStore
from libs.utils import TimeConv, LockCounter
from libs.market import *
from libs.exchanges.base_module import MultiProc_WS, RestAPIExchange, WebsocketExchange
from libs.account import AccountInfo, OpenPositionKeepAve, OpenPositionKeepAveLinear

# https://bitgetlimited.github.io/apidoc/en/mix/#restapi
# https://github.com/BitgetLimited/v3-bitget-api-sdk


class Kline(DataStore):
    _KEYS = ['symbol', 'interval', 'timestamp']
    _MAXLEN = 1000000

    def _onmessage(self, message):
        symbol= message.get('symbol')
        interval= message.get('interval')
        self._insert([{'symbol':symbol, 'interval':interval,
                       'timestamp':int(item[0])/1000, 'open':float(item[1]), 'high':float(item[2]), 'low':float(item[3]), 'close':float(item[4]), 'volume':float(item[5])}
                       for item in message.get('kline',[])])

class Bitget(pybotters.BitgetDataStore, TimeConv, RestAPIExchange, WebsocketExchange):

    # インバースタイプ契約かリニアタイプ契約か (証拠金通貨がUSDならリニア契約)
    def is_linear(self, symbol=None):
        return self._collateral_coin.get((symbol or self.symbol),'USDT')=="USDT"

    def units(self,value=0):
        if self.is_linear() :
            return {'unitrate' :1,                          # 損益額をプロフィットグラフに表示する単位に変換する係数
                    'profit_currency' : 0,                  # 利益額の単位 0:USD 1:JPY
                    'title':"USD {:+,.2f}".format(value),   # 表示フォーマット
                    'pos_rate':1}                           # ポジションをcurrency単位に変換する係数
        else:
            return {'unitrate' :self.execution_info.last,   # 損益額をプロフィットグラフに表示する単位に変換する係数
                    'profit_currency' : 0,                  # 利益額の単位 0:USD 1:JPY
                    'title':"USD {:+,.2f}".format(value),   # 表示フォーマット
                    'pos_rate':1}                           # ポジションをcurrency単位に変換する係数

    def __init__(self, logger, candle_store, apikey=('','',''), testnet=False):
        self._logger = logger
        self.candle_store = candle_store   # 分ローソク足変換クラス
        self.auth = (apikey[0]!='' and apikey[1]!='' and apikey[2]!='')
        self._apikey = apikey

        self._api_url = "https://api.bitget.com/api/mix/v1"
        self._endpoint = "wss://ws.bitget.com/mix/v1/stream"
        self.exchange_name = "bitget"

        self._client = pybotters.Client(apis={self.exchange_name: [apikey[0],apikey[1],apikey[2]]}, base_url=self._api_url)
        self._candle_api_lock = asyncio.Lock()
        self._datastore_lock = LockCounter(self._logger, self.__class__.__name__)
        self._order_dict_lock = LockCounter(self._logger, "order_dict")
        self._candle_subscribed = []
        super().__init__()

        self.PendingUntil = time.time()
        self.RateLimitRemaining_private = 10
        self.RateLimitRemaining_public = 20
        self._request_count_private = deque(maxlen=5)
        self._request_count_public = deque(maxlen=10)

        RestAPIExchange.__init__(self)
        WebsocketExchange.__init__(self)

    # APIリミットの管理
    def _update_public_limit(self,r):
        if r.status != 200 :
            self._logger.error( "{}: {}".format(r.status, r.reason) )
        self.RateLimitRemaining_public = int(r.headers.get('X-RateLimit-Remaining',self.RateLimitRemaining_public))

    async def check_public_request_limit(self, id=None):
        # 前回オーダーから3秒以上経っていたらカウンターを初期化
        if len(self._request_count_public)!=0 :
            if time.time()-list(self._request_count_public)[-1]>3 :
                self.RateLimitRemaining_public = 20

        if len(self._request_count_public)<10 :        # 10回
            # 今回の時間を追加
            self._request_count_public.append(time.time())
            return

        w = list(self._request_count_public)[0]+1-time.time()                            # 1秒あたり
        while w>0:
            w += random.random()
            self._logger.info( "public request sleep : {:.3f}".format(w, id) )
            await asyncio.sleep(w)
            w = list(self._request_count_public)[0]+1-time.time()                        # 1秒あたり

        # 今回の時間を追加
        self._request_count_public.append(time.time())
        return

    def _update_private_limit(self,r):
        if r.status != 200 :
            self._logger.error( "{}: {}".format(r.status, r.reason) )
        self.RateLimitRemaining_private = int(r.headers.get('X-RateLimit-Remaining',self.RateLimitRemaining_private))

    async def check_private_request_limit(self, id=None):
        self._logger.debug( "private deque : {}".format(self._request_count_private) )
        # 前回オーダーから3秒以上経っていたらカウンターを初期化
        if len(self._request_count_private)!=0 :
            if time.time()-list(self._request_count_private)[-1]>3 :
                self.RateLimitRemaining_private = 10

        if len(self._request_count_private)<5 :        # 5回
            # 今回の時間を追加
            self._request_count_private.append(time.time())
            return

        w = list(self._request_count_private)[0]+1-time.time()                            # 1秒あたり
        while w>0:
            w += random.random()
            self._logger.info( "private request sleep : {:.3f} {}".format(w,id) )
            await asyncio.sleep(w)
            w = list(self._request_count_private)[0]+1-time.time()                        # 1秒あたり

        # 今回の時間を追加
        self._request_count_private.append(time.time())
        return

    @property
    def api_remain1(self):
        return self.RateLimitRemaining_private

    @property
    def api_remain2(self):
        return self.RateLimitRemaining_public

    @property
    def api_remain3(self):
        return 300

    def minimum_order_size(self, symbol=None):
        return self._minimum_order_size_dict.get(symbol or self.symbol, 1)

    def round_order_price(self, price, symbol=None):
        price_unit = self._price_unit_dict.get((symbol or self.symbol),1)
        return round(round(price/price_unit)*price_unit, 8)

    # データストアへの接続を共通名に変換
    @property
    def ticker(self):
        return self._stores.get('ticker')

    @property
    def board(self):
        return self._stores.get('orderbook')

    @property
    def candle(self):
        return self._stores.get('candlesticks')

    @property
    def positions(self):
        return self._stores.get('positions')

    @property
    def orders(self):
        return self._stores.get('orders')

    @property
    def account(self):
        return self._stores.get('account')

    async def start(self, subscribe={}, symbol='BTCUSD'):

        self._param = subscribe
        self.symbol = symbol

        # マーケット情報の取得
        await self.check_public_request_limit()
        res = await self._client.get('/market/contracts', params={'productType':'umcbl'})
        self._update_public_limit(res)
        data = await res.json()
        market_info = data['data']
        await self.check_public_request_limit()
        res = await self._client.get('/market/contracts', params={'productType':'dmcbl'})
        self._update_public_limit(res)
        data = await res.json()
        market_info += data['data']

        # シンボルごとの価格呼び値（BTCなど指定のない物は1）
        self._price_unit_dict = dict([(s['symbol'].split('_')[0],10**-int(s['pricePlace'])*int(s['priceEndStep'])) for s in market_info])

        # 最小取引単位（レバレッジ契約など指定のない物は1）
        self._minimum_order_size_dict = dict([(s['symbol'].split('_')[0],float(s['minTradeNum'])) for s in market_info])

        # 取引するシンボルに使用する証拠金通貨 (リストにない物はUSD)
        self._collateral_coin = dict([(s['symbol'].split('_')[0],s['supportMarginCoins'][0]) for s in market_info])

        # 取引通貨（オーダー単位）
        self._currency = dict([(s['symbol'].split('_')[0],s['baseCoin']) for s in market_info])

        # ポジション管理クラス
        if self.is_linear() :
            self.my = AccountInfo(self._logger, OpenPositionKeepAveLinear, order_currency=self._currency[self.symbol])
        else:
            self.my = AccountInfo(self._logger, OpenPositionKeepAve, order_currency=self._currency[self.symbol])

        # データ保存用クラス
        self.board_info = BoardInfo( self._logger )
        self.execution_info = ExecuionInfo( self._logger )
        self.ticker_info = TickerInfo()
        self.ticker_info.open_interest = 0

        # 指定のチャンネルの購読
        self._handler = {'None':None}
        self._channels = []
        self._ws_args = []
        if self._param.get('execution', True) : self._subscribe(handler=self._on_executions, key="trade", instType='mc', channel="trade", instId=self.symbol)
        if self._param.get('ticker', False) :   self._subscribe(handler=self._on_ticker, key="ticker", instType='mc', channel="ticker", instId=self.symbol)
        if self._param.get('board', False) :    self._subscribe(handler=self._on_board, key="books", instType='mc', channel="books", instId=self.symbol)

        if self.auth:
            insttype = 'UMCBL' if self.is_linear() else 'DMCBL'

            self._subscribe(handler=self._on_my_account, key="account", instType=insttype, channel="account", instId="default")
            self._subscribe(handler=self._on_my_positions, key="positions", instType=insttype, channel="positions", instId="default")
            self._subscribe(handler=self._on_my_orders, key="orders", instType=insttype, channel="orders", instId="default")

            # DataStoreの初期化
            await self.initialize(
                self._client.get('/order/current', params={'symbol': self.symbol+'_'+insttype})
            )

        # ローソク足受信処理タスクを開始
        asyncio.create_task(self._wait_candle_event(), name="bitget_candle")

        # WebSocketタスクを開始
        options = {} if self.auth else {'auth': None}
        self.ws = MultiProc_WS( logger = self._logger, exchange_name = self.__class__.__name__,
                      handler = self.onmessage,
                      endpoint = self._endpoint,
                      apis = {self.exchange_name: [self._apikey[0],self._apikey[1],self._apikey[2]]},
                      send_json = self._ws_args,
                      lock = self._datastore_lock,
                      disconnect_handler = self._disconnected,
                      **options )
        asyncio.create_task(self.ws._start_websocket(logging=False), name="bitget_websocket")

        # 自動キャンセルタスクを開始
        asyncio.create_task(self._cancel_task(), name="bitget_cancel")

        if self.auth:
            while not (self.ws.pid) :
                await asyncio.sleep(1)
            self._logger.info("Websocket connected")

        return self

    def _subscribe(self, handler, key, **argv):
        self._logger.debug( "subscribe : {}".format(argv) )
        self._handler[key]=handler
        self._channels.append(argv)
        self._ws_args = [{"op": "subscribe", "args": self._channels}]
        return argv

    def _onmessage(self, msg, ws):
        self._logger.debug("recv bitget websocket [{}]".format(msg.get('arg',{}).get('channel', 'None')))
        self._logger.trace("recv data [{}]".format(msg))

        if 'arg' in msg and 'data' in msg:
            channel = msg['arg'].get('channel')
            topic_handler = self._handler.get(channel)

            if topic_handler != None:
                topic_handler(msg)

                if topic_handler in [self._on_ticker, self._on_my_account, self._on_my_positions, self._on_my_orders ] :
                    super()._onmessage(msg,ws)

            else:
                super()._onmessage(msg,ws)

        if msg.get('event', '') == 'error':
            self._logger.warning(msg)

            # {'event': 'error', 'code': 30004, 'msg': 'User not logged in/User must be logged in'}
            if msg.get('code',0)==30004 :
                self.ws.wdt = 100 # wsの再接続

    def _on_executions(self,msg):
        trades = msg.get('data')
        self.execution_info.time = self._epoc_to_dt(int(trades[0][0])/1000)
        for t in trades:
            self.execution_info.append_execution(float(t[1]),float(t[2]),"BUY" if t[3]=="buy" else "SELL", self._epoc_to_dt(int(t[0])/1000))
        self.execution_info.append_latency(self._jst_now_dt().timestamp()*1000 - int(trades[0][0]))
        self.my.position.ref_ltp = self.execution_info.last
        self.execution_info._event.set()

    def _on_ticker(self,msg):
        ticks = msg['data']
        for t in ticks:
            self.ticker_info.time = self._epoc_to_dt(int(t['systemTime'])/1000)
            self.ticker_info.last = float(t.get('last',self.ticker_info.last))
        self.my.position.ref_ltp = self.ticker_info.last


    def _on_board(self,msg):
        books = msg['data']
        self.board_info.time = self._epoc_to_dt(int(books[0]['ts'])/1000)
        msg_type=msg.get('action')    
        if msg_type=='snapshot' :
            self.board_info.initialize_dict()
        for b in books:
            self.board_info.update_asks(b.get('asks',[]))
            self.board_info.update_bids(b.get('bids',[]))
        self.board_info.event.set()
        for event in self.board._events:
            event.set()


    def _on_my_account(self,message):
        '''
        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'account', 'instId': 'default'}, 'data': [{'marginCoin': 'USDT', 'locked': '2.26544709', 'available': '113.69951041', 'maxOpenPosAvailable': '111.43406332', 'maxTransferOut': '111.43406332', 'equity': '113.69951041', 'usdtEquity': '113.699510415286'}]}
        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'account', 'instId': 'default'}, 'data': [{'marginCoin': 'USDT', 'locked': '22.60617859', 'available': '113.43409141', 'maxOpenPosAvailable': '88.51561282', 'maxTransferOut': '88.51561282', 'equity': '113.33834141', 'usdtEquity': '113.338341415286'}]}
        '''

        self._logger.debug("_on_my_account={}".format(message))


    def _on_my_positions(self,message):
        '''
        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'positions', 'instId': 'default'}, 'data': [{'posId': '885347000256208896', 'instId': 'BTCUSDT_UMCBL', 'instName': 'BTCUSDT', 'marginCoin': 'USDT', 'margin': '2.2165', 'marginMode': 'crossed', 'holdSide': 'long', 'holdMode': 'double_hold', 'total': '0.001', 'available': '0.001', 'locked': '0', 'averageOpenPrice': '44331', 'leverage': 20, 'achievedProfits': '0', 'upl': '0.0012', 'uplRate': '0.0005', 'liqPx': '0', 'keepMarginRate': '0.004', 'marginRate': '0.001793554076', 'cTime': '1646809094489', 'uTime': '1648300702545'}]}
        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'positions', 'instId': 'default'}, 'data': [{'posId': '885347000256208896', 'instId': 'BTCUSDT_UMCBL', 'instName': 'BTCUSDT', 'marginCoin': 'USDT', 'margin': '2.2165', 'marginMode': 'crossed', 'holdSide': 'long', 'holdMode': 'double_hold', 'total': '0.001', 'available': '0', 'locked': '0.001', 'averageOpenPrice': '44331', 'leverage': 20, 'achievedProfits': '0', 'upl': '-0.0047', 'uplRate': '-0.0021', 'liqPx': '0', 'keepMarginRate': '0.004', 'marginRate': '0.001793406219', 'cTime': '1646809094489', 'uTime': '1648300843616'}]}

        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'positions', 'instId': 'default'}, 'data': [{'posId': '885347000256208896', 'instId': 'BTCUSDT_UMCBL', 'instName': 'BTCUSDT', 'marginCoin': 'USDT', 'margin': '2.2165', 'marginMode': 'crossed', 'holdSide': 'long', 'holdMode': 'double_hold', 'total': '0.001', 'available': '0', 'locked': '0.001', 'averageOpenPrice': '44331', 'leverage': 20, 'achievedProfits': '0', 'upl': '-0.0957', 'uplRate': '-0.0431', 'liqPx': '0', 'keepMarginRate': '0.004', 'marginRate': '0.019703231142', 'cTime': '1646809094489', 'uTime': '1648301746614'}]}
        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'positions', 'instId': 'default'}, 'data': [{'posId': '885347000256208896', 'instId': 'BTCUSDT_UMCBL', 'instName': 'BTCUSDT', 'marginCoin': 'USDT', 'margin': '24.3348', 'marginMode': 'crossed', 'holdSide': 'long', 'holdMode': 'double_hold', 'total': '0.011', 'available': '0.01', 'locked': '0.001', 'averageOpenPrice': '44245.09', 'leverage': 20, 'achievedProfits': '0', 'upl': '-0.1082', 'uplRate': '-0.0044', 'liqPx': '0', 'keepMarginRate': '0.004', 'marginRate': '0.019751043735', 'cTime': '1646809094489', 'uTime': '1648301746760'}]}

        'holdSide' = side
        'total' = size
        '''
        self._logger.debug("_on_my_positions={}".format(message))


    def _on_my_orders(self,message):
        '''
        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'orders', 'instId': 'default'}, 'data': [{'accFillSz': '0', 'cTime': 1648300702404, 'clOrdId': '891603257300525057', 'force': 'normal', 'instId': 'BTCUSDT_UMCBL', 'lever': '20', 'notionalUsd': '44.3315', 'ordId': '891603257225027584', 'ordType': 'market', 'orderFee': [{'feeCcy': 'USDT', 'fee': '0'}], 'posSide': 'long', 'px': '0', 'side': 'buy', 'status': 'new', 'sz': '0.001', 'tdMode': 'cross', 'tgtCcy': 'USDT', 'uTime': 1648300702404}]}
        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'orders', 'instId': 'default'}, 'data': [{'accFillSz': '0.001', 'avgPx': '44331', 'cTime': 1648300702404, 'clOrdId': '891603257300525057', 'execType': 'T', 'fillFee': '-0.0265986', 'fillFeeCcy': 'USDT', 'fillNotionalUsd': '44.331', 'fillPx': '44331', 'fillSz': '0.001', 'fillTime': '1648300702475', 'force': 'normal', 'instId': 'BTCUSDT_UMCBL', 'lever': '20', 'notionalUsd': '44.3315', 'ordId': '891603257225027584', 'ordType': 'market', 'orderFee': [{'feeCcy': 'USDT', 'fee': '-0.0265986'}], 'pnl': '0', 'posSide': 'long', 'px': '0', 'side': 'buy', 'status': 'full-fill', 'sz': '0.001', 'tdMode': 'cross', 'tgtCcy': 'USDT', 'tradeId': '891603257598337027', 'uTime': 1648300702475}]}
        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'orders', 'instId': 'default'}, 'data': [{'accFillSz': '0', 'cTime': 1648300843616, 'clOrdId': '891603849586581504', 'force': 'normal', 'instId': 'BTCUSDT_UMCBL', 'lever': '20', 'notionalUsd': '44.332', 'ordId': '891603849469140992', 'ordType': 'limit', 'orderFee': [{'feeCcy': 'USDT', 'fee': '0'}], 'posSide': 'long', 'px': '44332', 'side': 'sell', 'status': 'new', 'sz': '0.001', 'tdMode': 'cross', 'tgtCcy': 'USDT', 'uTime': 1648300843616}]}

        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'orders', 'instId': 'default'}, 'data': [{'accFillSz': '0', 'cTime': 1648301746614, 'clOrdId': '891607637034704896', 'force': 'normal', 'instId': 'BTCUSDT_UMCBL', 'lever': '20', 'notionalUsd': '442.36', 'ordId': '891607636980178944', 'ordType': 'market', 'orderFee': [{'feeCcy': 'USDT', 'fee': '0'}], 'posSide': 'long', 'px': '0', 'side': 'buy', 'status': 'new', 'sz': '0.01', 'tdMode': 'cross', 'tgtCcy': 'USDT', 'uTime': 1648301746614}]}
        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'orders', 'instId': 'default'}, 'data': [{'accFillSz': '0.01', 'avgPx': '44236.5', 'cTime': 1648301746614, 'clOrdId': '891607637034704896', 'execType': 'T', 'fillFee': '-0.265419', 'fillFeeCcy': 'USDT', 'fillNotionalUsd': '442.365', 'fillPx': '44236.5', 'fillSz': '0.01', 'fillTime': '1648301746677', 'force': 'normal', 'instId': 'BTCUSDT_UMCBL', 'lever': '20', 'notionalUsd': '442.36', 'ordId': '891607636980178944', 'ordType': 'market', 'orderFee': [{'feeCcy': 'USDT', 'fee': '-0.265419'}], 'pnl': '0', 'posSide': 'long', 'px': '0', 'side': 'buy', 'status': 'full-fill', 'sz': '0.01', 'tdMode': 'cross', 'tgtCcy': 'USDT', 'tradeId': '891607637298962432', 'uTime': 1648301746677}]}

        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'orders', 'instId': 'default'}, 'data': [{'accFillSz': '0', 'cTime': 1648302536365, 'clOrdId': '891610949490483200', 'force': 'normal', 'instId': 'BTCUSDT_UMCBL', 'lever': '20', 'notionalUsd': '487.069', 'ordId': '891610949431762944', 'ordType': 'market', 'orderFee': [{'feeCcy': 'USDT', 'fee': '0'}], 'posSide': 'short', 'px': '0', 'side': 'sell', 'status': 'new', 'sz': '0.011', 'tdMode': 'cross', 'tgtCcy': 'USDT', 'uTime': 1648302536365}]}
        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'orders', 'instId': 'default'}, 'data': [{'accFillSz': '0.011', 'avgPx': '44278', 'cTime': 1648302536365, 'clOrdId': '891610949490483200', 'execType': 'T', 'fillFee': '-0.2922348', 'fillFeeCcy': 'USDT', 'fillNotionalUsd': '487.058', 'fillPx': '44278', 'fillSz': '0.011', 'fillTime': '1648302536431', 'force': 'normal', 'instId': 'BTCUSDT_UMCBL', 'lever': '20', 'notionalUsd': '487.058', 'ordId': '891610949431762944', 'ordType': 'market', 'orderFee': [{'feeCcy': 'USDT', 'fee': '-0.2922348'}], 'pnl': '0', 'posSide': 'short', 'px': '0', 'side': 'sell', 'status': 'full-fill', 'sz': '0.011', 'tdMode': 'cross', 'tgtCcy': 'USDT', 'tradeId': '891610949767323651', 'uTime': 1648302536431}]}

        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'orders', 'instId': 'default'}, 'data': [{'accFillSz': '0', 'cTime': 1648300843616, 'clOrdId': '891603849586581504', 'force': 'normal', 'instId': 'BTCUSDT_UMCBL', 'lever': '20', 'notionalUsd': '44.332', 'ordId': '891603849469140992', 'ordType': 'limit', 'orderFee': [{'feeCcy': 'USDT', 'fee': '0'}], 'posSide': 'long', 'px': '44332', 'side': 'sell', 'status': 'cancelled', 'sz': '0.001', 'tdMode': 'cross', 'tgtCcy': 'USDT', 'uTime': 1648302567591}]}
        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'orders', 'instId': 'default'}, 'data': [{'accFillSz': '0', 'cTime': 1648302587119, 'clOrdId': '891611162368188416', 'force': 'normal', 'instId': 'BTCUSDT_UMCBL', 'lever': '20', 'notionalUsd': '487.201', 'ordId': '891611162296885248', 'ordType': 'market', 'orderFee': [{'feeCcy': 'USDT', 'fee': '0'}], 'posSide': 'long', 'px': '0', 'side': 'sell', 'status': 'new', 'sz': '0.011', 'tdMode': 'cross', 'tgtCcy': 'USDT', 'uTime': 1648302587119}]}
        {'action': 'snapshot', 'arg': {'instType': 'umcbl', 'channel': 'orders', 'instId': 'default'}, 'data': [{'accFillSz': '0.011', 'avgPx': '44291', 'cTime': 1648302587119, 'clOrdId': '891611162368188416', 'execType': 'T', 'fillFee': '-0.2923206', 'fillFeeCcy': 'USDT', 'fillNotionalUsd': '487.201', 'fillPx': '44291', 'fillSz': '0.011', 'fillTime': '1648302587151', 'force': 'normal', 'instId': 'BTCUSDT_UMCBL', 'lever': '20', 'notionalUsd': '487.201', 'ordId': '891611162296885248', 'ordType': 'market', 'orderFee': [{'feeCcy': 'USDT', 'fee': '-0.2923206'}], 'pnl': '0.504999999999', 'posSide': 'long', 'px': '0', 'side': 'sell', 'status': 'full-fill', 'sz': '0.011', 'tdMode': 'cross', 'tgtCcy': 'USDT', 'tradeId': '891611162502422529', 'uTime': 1648302587151}]}

        '''
        self._logger.debug("_on_my_orders={}".format(message))

        self._order_dict_lock.wait()
        for d in message.get('data',[]) :
            id = d.get('ordId')
            if d['status']=='new' :
                if self.my.order.update_order(id, side=d['side'].upper(), price=float(d['px']), size=float(d['sz'])) :
                    self._logger.debug( self.__class__.__name__ + " : [order] New :"+ str(d) )

                else:
                    self._logger.trace( self.__class__.__name__ + " : Not My order : " + str(id) )

            elif d['status']=='full-fill' :
                if self.my.order.executed( id=id, side=d['side'].upper(), price=float(d['fillPx']), size=float(d['accFillSz']) ) :
                    self._logger.debug( self.__class__.__name__ + " : [order] Filled : " + str(d) )

                    # --------------------------------------------------------------------------------

                    if self.is_linear() :
                        self.my.position.executed( id, price=float(d['fillPx']), side=d['side'].upper(), size=float(d['accFillSz']), commission=float(d['fillFee']) )
                    else:
                        self.my.position.executed( id, price=float(d['fillPx']), side=d['side'].upper(), size=float(d['accFillSz']), commission=float(d['fillFee'])/self.my.position.ref_ltp )
                    # --------------------------------------------------------------------------------

                else:
                    self._logger.trace( self.__class__.__name__ + " : Not My order : " + str(id) )

            elif d['status']=='cancelled' :
                if self.my.order.remove_order( id ) :
                    self._logger.debug( self.__class__.__name__ + " : [order] Canceled : " + str(d) )
                else:
                    self._logger.trace( self.__class__.__name__ + " : Not My order : " + str(id) )

            else:
                self._logger.debug( self.__class__.__name__ + " : [order] Unknown : " + str(d) )


    # wsでローソク足を受信したら登録されたターゲット足を生成
    async def _wait_candle_event(self):
        while self._logger.running:
            await self.candle.wait()
            with self._datastore_lock :
                self.candle_store.resample()

            # イベントが都度発生しないようにまとめてローソク足のデータストアへ入れる
            for c in self.candle_store._target_candle_list:
                pool = c.pop('pool')
                c['pool']=[]
                for p in pool:
                    c['target_candle']._onmessage(p)

    async def get_candles( self, timeframe, num_of_candle=500, symbol=None, since=None, need_result=True ):

        if need_result:
            self.create_candle_class('temp_candle')

        target_symbol = symbol or self.symbol

        # 初めて取得するシンボルであればwsを購読
        if (not need_result) and (target_symbol not in self._candle_subscribed):
            send_json = self._subscribe(handler=None, key="candle1m", instType='mc', channel="candle1m", instId=target_symbol)
            self._candle_subscribed.append(target_symbol)
            self.ws.send_subscribe( {"op": "subscribe", "args": [send_json]}, self._ws_args )

        insttype = 'UMCBL' if self.is_linear(target_symbol) else 'DMCBL'

        await self._candle_api_lock.acquire()

        # 基本の分足を入れるDataStoreクラスをローソク足変換クラスへ登録
        exchange_symbol = self.__class__.__name__+target_symbol
        self.candle_store.set_min_candle(exchange_symbol,self.candle)

        api_timeframe = 60
        timeframe_sec = timeframe*api_timeframe

        endtime = int(datetime.now(tz=timezone.utc).timestamp()//api_timeframe*api_timeframe)
        starttime = (endtime//timeframe_sec*timeframe_sec)-timeframe_sec*num_of_candle-api_timeframe
        keep_running = True

        while keep_running and starttime<=endtime and num_of_candle!=0:

            self._logger.debug( "starttime: {}".format(datetime.fromtimestamp(starttime,tz=timezone(timedelta(hours=9),'JST'))) )
            self._logger.debug( "endtime  : {}".format(datetime.fromtimestamp(endtime,tz=timezone(timedelta(hours=9),'JST'))) )
            limittime = endtime-api_timeframe*100  # 最大取得本数は100本

            self._logger.debug( "limittime  : {}".format(datetime.fromtimestamp(limittime,tz=timezone(timedelta(hours=9),'JST'))) )

            try:
                responce = ''
                await self.check_public_request_limit('getcandles')
                res =await self._client.get('/market/candles', params={'symbol': target_symbol+'_'+insttype, 'granularity':60, 'startTime':max(starttime,limittime)*1000, 'endTime':endtime*1000 })
                self._update_public_limit(res)
                responce = await res.json()
                await asyncio.sleep(0.5)

                if type(responce)==dict :
                    self._logger.error( "responce : {}".format(responce) )
                    continue

                # 得られたデータが空っぽなら終了
                if type(responce)!=list or len(responce)==0 :
                    keep_running = False
                    self._logger.debug( "{} : Fetch candles from API (request {}-{}) [empty]".format(target_symbol,
                        datetime.fromtimestamp(max(starttime,limittime),tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M'),
                        datetime.fromtimestamp(endtime,tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M') ) )
                    break

                data = [c  for c in responce if starttime<=int(c[0])/1000<=endtime]

                self._logger.info( "{} : Fetch {} candles timeframe{} from API [{}~{}]".format(target_symbol,len(data),api_timeframe,
                    datetime.fromtimestamp(int(data[0][0])/1000,tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M'),
                    datetime.fromtimestamp(int(data[-1][0])/1000,tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M')) )

                # 分足データストアに保管
                with self._datastore_lock :
                    self.candle._onmessage({'arg':{'instId':target_symbol, 'channel':'1m'}, 'data': data})

                # データ取得用一時ストア
                if need_result:
                    self._stores.get('temp_candle')._onmessage({'symbol': target_symbol, 'interval': timeframe_sec, 'kline': data})

                # 取得した分足が8000本を超えるようなら一度リサンプルを行う
                if len(self.candle)>8000:
                    with self._datastore_lock :
                        self.candle_store.resample(symbol=target_symbol)

                # 次回の取得開始時
                endtime = int(int(responce[0][0])/1000-api_timeframe)

                # 得られたデータが目的のendtimeを超えていたら終了
                if int(responce[0][0])/1000 < starttime:
                    keep_running = False

            except Exception:
                if responce :
                    self._logger.info( responce )
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
        df = df.set_index('timestamp').sort_index().groupby(level=0).last()

        agg_param = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
        df = df.resample(f'{timeframe*60}S').agg(agg_param)
        df['close'].fillna(method='ffill', inplace=True)
        df['open'].fillna(df['close'], inplace=True)
        df['high'].fillna(df['close'], inplace=True)
        df['low'].fillna(df['close'], inplace=True)
        df['volume'].fillna(0, inplace=True)

        df = df.tz_convert(timezone(timedelta(hours=9), 'JST'))
        df = df.astype({'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float', 'volume': 'float'})

        self._candle_api_lock.release()
        return df.tail(num_of_candle)

    def create_candle_class(self,id):
        self.create(id, datastore_class=Kline)


    # APIからTickerを取得
    async def ticker_api(self, **kwrgs):
        try:
            symbol = kwrgs.get('symbol', self.symbol)
            insttype = 'UMCBL' if self.is_linear(symbol) else 'DMCBL'
            await self.check_public_request_limit('getticker')
            res = await self._client.get('/market/ticker', params={'symbol': symbol+'_'+insttype})
            self._update_public_limit(res)
            if res.status==200:
                data = await res.json()
                self.my.position.ref_ltp = float(data.get('data',{}).get('last',0))

                return {'stat':int(data['code']) or 0 , 'ltp':float(data.get('data',{}).get('last',0)), 'msg':data}
            else:
                return {'stat': res.status, 'msg': res.reason}
        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

    # APIから現在ポジを取得
    async def getpositions(self, **kwrgs):
        try:
            symbol = kwrgs.get('symbol', self.symbol)
            insttype = 'UMCBL' if self.is_linear(symbol) else 'DMCBL'
            await self.check_private_request_limit('getpositions')
            r = await self._client.get('/position/allPosition', params={'productType': insttype.lower()})
            self._update_private_limit(r)
            res = await r.json()
        except Exception:
            self._logger.error(traceback.format_exc())
            return []

        if int(res['code'])==0 :
            data = [{'side':'BUY' if p['holdSide']=='long' else 'SELL', 'size':p['total']} for p in res.get("data",[]) if p['symbol']==symbol.upper()+'_'+insttype]
            return data
        else:
            return []

    # 証拠金口座の証拠金額 (coinが指定されていない場合には取引するシンボルに使用する通貨、指定されていれば指定された通貨)
    async def getcollateral(self, coin=None):

        target_coin = self._collateral_coin.get(self.symbol,'USD')

        try:
            res= await self.getbalance()
            if int(res.get('code',-1))!=0 :
                return {'stat': int(res.get('code',-1)), 'msg': res}

            balance = 0
            for r in res['data'] :
                try:
                    if r['marginCoin']==(coin or target_coin) :
                        balance = float(r['available'])
                except:
                    pass
            return {'stat': 0, 'collateral': balance, 'msg': res}

        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'core': -999, 'msg': str(e)}

    async def getbalance(self):
        try:
            insttype = 'UMCBL' if self.is_linear(self.symbol) else 'DMCBL'
            await self.check_private_request_limit('getbalance')
            r = await self._client.get('/account/accounts', params={'productType':insttype, })
            self._update_private_limit(r)
            return await r.json()
        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

    # 発注
    async def sendorder(self, order_type, side, size, price=0, auto_cancel_after=2592000, **kwargs ):
        """
        sendorder( order_type='LIMIT',             # 指値注文の場合は "LIMIT", 成行注文の場合は "MARKET" を指定します。
                   side='BUY',                     # 買い注文の場合は "BUY", 売り注文の場合は "SELL" を指定します。
                   size=1000,                      # 注文数量を指定します。
                   price=4800000,                  # 価格を指定します。order_type に "LIMIT" を指定した場合は必須です。
                   auto_cancel_after,              # 0 以外を指定すると指定秒数経過後キャンセルを発行します
                   symbol="BTCUSD"                 # 省略時には起動時に指定した symbol が選択されます
                   time_in_force="normal",         # 執行数量条件 を "normal", "postOnly", "ioc", "fok" で指定します。
                   reduce_only,                    # True means your position can only reduce in size if this order is triggered
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

        if self.RateLimitRemaining_private<2:
            self._logger.info("RateLimitRemaining_private : {}/10".format(self.RateLimitRemaining_private) )
            return {'stat': -999, 'msg': "RateLimitRemaining_private "+str(self.RateLimitRemaining_private)+"/10", 'ids': []}

        if self.PendingUntil > time.time() :
            self._logger.info("Order pending : {:.1f}sec".format(self.PendingUntil-time.time()) )
            return {'stat': -999, 'msg': "Order pending : {:.1f}sec".format(self.PendingUntil-time.time()), 'ids': []}

        adjust_flag = kwargs.get('adjust_flag', False)

        # 対応しているオプションだけにする
        params={}
        for k,v in kwargs.items():
            if k=='timeInForceValue' :
                if v in ["normal", "postOnly", "ioc", "fok"] :
                    params[k]=v
            elif k in ['presetTakeProfitPrice','presetStopLossPrice']:
                params[k]=v

        symbol = kwargs.get('symbol', self.symbol).upper()
        params['symbol']=symbol
        params['orderType']=order_type.lower()
        params['side']=side.lower() #'open_long open_short close_long close_short'
        params['size']=size
        params['price']=self.round_order_price(price, symbol=symbol)
        params['marginCoin'] = self._collateral_coin.get(symbol,'USD')

        # ノートレード期間はポジ以上のクローズは行わない
        if self.noTrade :
            params['size'] = min(abs(self.my.position.size),params['size'])
            self._logger.info("No trade period (change order size to {})".format(params['size']) )

        # 最低取引量のチェック（無駄なAPIを叩かないように事前にチェック）
        if params['size']<self.minimum_order_size(symbol=symbol) :
            return {'stat': -153, 'msg': '最低取引数量を満たしていません', 'ids': []}

        # クローズオーダー可能なポジションを探す
        positions = self.positions.find()
        available=sum([float(p['available']) for p in positions if p['instName']==symbol and p['holdSide']==('short' if side.lower()=='buy' else 'long')])
        available = min( params['size'], available )
        remain = round(max(params['size']-available,0 ),8)

        insttype = 'UMCBL' if self.is_linear(symbol) else 'DMCBL'
        params['symbol'] = symbol+'_'+insttype

        ordered_id_list = []

        if available>=self.minimum_order_size(symbol=symbol) :
            params['size'] = available
            params['side'] = 'close_short' if side.lower()=='buy' else 'close_long'

            with self._order_dict_lock :
                try:
                    await self.check_private_request_limit(params)
                    self._logger.info("[send closeorder] : {}".format(params) )
                    r = await self._client.post('/order/placeOrder', data=params)
                    self._update_private_limit(r)
                    res = await r.json()
                except Exception as e:
                    self._logger.error(traceback.format_exc())
                    return {'stat': -999, 'msg': str(e), 'ids':[]}

                ret_code = int(res.get('code',-999))
                if ret_code==0 : 
                    r = res.get('data',{})
                    if self.my and not adjust_flag:
                        self.my.order.new_order( symbol=symbol, id=r['orderId'] , side=side.upper(), price=params['price'], size=params['size'], expire=time.time()+auto_cancel_after, invalidate=time.time()+2592000 )
                    ordered_id_list.append(r['orderId'])

                #{'code': '40715', 'msg': 'delegate count can not high max of open count', 'requestTime': 1649405043039, 'data': None}
                elif ret_code==40715 : 
                    remain = size

                else: 
                    self._logger.error("Send order param : {}".format(params) )
                    self._logger.error("Error response [sendorder] : {}".format(res) )
                    return {'stat':ret_code , 'msg':res.get('msg'), 'ids':[]}

        # 決済だけでオーダー出し終わった場合
        if remain==0 :
            return {'stat':0 , 'msg':"", 'ids':ordered_id_list}

        # 新規オーダー
        params['size'] = remain
        params['side']='open_long' if side.lower()=='buy' else 'open_short'

        with self._order_dict_lock :
            try:
                await self.check_private_request_limit(params)
                self._logger.info("[sendorder] : {}".format(params) )
                r = await self._client.post('/order/placeOrder', data=params)
                self._update_private_limit(r)
                res = await r.json()
            except Exception as e:
                self._logger.error(traceback.format_exc())
                return {'stat': -999, 'msg': str(e), 'ids':[]}

            ret_code = int(res.get('code',-999))
            if ret_code==0 : 
                r = res.get('data',{})
                if self.my and not adjust_flag:
                    self.my.order.new_order( symbol=symbol, id=r['orderId'] , side=side.upper(), price=params['price'], size=params['size'], expire=time.time()+auto_cancel_after, invalidate=time.time()+2592000 )
                ordered_id_list.append(r['orderId'])
                return {'stat':0 , 'msg':"", 'ids':ordered_id_list}

            else: 
                self._logger.error("Send order param : {}".format(params) )
                self._logger.error("Error response [sendorder] : {}".format(res) )
                return {'stat':ret_code , 'msg':res.get('msg'), 'ids':ordered_id_list}


    # キャンセル
    async def cancelorder(self, id, **kwargs ):
        """
        cancelorder( id                      # キャンセルする注文の ID です。
                   symbol="BTCUSD"           # 省略時には起動時に指定した symbol が選択されます
                  ) )

        return: { 'stat': エラーコード,      # キャンセル成功時 0
                  'msg':  エラーメッセージ,
                }
        """

        if id not in self.my.order.order_dict :
            self._logger.info("ID:{} is already filled or canceld or expired".format(id) )
            return {'stat':-999 , 'msg':"Order is already filled or canceld or expired"}

        if self.RateLimitRemaining_private<2:
            self._logger.info("RateLimitRemaining_private : {}/10".format(self.RateLimitRemaining_private) )
            return {'stat': -999, 'msg': "RateLimitRemaining_private "+str(self.RateLimitRemaining_private)+"/10", 'ids': []}

        kwargs['orderId']=id

        symbol = kwargs.get("symbol", self.my.order.order_dict[id]['symbol'] )
        insttype = 'UMCBL' if self.is_linear(symbol) else 'DMCBL'
        kwargs['symbol'] = symbol+'_'+insttype
        kwargs['marginCoin'] = self._collateral_coin.get(symbol,'USD')

        try:
            await self.check_private_request_limit(kwargs)
            self._logger.info("[cancelorder] : {}".format(kwargs) )
            self.my.order.mark_as_invalidate( id )
            r = await self._client.post('/order/cancel-order', data=kwargs)
            self._update_private_limit(r)
            res = await r.json()
        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

        ret_code = int(res.get('code',-999))

        if ret_code==0 : 
            return {'stat': 0, 'msg': ""}

        self._logger.error("Error response [cancelorder] : {}".format(res) )
        return {'stat':ret_code , 'msg':res.get('msg')}
