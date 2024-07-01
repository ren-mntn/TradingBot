# coding: utf-8
#!/usr/bin/python3

import asyncio
from collections import deque
from datetime import datetime, timedelta, timezone
import random
import time
import traceback
import pandas as pd

import pybotters
from pybotters.store import DataStore
from libs.utils import TimeConv, LockCounter
from libs.market import *
from libs.exchanges.base_module import MultiProc_WS, RestAPIExchange, WebsocketExchange
from libs.account import AccountInfo, OpenPositionKeepAve, OpenPositionKeepAveLinear

# https://github.com/phemex/phemex-api-docs/
# https://github.com/phemex/phemex-api-docs/blob/master/Public-Contract-API-en.md
# https://github.com/phemex/phemex-python-api/blob/master/phemex/client.py
# https://github.com/ccxt/ccxt/blob/master/python/ccxt/phemex.py

class Kline(DataStore):
    _KEYS = ['symbol', 'interval', 'timestamp']
    _MAXLEN = 3000000

    def _onmessage(self, message):
        symbol= message.get('symbol')
        interval= message.get('interval')
        self._insert([{'symbol':symbol, 'interval':interval,
                       'timestamp':int(item[0]/1000), 'open':float(item[1]), 'high':float(item[2]), 'low':float(item[3]), 'close':float(item[4]), 'volume':float(item[5]), 'turnover':float(item[6])}
                       for item in message.get('kline',[])])

class Phemex(pybotters.PhemexDataStore, TimeConv, RestAPIExchange, WebsocketExchange):

    # インバースタイプ契約かリニアタイプ契約か (証拠金通貨がUSDならリニア契約)
    @property
    def is_linear(self):
        return self._collateral_coin.get(self.symbol,'USD')=="USD"

    def units(self,value=0):
        if self.is_linear :
            return {'unitrate' :1,                                # 損益額をプロフィットグラフに表示する単位に変換する係数
                    'profit_currency' : 0,                        # 利益額の単位 0:USD 1:JPY
                    'title':"USD {:+,.2f}".format(value),         # 表示フォーマット
                    'pos_rate':self._order_rate[self.symbol]}     # ポジションをcurrency単位に変換する係数
        else:
            return {'unitrate' :self.execution_info.last,         # 損益額をプロフィットグラフに表示する単位に変換する係数
                    'profit_currency' : 0,                        # 利益額の単位 0:USD 1:JPY
                    'title':"USD {:+,.2f}".format(value),         # 表示フォーマット
                    'pos_rate':float(1/self.execution_info.last)} # ポジションをcurrency単位に変換する係数

    def __init__(self, logger, candle_store, apikey=('',''), testnet=False):
        self._logger = logger
        self.candle_store = candle_store   # 分ローソク足変換クラス
        self.auth = (apikey[0]!='' and apikey[1]!='')
        self._apikey = apikey

        if testnet :
            self._api_url = "https://testnet-api.phemex.com"
            self._endpoint = "wss://testnet.phemex.com/ws"
            self.exchange_name = "phemex_testnet"
        else:
            self._api_url = "https://api.phemex.com"
            self._endpoint = "wss://phemex.com/ws"
            self.exchange_name = "phemex"

        self._client = pybotters.Client(apis={self.exchange_name: [apikey[0],apikey[1]]}, base_url=self._api_url)
        self._candle_api_lock = asyncio.Lock()
        self._datastore_lock = LockCounter(self._logger, self.__class__.__name__)
        self._order_dict_lock = LockCounter(self._logger, "order_dict")
        self._candle_subscribed = []
        self._candle_subscribe_id = 300
        super().__init__()

        self.PendingUntil = time.time()
        self.RateLimitRemaining = 500
        self._request_count = deque(maxlen=300)

        RestAPIExchange.__init__(self)
        WebsocketExchange.__init__(self)

    # APIリミットの管理
    def _update_api_limit(self,r):
        if r.status != 200 :
            self._logger.error( "{}: {}".format(r.status, r.reason) )
        self.RateLimitRemaining = int(r.headers.get('X-RateLimit-Remaining-CONTRACT',self.RateLimitRemaining))
        if 'X-RateLimit-Retry-After-CONTRACT' in r.headers :
            self.PendingUntil = max(self.api.PendingUntil, time.time()+r.headers['X-RateLimit-Retry-After-CONTRACT'])

    async def check_request_limit(self):
        # 前回オーダーから60秒以上経っていたらカウンターを初期化
        if len(self._request_count)!=0 :
            t = self._request_count.pop()
            if time.time()-t>60 :
                self.RateLimitRemaining = 500
            self._request_count.append(t)

        # 今回の時間を追加
        self._request_count.append(time.time())
        if len(self._request_count)<300 :        # 300回
            return
        t = self._request_count.popleft()
        w = t+60-time.time()                     # 60秒あたり
        if w>0 :
            self._logger.info( "public request sleep : {:.3f}".format(w) )
            await asyncio.sleep(w)
        return

    @property
    def api_remain1(self):
        return self.RateLimitRemaining

    @property
    def api_remain2(self):
        return 500

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
        return self._stores.get('kline')

    @property
    def positions(self):
        return self._stores.get('positions')

    @property
    def orders(self):
        return self._stores.get('orders')

    @property
    def account(self):
        return self._stores.get('accounts')

    async def start(self, subscribe={}, symbol='BTCUSD'):

        self._param = subscribe
        self.symbol = symbol

        # マーケット情報の取得
        await self.check_request_limit()
        res = await self._client.get('/public/products')
        self._update_api_limit(res)
        data = await res.json()
        market_info = [s for s in data['data']['products'] if s['type']=='Perpetual']

        # シンボルごとの価格呼び値（BTCなど指定のない物は1）
        self._price_unit_dict = dict([(s['symbol'],s['tickSize']) for s in market_info])

        # 最小取引単位（発注ロットの倍率が指定されるので、オーダー単位としては最小取引単位は１）
        self._minimum_order_size_dict = dict([(s['symbol'],1) for s in market_info])

        # 取引するシンボルに使用する証拠金通貨 (リストにない物はUSD)
        self._collateral_coin = dict([(s['symbol'],s['settleCurrency']) for s in market_info])

        # ticker取得のための通貨
        self._currency = dict([(s['symbol'],s['indexSymbol']) for s in market_info])

        # 発注ロットの倍率
        self._order_rate = dict([(s['symbol'],s['contractSize']) for s in market_info])

        # ポジション管理クラス
        if self.is_linear :
            self.my = AccountInfo(self._logger, OpenPositionKeepAveLinear, order_rate=self._order_rate[self.symbol], order_currency=self._currency[self.symbol])
        else:
            self.my = AccountInfo(self._logger, OpenPositionKeepAve, order_currency='USD')

        # データ保存用クラス
        self.board_info = BoardInfo( self._logger )
        self.execution_info = ExecuionInfo( self._logger )
        self.ticker_info = TickerInfo()
        self.ticker_info.open_interest = 0

        # 指定のチャンネルの購読
        self._handler = {'None':None}
        self._ws_args = []
        if self._param.get('execution', True) : self._subscribe(handler=self._on_executions, key="trades", id=100, method="trade.subscribe", params=[self.symbol])
        if self._param.get('ticker', False) :   self._subscribe(handler=self._on_ticker, key="tick", id=101, method="tick.subscribe", params=[self._currency[self.symbol]])
        if self._param.get('board', False) :    self._subscribe(handler=self._on_board, key="book", id=102, method="orderbook.subscribe", params=[self.symbol])

        if self.auth:
            self._subscribe(handler=self._on_my_orders, key="aop", id=200, method="aop.subscribe", params=[])

        # ローソク足受信処理タスクを開始
        asyncio.create_task(self._wait_candle_event(), name="phemex_candle")

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
        asyncio.create_task(self.ws._start_websocket(logging=False), name="phemex_websocket")

        # 自動キャンセルタスクを開始
        asyncio.create_task(self._cancel_task(), name="phemex_cancel")

        if self.auth:
            while not (self.ws.pid) :
                await asyncio.sleep(1)
            self._logger.info("Websocket connected")

        return self

    def _subscribe(self, handler, key, **argv):
        self._logger.debug( "subscribe : {}".format(argv) )
        self._handler[key]=handler
        self._ws_args.append(argv)
        return argv

    def _onmessage(self, msg, ws):
        self._logger.debug("recv phemex websocket [{}]".format(msg.keys()))
        self._logger.trace("recv data [{}]".format(msg))

        id = msg.get('id')
        result = msg.get('result')
        topic_handler = None
        if not id:
            if 'trades' in msg :
                topic_handler = self._handler.get('trades')
            elif 'book' in msg :
                topic_handler = self._handler.get('book')
            elif 'tick' in msg :
                topic_handler = self._handler.get('tick')
            elif ('accounts' in msg) or ('orders' in msg) or ('positions' in msg):
                topic_handler = self._handler.get('aop')

            if topic_handler != None:
                topic_handler(msg)

                if topic_handler in [self._on_ticker, self._on_my_orders] :
                    super()._onmessage(msg,ws)

            else:
                super()._onmessage(msg,ws)

        elif result!='pong':
            super()._onmessage(msg,ws)

    def _on_executions(self,msg):
        if msg.get('type','snapshot')=='snapshot' : return # 初回のsnapshotは読み込まない
        trades = msg['trades']
        self.execution_info.time = self._epoc_to_dt(int(trades[0][0])/1000000000)
        for t in trades:
            self.execution_info.append_execution(float(t[2])/10000,float(t[3]),"BUY" if t[1]=="Buy" else "SELL", self._epoc_to_dt(int(t[0])/1000000000))
        self.execution_info.append_latency(self._jst_now_dt().timestamp()*1000 - int(trades[0][0])/1000000)
        self.my.position.ref_ltp = self.execution_info.last
        self.execution_info._event.set()

    def _on_ticker(self,msg):
        tick = msg['tick']
        self.ticker_info.time = self._epoc_to_dt(int(tick['timestamp'])/1000000000)
        self.ticker_info.last = float(tick.get('last',self.ticker_info.last*10000))/10000
        self.my.position.ref_ltp = self.ticker_info.last


    def _on_board(self,msg):
        book = msg['book']
        self.board_info.time = self._epoc_to_dt(int(msg['timestamp'])/1000000000)
        msg_type=msg.get('type')    
        if msg_type=='snapshot' :
            self.board_info.initialize_dict()
        if self.is_linear :
            rate = self._order_rate[self.symbol]
            self.board_info.update_asks([(p/10000,s*rate) for p,s in book.get('asks',[])])
            self.board_info.update_bids([(p/10000,s*rate) for p,s in book.get('bids',[])])
        else:
            self.board_info.update_asks([(p/10000,s/p*10000) for p,s in book.get('asks',[])])
            self.board_info.update_bids([(p/10000,s/p*10000) for p,s in book.get('bids',[])])
        self.board_info.event.set()
        for event in self.board._events:
            event.set()


    def _on_my_orders(self,message):
        '''
        {'accountID': 1111111111, 'action': 'New', 'actionBy': 'ByUser', 'actionTimeNs': 1615290931272863133, 'addedSeq': 5863708589, 'bonusChangedAmountEv': 0, 'clOrdID': 'BFSX2-1615290930', 'closedPnlEv': 0, 'closedSize': 0, 'code': 0, 'cumQty': 0, 'cumValueEv': 0, 'curAccBalanceEv': 88345, 'curAssignedPosBalanceEv': 0, 'curBonusBalanceEv': 75330, 'curLeverageEr': 0, 'curPosSide': 'None', 'curPosSize': 0, 'curPosTerm': 8, 'curPosValueEv': 0, 'curRiskLimitEv': 10000000000, 'currency': 'BTC', 'cxlRejReason': 0, 'displayQty': 0, 'execFeeEv': 0, 'execID': '00000000-0000-0000-0000-000000000000', 'execPriceEp': 0, 'execQty': 0, 'execSeq': 5863708589, 'execStatus': 'New', 'execValueEv': 0, 'feeRateEr': 0, 'leavesQty': 1, 'leavesValueEv': 1923, 'message': 'No error', 'nthItem': 1, 'ordStatus': 'New', 'ordType': 'Limit', 'orderID': '2ceb1d43-4ca5-4083-9c5e-ccb17a84226e', 'orderQty': 1, 'pegOffsetValueEp': 0, 'platform': 'API', 'priceEp': 520000000, 'relatedPosTerm': 8, 'relatedReqNum': 81, 'side': 'Buy', 'slTrigger': 'ByMarkPrice', 'stopLossEp': 0, 'stopPxEp': 0, 'symbol': 'BTCUSD', 'takeProfitEp': 0, 'timeInForce': 'GoodTillCancel', 'totalItems': 1, 'tpTrigger': 'ByLastPrice', 'transactTimeNs': 1615290931275994937, 'userID': 778748, 'vsAccountID': 0, 'vsUserID': 0}
        {'accountID': 1111111111, 'action': 'New', 'actionBy': 'ByUser', 'actionTimeNs': 1615378305577855512, 'addedSeq': 5890866606, 'bonusChangedAmountEv': 0, 'clOrdID': 'BFSX2-1615378305', 'closedPnlEv': 11, 'closedSize': 3, 'code': 0, 'cumQty': 3, 'cumValueEv': 5431, 'curAccBalanceEv': 88099, 'curAssignedPosBalanceEv': 781, 'curBonusBalanceEv': 73805, 'curLeverageEr': 0, 'curPosSide': 'Buy', 'curPosSize': 37, 'curPosTerm': 108, 'curPosValueEv': 67129, 'curRiskLimitEv': 10000000000, 'currency': 'BTC', 'cxlRejReason': 0, 'displayQty': 0, 'execFeeEv': -1, 'execID': 'adcf6d13-9c73-54a7-a51c-af8278b75db0', 'execPriceEp': 552310000, 'execQty': 3, 'execSeq': 5891046209, 'execStatus': 'MakerFill', 'execValueEv': 5431, 'feeRateEr': -25000, 'lastLiquidityInd': 'AddedLiquidity', 'leavesQty': 67, 'leavesValueEv': 121308, 'message': 'No error', 'nthItem': 2, 'ordStatus': 'PartiallyFilled', 'ordType': 'Limit', 'orderID': '35b6bbca-2fa9-4757-a1cc-c9a33e5d10a2', 'orderQty': 70, 'pegOffsetValueEp': 0, 'platform': 'API', 'priceEp': 552310000, 'relatedPosTerm': 108, 'relatedReqNum': 1402, 'side': 'Sell', 'slTrigger': 'ByMarkPrice', 'stopLossEp': 0, 'stopPxEp': 0, 'symbol': 'BTCUSD', 'takeProfitEp': 0, 'timeInForce': 'GoodTillCancel', 'totalItems': 3, 'tpTrigger': 'ByLastPrice', 'tradeType': 'Trade', 'transactTimeNs': 1615378830054782485, 'userID': 778748, 'vsAccountID': 8293920001, 'vsUserID': 829392}
        {'accountID': 1111111111, 'action': 'Cancel', 'actionBy': 'ByUser', 'actionTimeNs': 1615290941440697507, 'addedSeq': 5863708589, 'bonusChangedAmountEv': 0, 'clOrdID': 'BFSX2-1615290930', 'closedPnlEv': 0, 'closedSize': 0, 'code': 0, 'cumQty': 0, 'cumValueEv': 0, 'curAccBalanceEv': 88345, 'curAssignedPosBalanceEv': 0, 'curBonusBalanceEv': 75330, 'curLeverageEr': 0, 'curPosSide': 'None', 'curPosSize': 0, 'curPosTerm': 8, 'curPosValueEv': 0, 'curRiskLimitEv': 10000000000, 'currency': 'BTC', 'cxlRejReason': 0, 'displayQty': 0, 'execFeeEv': 0, 'execID': '00000000-0000-0000-0000-000000000000', 'execPriceEp': 0, 'execQty': 1, 'execSeq': 5863711780, 'execStatus': 'Canceled', 'execValueEv': 0, 'feeRateEr': 0, 'leavesQty': 0, 'leavesValueEv': 0, 'message': 'No error', 'nthItem': 1, 'ordStatus': 'Canceled', 'ordType': 'Limit', 'orderID': '2ceb1d43-4ca5-4083-9c5e-ccb17a84226e', 'orderQty': 1, 'pegOffsetValueEp': 0, 'platform': 'API', 'priceEp': 520000000, 'relatedPosTerm': 8, 'relatedReqNum': 82, 'side': 'Buy', 'slTrigger': 'ByMarkPrice', 'stopLossEp': 0, 'stopPxEp': 0, 'symbol': 'BTCUSD', 'takeProfitEp': 0, 'timeInForce': 'GoodTillCancel', 'totalItems': 1, 'tpTrigger': 'ByLastPrice', 'transactTimeNs': 1615290941445892128, 'userID': 778748, 'vsAccountID': 0, 'vsUserID': 0}
        '''
        if message.get('type')=='snapshot' : return

        self._logger.debug("_on_my_orders={}".format(message))

        self._order_dict_lock.wait()
        for d in message.get('orders',[]) :
            id = d.get('orderID')
            if d['ordStatus']=='New' :
                if self.my.order.update_order(id, side=d['side'].upper(), price=float(d['priceEp'])/10000, size=d['orderQty']) :
                    self._logger.debug( self.__class__.__name__ + " : [order] "+d['action']+" : " + str(d) )

                else:
                    self._logger.trace( self.__class__.__name__ + " : Not My order : " + str(id) )

            elif d['ordStatus']=='Canceled' and d['action']!='Replace':
                if self.my.order.remove_order( id ) :
                    self._logger.debug( self.__class__.__name__ + " : [order] Canceled : " + str(d) )

                else:
                    self._logger.trace( self.__class__.__name__ + " : Not My order : " + str(id) )

            elif d['ordStatus']=='Filled' :
                if self.my.order.executed( id=id, side=d['side'].upper(), price=d['execPriceEp']/10000, size=d['execQty'] ) :
                    self._logger.debug( self.__class__.__name__ + " : [order] Filled : " + str(d) )
                    if self.is_linear :
                        self.my.position.executed( id, price=d['execPriceEp']/10000, side=d['side'].upper(), size=d['execQty'],
                                                    commission=self._round(-d['execQty']*self._order_rate[self.symbol]*d['execPriceEp']*d['feeRateEr']/1000000000000) )
                    else:
                        self.my.position.executed( id, price=d['execPriceEp']/10000, side=d['side'].upper(), size=d['execQty'],
                                                    commission=self._round(-d['execQty']*self._order_rate[self.symbol]/d['execPriceEp']*d['feeRateEr']/10000) )
                else:
                    self._logger.trace( self.__class__.__name__ + " : Not My order : " + str(id) )

            elif d['ordStatus']=='PartiallyFilled' :
                if self.my.order.executed( id=id, side=d['side'].upper(), price=d['execPriceEp']/10000, size=d['execQty'] ) :
                    self._logger.debug( self.__class__.__name__ + " : [order] PartiallyFilled : " + str(d) )
                    if self.is_linear :
                        self.my.position.executed( id, price=d['execPriceEp']/10000, side=d['side'].upper(), size=d['execQty'],
                                                    commission=self._round(-d['execQty']*self._order_rate[self.symbol]*d['execPriceEp']*d['feeRateEr']/1000000000000) )
                    else:
                        self.my.position.executed( id, price=d['execPriceEp']/10000, side=d['side'].upper(), size=d['execQty'],
                                                    commission=self._round(-d['execQty']*self._order_rate[self.symbol]/d['execPriceEp']*d['feeRateEr']/10000) )
                else:
                    self._logger.trace( self.__class__.__name__ + " : Not My order : " + str(id) )

            else:
                self._logger.debug( self.__class__.__name__ + " : [order] Unknown : " + str(d) )

    # 損益額の丸め
    def _round(self,value):
        return round(value-0.000000005,8)

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
            send_json = self._subscribe(handler=None, key="kline", id=self._candle_subscribe_id, method="kline.subscribe", params=[target_symbol,60])
            self._candle_subscribe_id += 1
            self._candle_subscribed.append(target_symbol)
            self.ws.send_subscribe( send_json, self._ws_args )

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
            limittime = starttime+api_timeframe*2000  # 最大取得本数は2000本
            self._logger.debug( "limittime  : {}".format(datetime.fromtimestamp(limittime,tz=timezone(timedelta(hours=9),'JST'))) )

            try:
                responce = ''
                await self.check_request_limit()
                res = await self._client.get('/exchange/public/md/kline', params={'symbol':target_symbol, 'resolution':60, 'from':starttime, 'to':min(endtime,limittime)})
                self._update_api_limit(res)

                # 429: Too Many Requests
                if res.status==429:
                    await asyncio.sleep(30)
                    continue

                await asyncio.sleep(1)
                responce = await res.json()

                # {'code': '39995', 'msg': 'Too many requests.'}
                if responce.get('code')=='39995' :
                    await asyncio.sleep(5)
                    continue

                if responce.get('code')!=0 :
                    raise RuntimeError

                rows = responce['data']['rows']

                # 得られたデータが空っぽなら次の区間を探索
                if len(rows)==0 :
                    self._logger.debug( "starttime: {}".format(datetime.fromtimestamp(starttime,tz=timezone(timedelta(hours=9),'JST'))) )
                    self._logger.debug( "{} : Fetch candles from API (request {}-{}) [empty]".format(target_symbol,
                        datetime.fromtimestamp(starttime,tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M'),
                        datetime.fromtimestamp(min(endtime,limittime),tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M') ) )

                    await asyncio.sleep(1)
                    starttime = limittime
                    continue

                # 得られたデータが指定されたスタート時刻まで含まれていなかったらまだその時刻のローソクはできていないので終了
                if starttime>int(rows[-1][0]) :
                    keep_running = False
                    break

                data = [ c for c in rows if starttime<=int(c[0])<=endtime]
                self._logger.info( "{} : Fetch {} candles timeframe{} from API [{}~{}]".format(target_symbol,len(data),api_timeframe,
                    datetime.fromtimestamp(data[0][0],tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M'),
                    datetime.fromtimestamp(data[-1][0],tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M')) )

                # 分足データストアに保管
                self.candle._onmessage({'symbol':target_symbol, 'kline': data})

                # データ取得用一時ストア
                if need_result:
                    self._stores.get('temp_candle')._onmessage({'symbol': target_symbol, 'interval': timeframe_sec, 'kline':
                                     [(c[0]*1000,c[3]/10000,c[4]/10000,c[5]/10000,c[6]/10000,c[7],c[8]/10000) for c in data]})

                # 取得した分足が8000本を超えるようなら一度リサンプルを行う
                if len(self.candle)>8000:
                    with self._datastore_lock :
                        self.candle_store.resample(symbol=target_symbol)

                # 次回の取得開始時
                starttime = int(rows[-1][0]+api_timeframe)
                endtime = int(datetime.now(tz=timezone.utc).timestamp()//api_timeframe*api_timeframe)

                # 得られたデータが目的のendtimeを超えていたら終了
                if endtime < rows[-1][0]:
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

        agg_param = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum', 'turnover': 'sum'}
        df = df.resample(f'{timeframe*60}S').agg(agg_param)
        df['close'].fillna(method='ffill', inplace=True)
        df['open'].fillna(df['close'], inplace=True)
        df['high'].fillna(df['close'], inplace=True)
        df['low'].fillna(df['close'], inplace=True)
        df['volume'].fillna(0, inplace=True)
        df['turnover'].fillna(0, inplace=True)

        self._candle_api_lock.release()
        return df.tz_convert(timezone(timedelta(hours=9), 'JST')).tail(num_of_candle)

    def create_candle_class(self,id):
        self.create(id, datastore_class=Kline)


    # APIからTickerを取得
    async def ticker_api(self, **kwrgs):
        try:
            await self.check_request_limit()
            res = await self._client.get('/md/ticker/24hr', params={'symbol': kwrgs.get('symbol', self.symbol)})
            self._update_api_limit(res)
            if res.status==200:
                data = await res.json()
                self.my.position.ref_ltp = data['result']['close']/10000
                return {'stat':data['error'] or 0 , 'ltp':data['result']['close']/10000, 'msg':data}
            else:
                return {'stat': res.status, 'msg': res.reason}
        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

    # APIから現在ポジを取得
    async def getpositions(self, **kwrgs):
        target_coin = self._collateral_coin.get(self.symbol,'USD')
        try:
            await self.check_request_limit()
            r = await self._client.get('/accounts/positions', params={'currency': target_coin})
            self._update_api_limit(r)
            res = await r.json()
        except Exception:
            self._logger.error(traceback.format_exc())
            return []

        if res['code']==0 :
            for p in res.get("data",{}).get("positions"):
                if p['symbol']==self.symbol :
                    return [p]
            return res.get("data",{}).get("positions")
        else:
            return []

    # 証拠金口座の証拠金額 (coinが指定されていない場合には取引するシンボルに使用する通貨、指定されていれば指定された通貨)
    async def getcollateral(self, coin=None):

        target_coin = self._collateral_coin.get(self.symbol,'USD')

        try:
            res= await self.getbalance()
            if res.get('code',-1)!=0 :
                return {'stat': res.get('code',-1), 'msg': res}

            balance = 0
            for r in res['data'][0]['userMarginVo'] :
                try:
                    if r['currency']==(coin or target_coin) :
                        balance = float(r['accountBalance'])
                except:
                    pass
            return {'stat': 0, 'collateral': balance, 'msg': res}

        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

    async def getbalance(self ):
        try:
            await self.check_request_limit()
            r = await self._client.get('/phemex-user/users/children')
            self._update_api_limit(r)
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
                   time_in_force="GoodTillCancel", # 執行数量条件 を "GoodTillCancel", "ImmediateOrCancel", "FillOrKill", "PostOnly"で指定します。
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

        if self.RateLimitRemaining<10:
            self._logger.info("RateLimitRemaining : {}/500".format(self.RateLimitRemaining) )
            return {'stat': -999, 'msg': "RateLimitRemaining "+str(self.RateLimitRemaining)+"/500", 'ids': []}

        if self.PendingUntil > time.time() :
            self._logger.info("Order pending : {:.1f}sec".format(self.PendingUntil-time.time()) )
            return {'stat': -999, 'msg': "Order pending : {:.1f}sec".format(self.PendingUntil-time.time()), 'ids': []}

        adjust_flag = kwargs.get('adjust_flag', False)

        # 対応しているオプションだけにする
        params={}
        for k,v in kwargs.items():
            if k=='timeInForce' :
                if v in ["GoodTillCancel", "ImmediateOrCancel", "FillOrKill", "PostOnly"] :
                    params[k]=v
            elif k=='triggerType' :
                if v in ["ByMarkPrice", "ByLastPrice"] :
                    params[k]=v
            elif k=='pegPriceType' :
                if v in ["TrailingStopPeg", "TrailingTakeProfitPeg"] :
                    params[k]=v
            elif k in ['symbol','clOrdID','stopPxEp','reduceOnly','closeOnTrigger','takeProfitEp','stopLossEp','pegOffsetValueEp']:
                params[k]=v

        params['clOrdID']=params.get('clOrdID', 'BFSX-'+str(int(time.time()*100000))+str(random.random()) )
        params['symbol']=params.get('symbol', self.symbol)
        params['ordType']=order_type.capitalize()
        params['side']=side.capitalize()
        params['orderQty']=size
        params['priceEp']=int(self.round_order_price(price, params['symbol'])*10000)

        # ノートレード期間はポジ以上のクローズは行わない
        if self.noTrade :
            params['orderQty'] = min(abs(self.my.position.size),params['orderQty'])
            self._logger.info("No trade period (change order size to {})".format(params['orderQty']) )

        # 最低取引量のチェック（無駄なAPIを叩かないように事前にチェック）
        if params['orderQty']<self.minimum_order_size(symbol=params['symbol']) :
            return {'stat': -153, 'msg': '最低取引数量を満たしていません', 'ids': []}

        self._logger.info("[sendorder] : {}".format(params) )
        with self._order_dict_lock :
            try:
                await self.check_request_limit()
                r = await self._client.post('/orders', data=params)
                self._update_api_limit(r)
                res = await r.json()
            except Exception as e:
                self._logger.error(traceback.format_exc())
                return {'stat': -999, 'msg': str(e), 'ids':[]}

            ret_code = res.get('code',-999)

            if ret_code==0 : 
                r = res.get('data')
                if self.my and not adjust_flag:
                    self.my.order.new_order( symbol=r['symbol'], id=r['orderID'] , side=r['side'].upper(), price=r['priceEp']/10000, size=r['orderQty'], expire=time.time()+auto_cancel_after, invalidate=time.time()+2592000 )
                return {'stat':0 , 'msg':"", 'ids':[r['orderID']]}
            else: 
                self._logger.error("Send order param : {}".format(params) )
                self._logger.error("Error response [sendorder] : {}".format(res) )
                return {'stat':ret_code , 'msg':res.get('msg'), 'ids':[]}

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

        if self.RateLimitRemaining<10:
            self._logger.info("RateLimitRemaining : {}/120".format(self.RateLimitRemaining) )
            return {'stat': -999, 'msg': "RateLimitRemaining "+str(self.RateLimitRemaining)+"/120", 'ids': []}

        kwargs['orderID']=id
        kwargs['symbol']=kwargs.get("symbol", self.my.order.order_dict[id]['symbol'] )

        self._logger.debug("[cancelorder] : {}".format(kwargs) )

        self.my.order.mark_as_invalidate( id )

        try:
            await self.check_request_limit()
            r = await self._client.delete('/orders/cancel', params=kwargs)
            self._update_api_limit(r)
            res = await r.json()
        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

        ret_code = res.get('code',-999)

        if ret_code==0 : 
            return {'stat': 0, 'msg': ""}

        # すでにオーダーがなくなっている？
        # {'code': 10002, 'msg': 'OM_ORDER_NOT_FOUND', 'data': None}
        if ret_code==10002 :
            self.my.order.remove_order( id=str(id) )

        self._logger.error("Error response [cancelorder] : {}".format(res) )
        return {'stat':ret_code , 'msg':res.get('msg')}

    # 変更
    async def changeorder(self, id, price, **kwargs ):
        try:
            params = {'orderID':id, 'symbol':self.symbol,
                      'priceEp':int((self.round_order_price(price,kwargs.get('symbol', self.my.order.order_dict.get(id,{}).get('symbol',self.symbol))))*10000)}
            self._logger.info("[changeorder] : {}".format(params) )
            await self.check_request_limit()
            r = await self._client.put('/orders/replace', params=params)
            self._update_api_limit(r)
            res = await r.json()

        except Exception as e:
            self._logger.error( traceback.format_exc() )
            return {'stat': -999, 'msg': str(e)}
        return {'stat':res.get('code',-1) , 'msg':res.get('msg')}

