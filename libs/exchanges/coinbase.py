# coding: utf-8
#!/usr/bin/python3

import asyncio

import pybotters
from pybotters.store import DataStoreManager, DataStore

from libs.utils import TimeConv, LockCounter
from libs.market import *
from libs.exchanges.base_module import MultiProc_WS, WebsocketExchange

# https://docs.pro.coinbase.com/#websocket-feed

class Coinbase(DataStoreManager, TimeConv, WebsocketExchange):

    def __init__(self, logger, candle_store, apikey=('','')):
        self._logger = logger
#        self.candle_store = candle_store   # 分ローソク足変換クラス
        self.auth = (apikey[0]!='' and apikey[1]!='')
        self._apikey = apikey
        self.exchange_name = "coinbase"

        self._client = pybotters.Client(apis={self.exchange_name: [self._apikey[0],self._apikey[1]]})
        self._datastore_lock = LockCounter(self._logger, self.__class__.__name__)
        super().__init__()
        WebsocketExchange.__init__(self)

        self.create("ticker", datastore_class=Ticker)
        self.create("board", datastore_class=Board)

    # データストアへの接続を共通名に変換
    @property
    def ticker(self):
        return self._stores.get('ticker')

    @property
    def board(self):
        return self._stores.get('board')

    async def start(self, subscribe={}, symbol='BTC-USD'):
        self._param = subscribe
        self.symbol = symbol

        self._endpoint = "wss://ws-feed.pro.coinbase.com"

        # データ保存用クラス
        self.board_info = BoardInfo( self._logger )
        self.execution_info = ExecuionInfo( self._logger )
        self.ticker_info = TickerInfo()
        self.ticker_info.best_ask = 0
        self.ticker_info.best_bid = 0

        # 指定のチャンネルの購読
        self._handler = {'None':None}
        self._ws_args = []
        if self._param.get('execution', True) or self._param.get('ticker', False) :
            self._subscribe({"type": "subscribe", "channels": [{ "name": "ticker"}],"product_ids": [self.symbol]},"ticker", self._on_ticker)
        if self._param.get('board', False) :
            self._subscribe({"type": "subscribe", "channels": [{ "name": "level2"}],"product_ids": [self.symbol]},"snapshot", self._on_board)
            self._subscribe({"type": "subscribe", "channels": [{ "name": "level2"}],"product_ids": [self.symbol]},"l2update", self._on_board)

        # WebSocketタスクを開始
        options = {} if self.auth else {'auth': None}
        self.ws = MultiProc_WS( logger = self._logger, exchange_name = self.__class__.__name__,
                      handler = self._onmessage,
                      endpoint = self._endpoint,
                      apis = {self.exchange_name: [self._apikey[0],self._apikey[1]]},
                      send_json = self._ws_args,
                      lock = self._datastore_lock,
                      disconnect_handler = self._disconnected,
                      **options )
        asyncio.create_task(self.ws._start_websocket(logging=False), name="coinbase_websocket")

        return self

    def _subscribe(self, options, key, handler):
        self._logger.debug( "subscribe : {}".format(options) )
        self._handler[key]=handler
        self._ws_args.append(options)
        return options

    def _onmessage(self, msg, ws):
        self._logger.debug("recv coinbase websocket [{}]".format(msg.get('type', 'None')))
        self._logger.trace("recv data [{}]".format(msg))

        topic_handler = self._handler.get(msg.get('type', 'None'))
        if topic_handler != None:
            topic_handler(msg)

    def _on_ticker(self,msg):
        self.ticker._onmessage(msg)
        self.ticker_info.time = self._utcstr_to_dt(msg['time'])
        self.ticker_info.last = float(msg.get('price',self.ticker_info.last))
        self.ticker_info.best_ask = float(msg.get('best_ask',self.ticker_info.best_ask))
        self.ticker_info.best_bid = float(msg.get('best_bid',self.ticker_info.best_bid))

        self.execution_info.time = self.ticker_info.time
        self.execution_info.append_latency((self._jst_now_dt().timestamp() - self.execution_info.time.timestamp())*1000)
        self.execution_info.append_execution(float(msg['price']),float(msg['last_size']),msg['side'].upper(),self.ticker_info.time,msg['trade_id'])
        self.execution_info._event.set()

    def _on_board(self,msg):
        msg_type=msg.get('type')
        if msg_type=='snapshot' :
            self.board_info.initialize_dict()
            self.board_info.update_asks(msg.get('asks',[]))
            self.board_info.update_bids(msg.get('bids',[]))
        elif msg_type=='l2update' :
            self.board_info.time = self._utcstr_to_dt(msg['time'])
            changes=msg.get('changes')
            self.board_info.update_asks([[a[1],a[2]] for a in changes if a[0]=='sell'])
            self.board_info.update_bids([[b[1],b[2]] for b in changes if b[0]=='buy'])
        self.board_info.event.set()
        for event in self.board._events:
            event.set()

class Ticker(DataStore):
    _KEYS = ["product_id"]

    def _onmessage(self, message):
        self._update([message])

# ダミー
class Board(DataStore):
    _KEYS = ["product_id", "side", "price"]

