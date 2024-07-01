# coding: utf-8
#!/usr/bin/python3

import asyncio

import pybotters

from libs.utils import TimeConv, LockCounter
from libs.market import *
from libs.exchanges.base_module import MultiProc_WS, WebsocketExchange

# https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
# https://binance-docs.github.io/apidocs/spot/en/#market-data-endpoints

class Binance(pybotters.BinanceDataStore, TimeConv, WebsocketExchange):

    def __init__(self, logger, candle_store, apikey=('','')):
        self._logger = logger
#        self.candle_store = candle_store   # 分ローソク足変換クラス
        self.auth = (apikey[0]!='' and apikey[1]!='')
        self._apikey = apikey
        self.exchange_name = "binance"

        self._client = pybotters.Client(apis={self.exchange_name: [self._apikey[0],self._apikey[1]]})
        self._datastore_lock = LockCounter(self._logger, self.__class__.__name__)
        super().__init__()
        WebsocketExchange.__init__(self)

    # データストアへの接続を共通名に変換
    @property
    def ticker(self):
        return self._stores.get('ticker')

    @property
    def board(self):
        return self._stores.get('orderbook')

    async def start(self, subscribe={}, symbol='BTCUSDT'):
        self._param = subscribe
        self.symbol = symbol.lower()

        self._endpoint = "wss://fstream.binance.com/ws"

        # データ保存用クラス
        self.board_info = BoardInfo( self._logger )
        self.execution_info = ExecuionInfo( self._logger )
        self.ticker_info = TickerInfo()

        # 指定のチャンネルの購読
        self._handler = {'None':None}
        self._ws_args = []
        if self._param.get('execution', True) : self._subscribe({"method": "SUBSCRIBE", "params": [f"{self.symbol}@trade"], "id": 1},"trade", self._on_executions)
        if self._param.get('board', False) :    self._subscribe({"method": "SUBSCRIBE", "params": [f"{self.symbol}@depth@100ms"], "id": 1},"depthUpdate", self._on_board)
        if self._param.get('ticker', False) :   self._subscribe({"method": "SUBSCRIBE", "params": [f"{self.symbol}@ticker"], "id": 1},"24hrTicker", self._on_ticker)

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
        asyncio.create_task(self.ws._start_websocket(logging=False), name="binance_websocket")

        return self

    def _subscribe(self, options, key, handler):
        self._logger.debug( "subscribe : {}".format(options) )
        self._handler[key]=handler
        self._ws_args.append(options)
        return options

    def _onmessage(self, msg, ws):
        self._logger.debug("recv binance websocket [{}]".format(msg.get('e', 'None')))
        self._logger.trace("recv data [{}]".format(msg))

        topic_handler = self._handler.get(msg.get('e', 'None'))
        if topic_handler != None:
            topic_handler(msg)

            if topic_handler in [self._on_ticker, ] :
                super()._onmessage(msg,ws)

        else:
            super()._onmessage(msg,ws)

    def _on_executions(self,msg):
        self.execution_info.time = self._epoc_to_dt(int(msg['T'])/1000)
        self.execution_info.append_execution(float(msg['p']),float(msg['q']),"SELL" if msg['m'] else "BUY", self._epoc_to_dt(int(msg['T'])/1000), msg['t'])
        self.execution_info.append_latency(self._jst_now_dt().timestamp()*1000 - msg['T'])
        self.execution_info._event.set()

    def _on_board(self,msg):
        self.board_info.time = self._epoc_to_dt(int(msg['E'])/1000)
        self.board_info.update_asks(msg.get('a',[]))
        self.board_info.update_bids(msg.get('b',[]))
        self.board_info.event.set()
        for event in self.board._events:
            event.set()

    def _on_ticker(self,msg):
        recept_data = msg.get('data')
        self.ticker_info.time = self._epoc_to_dt(int(msg['E'])/1000)
        self.ticker_info.last = float(msg.get('c',self.ticker_info.last))

