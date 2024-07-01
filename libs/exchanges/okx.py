# coding: utf-8
#!/usr/bin/python3

import asyncio

import pybotters

from libs.utils import TimeConv, LockCounter
from libs.market import *
from libs.exchanges.base_module import MultiProc_WS, WebsocketExchange

# https://www.okx.com/docs-v5/en/#websocket-api

class Okx(pybotters.OKXDataStore, TimeConv, WebsocketExchange):

    def __init__(self, logger, candle_store, apikey=('','')):
        self._logger = logger
#        self.candle_store = candle_store   # 分ローソク足変換クラス
        self.auth = (apikey[0]!='' and apikey[1]!='')
        self._apikey = apikey
        self.exchange_name = "okx"

        self._client = pybotters.Client(apis={self.exchange_name: [self._apikey[0],self._apikey[1]]})
        self._datastore_lock = LockCounter(self._logger, self.__class__.__name__)
        super().__init__()
        WebsocketExchange.__init__(self)

    # データストアへの接続を共通名に変換
    @property
    def ticker(self):
        return self._stores.get('tickers')

    @property
    def board(self):
        return self._stores.get('books')

    async def start(self, subscribe={}, symbol='BTC-USD-SWAP'):
        self._param = subscribe
        self.symbol = symbol

        self._endpoint = "wss://ws.okx.com:8443/ws/v5/public"

        # データ保存用クラス
        self.board_info = BoardInfo( self._logger )
        self.execution_info = ExecuionInfo( self._logger )
        self.ticker_info = TickerInfo()
        self.ticker_info.best_ask = 0
        self.ticker_info.best_bid = 0

        # 指定のチャンネルの購読
        self._handler = {'None':None}
        self._ws_args = []
        if self._param.get('execution', True) : self._subscribe({"op": "subscribe", "args": [{"channel": "trades", "instId": self.symbol}]},"trades", self._on_executions)
        if self._param.get('board', False) :    self._subscribe({"op": "subscribe", "args": [{"channel": "books", "instId": self.symbol}]},"books", self._on_board)
        if self._param.get('ticker', False) :   self._subscribe({"op": "subscribe", "args": [{"channel": "tickers", "instId": self.symbol}]},"tickers", self._on_ticker)

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
        asyncio.create_task(self.ws._start_websocket(logging=False), name="okx_websocket")

        return self

    def _subscribe(self, options, key, handler):
        self._logger.debug( "subscribe : {}".format(options) )
        self._handler[key]=handler
        self._ws_args.append(options)
        return options

    def _onmessage(self, msg, ws):
        self._logger.debug("recv okex websocket [{}] channel[{}]".format(msg.keys(),msg.get('arg',{}).get('channel')))
        self._logger.trace("recv data [{}]".format(msg))

        if 'data' in msg :
            arg = msg.get('arg',{})
            topic_handler = self._handler.get(arg.get('channel','None'))
            if topic_handler != None:
                topic_handler(msg)

                if topic_handler in [self._on_ticker, ] :
                    super()._onmessage(msg,ws)
            else:
                super()._onmessage(msg,ws)
        else:
            super()._onmessage(msg,ws)

    def _on_executions(self,msg):
        recept_data = msg.get('data')
        self.execution_info.time = self._epoc_to_dt(int(recept_data[-1]['ts'])/1000)
        self.execution_info.append_latency((self._jst_now_dt().timestamp() - self.execution_info.time.timestamp())*1000)
        for i in recept_data:
            self.execution_info.append_execution(float(i['px']),float(i['sz']),i['side'].upper(),self._epoc_to_dt(int(i['ts'])/1000),i['tradeId'])
        self.execution_info._event.set()

    def _on_ticker(self,msg):
        recept_data = msg.get('data')
        self.ticker_info.time = self._epoc_to_dt(int(recept_data[-1]['ts'])/1000)
        for d in recept_data:
            self.ticker_info.last = float(d.get('last',self.ticker_info.last))
            self.ticker_info.best_ask = float(d.get('askPx',self.ticker_info.best_ask))
            self.ticker_info.best_bid = float(d.get('bidPx',self.ticker_info.best_bid))

    def _on_board(self,msg):
        recept_data = msg.get('data')
        if msg.get('action')=='snapshot':
            self.board_info.initialize_dict()
        for i in recept_data :
            self.board_info.time = self._epoc_to_dt(int(recept_data[-1]['ts'])/1000)
            self.board_info.update_asks(i.get('asks',[]))
            self.board_info.update_bids(i.get('bids',[]))
        self.board_info.event.set()
        for event in self.board._events:
            event.set()