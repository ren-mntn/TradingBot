# coding: utf-8
#!/usr/bin/python3

import asyncio

import pybotters
from pybotters.store import DataStore

from libs.utils import TimeConv, LockCounter
from libs.market import *
from libs.exchanges.base_module import MultiProc_WS, WebsocketExchange

# https://www.bitmex.com/app/wsAPI

class Bitmex(pybotters.BitMEXDataStore, TimeConv, WebsocketExchange):

    def __init__(self, logger, candle_store, apikey=('','')):
        self._logger = logger
#        self.candle_store = candle_store   # 分ローソク足変換クラス
        self.auth = (apikey[0]!='' and apikey[1]!='')
        self._apikey = apikey
        self.exchange_name = "bitmex"

        self._client = pybotters.Client(apis={self.exchange_name: [self._apikey[0],self._apikey[1]]})
        self._datastore_lock = LockCounter(self._logger, self.__class__.__name__)
        super().__init__()
        WebsocketExchange.__init__(self)

        self.create('orderBookL2', datastore_class=DataStore) # ダミーデータストア

    # データストアへの接続を共通名に変換
    @property
    def ticker(self):
        return self._stores.get('instrument')

    @property
    def board(self):
        return self._stores.get('orderBookL2')

    async def start(self, subscribe={}, symbol='XBTUSD'):
        self._param = subscribe
        self.symbol = symbol

        self._endpoint = "wss://www.bitmex.com/realtime"

        # データ保存用クラス
        self.board_info = BoardInfo( self._logger )
        self.execution_info = ExecuionInfo( self._logger )
        self.ticker_info = TickerInfo()
        self.ticker_info.best_ask = 0
        self.ticker_info.best_bid = 0
        self.ticker_info.open_interest = 0
        self.ticker_info.mark_price = 0

        # 指定のチャンネルの購読
        self._handler = {'None':None}
        self._ws_args = []
        if self._param.get('execution', True) : self._subscribe({"op": "subscribe", "args": [f"trade:{self.symbol}"]},"trade", self._on_executions)
        if self._param.get('board', False) :    self._subscribe({"op": "subscribe", "args": [f"orderBookL2:{self.symbol}"]},"orderBookL2", self._on_board)
        if self._param.get('ticker', False) :   self._subscribe({"op": "subscribe", "args": [f"instrument:{self.symbol}"]},"instrument", self._on_ticker)

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
        asyncio.create_task(self.ws._start_websocket(logging=False), name="bitmex_websocket")

        return self

    def _subscribe(self, options, key, handler):
        self._logger.debug( "subscribe : {}".format(options) )
        self._handler[key]=handler
        self._ws_args.append(options)
        return options

    def _onmessage(self, msg, ws):
        self._logger.debug("recv bitmex websocket [{}]".format(msg.get('table', 'None')))
        self._logger.trace("recv data [{}]".format(msg))

        topic_handler = self._handler.get(msg.get('table', 'None'))
        if topic_handler != None:
            topic_handler(msg)

            if topic_handler in [self._on_ticker, ] :
                super()._onmessage(msg,ws)

        else:
            super()._onmessage(msg,ws)

    def _on_executions(self,msg):
        recept_data = msg.get('data')
        self.execution_info.time = self._utcstr_to_dt(recept_data[-1]['timestamp'])
        self.execution_info.append_latency((self._jst_now_dt().timestamp() - self.execution_info.time.timestamp())*1000)
        for i in recept_data:
            self.execution_info.append_execution(i['price'],i['size'],i['side'].upper(),self._utcstr_to_dt(i['timestamp']),i['trdMatchID'])
        self.execution_info._event.set()

    def _on_ticker(self,msg):
        recept_data = msg.get('data')
        self.ticker_info.time = self._utcstr_to_dt(recept_data[-1]['timestamp'])
        for d in recept_data:
            try:
                self.ticker_info.mark_price = float(d.get('markPrice',self.ticker_info.mark_price))
                self.ticker_info.last = float(d.get('lastPrice',self.ticker_info.last))
                self.ticker_info.open_interest = float(d.get('openInterest',self.ticker_info.open_interest))
                self.ticker_info.best_ask = float(d.get('askPrice',self.ticker_info.best_ask))
                self.ticker_info.best_bid = float(d.get('bidPrice',self.ticker_info.best_bid))
            except:
                pass

    def _on_board(self,msg):
        self.board_info.time = self._jst_now_dt()
        recept_data = msg.get('data')
        msg_type=msg.get('action')
        if msg_type=='partial':
            self.board_info.initialize_dict()
            self.board_info.insert(recept_data)
        if msg_type=='insert' :
            self.board_info.insert(recept_data)
        elif msg_type=='delete' :
            self.board_info.delete(recept_data)
        elif msg_type=='update' :
            self.board_info.change(recept_data)
        self.board_info.event.set()
        for event in self.board._events:
            event.set()

