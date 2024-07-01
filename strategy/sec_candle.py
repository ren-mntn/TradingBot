# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy

class MyStrategy(Strategy):

    # 初期化
    async def initialize(self):

        # 指定された取引所とシンボルを取得するwebsocketを作成
        self.websocket = self.ExchangeWebsocket(exchange= self.parameters['exchange'], logger=self._logger)

        # websocketの購読スタート
        await self.websocket.start(subscribe={'execution': True}, symbol= self.parameters['symbol'])

        # 秒ローソク足を作成するクラスを作成
        self.candlegen = self.CandleGenerator(timescale=self.parameters['timescale'],
                                              num_of_candle=self.parameters['num_of_candle'],
                                              exchange=self.websocket, callback=self.update)



    # 作成された秒ローソクを表示
    async def update(self):

        # 最後の足は未確定足を含んでいます
        print( self.candlegen.candle )
