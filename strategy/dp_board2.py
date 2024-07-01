# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy

class MyStrategy(Strategy):
    """
    2秒間隔で板情報を表示し続けるサンプルロジック
    """
    async def initialize(self):
        # 初回の板を受信するまで待機
        await self.exchange.board.wait()

        # 2秒間隔で callback で指定した logic関数が呼び出されるように設定
        self.Scheduler(interval=2, callback=self.logic)

    async def logic(self):
        # self.asks/self.bids に入っている板データ(上下5個)を表示
        self._logger.info( '-'*50 )
        for i in self.asks[5::-1] :
            self._logger.info( i )
        self._logger.info( "-----mid : {}".format(self.mid_price) )
        for i in self.bids[:5] :
            self._logger.info( i )
