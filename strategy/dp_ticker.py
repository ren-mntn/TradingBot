# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy

class MyStrategy(Strategy):
    """
    定期的にAPIでtickerを取得して表示するだけのロジック (wsは利用しない)
    """
    async def initialize(self):
        # 3秒間隔で callback で指定した logic関数が呼び出されるように設定
        self.Scheduler(interval=3, callback=self.logic)

    async def logic(self):
        self._logger.info( (await self.exchange.ticker_api())['ltp'] )
