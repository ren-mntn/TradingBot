# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy
import asyncio

class MyStrategy(Strategy):
    """
    定期的にAPIでtickerを取得して表示するだけのロジック (wsは利用しない)
    """
    async def initialize(self):
        # 3秒間隔で callback で指定した logic関数が呼び出されるように設定
        self.Scheduler(interval=3, callback=self.logic)

    async def logic(self):

        # 全 Ticker を非同期で同時に API 取得
        result = await asyncio.gather(*[
            self.exchange.ticker_api(symbol=s)
            for s in self.parameters['symbols']])

        tickers = dict( [(symbol,d.get('ltp'))
                         for (symbol,d) in zip(*[self.parameters['symbols'],result])
                         if d.get('stat',-1)==0] )

        self._logger.info( tickers )
