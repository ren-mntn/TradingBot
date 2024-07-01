# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy
import asyncio

class MyStrategy(Strategy):
    """
    wsでtickerが更新されたら表示するだけのロジック
    """
    async def initialize(self):
        # logic_loopを新規タスクとして立ち上げる
        asyncio.create_task(self.logic_loop())

    async def logic_loop(self):
        while True:
           await self.exchange.ticker.wait()
           print( self.exchange.ticker.find() )
