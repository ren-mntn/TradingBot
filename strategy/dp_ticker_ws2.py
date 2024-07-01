# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy
import asyncio

class MyStrategy(Strategy):
    """
    wsでtickerが更新されたら表示するだけのロジック(watchメソッドを使った例)
    """
    async def initialize(self):
        # logic_loopを新規タスクとして立ち上げる
        asyncio.create_task(self.logic_loop())

    async def logic_loop(self):
        with self.exchange.ticker.watch() as stream:
           async for change in stream:
                print(f"operation: {change.operation}")
                print(f"data: {change.data}")
