# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy
import asyncio

class MyStrategy(Strategy):
    """
    30秒ごとにLTPの上下2%の位置に5個づつ指値をばらまくサンプル
    """
    async def initialize(self):

        # 初回の約定履歴を受信するまで待機
        await self.exchange.execution_info.event.wait()

        # 30秒間隔で callback で指定した logic関数が呼び出されるように設定
        self.Scheduler(interval=30, callback=self.logic)

    async def logic(self):

        # 上下に5つのオーダー（非同期で同時に発注）
        coroutines: List[Coroutine] = []
        for i in range(5):
            coroutines.append(self.sendorder(order_type="LIMIT", side='BUY',  size=self.minimum_order_size, price=self.ltp*(1-0.02-0.001*i), auto_cancel_after=25))
            coroutines.append(self.sendorder(order_type="LIMIT", side='SELL', size=self.minimum_order_size, price=self.ltp*(1+0.02+0.001*i), auto_cancel_after=25))
        await asyncio.gather(*coroutines)
