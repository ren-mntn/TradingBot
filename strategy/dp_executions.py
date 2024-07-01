# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy

class MyStrategy(Strategy):
    """
    wsで約定履歴が流れてきたら表示するだけのロジック
    """
    async def initialize(self):
        # 約定履歴が流れてきたら logic関数が呼び出されるように設定
        self.exec_que = self.ExecutionQueue(callback=self.logic)

    async def logic(self):
        # self.exec_que に入っている約定データを取り出して表示する
        while len(self.exec_que)!=0:
            i = self.exec_que.popleft()
            self._logger.info( i )
