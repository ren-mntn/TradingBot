# -*- coding: utf-8 -*-
import asyncio
from libs.base_strategy import Strategy

class MyStrategy(Strategy):
    """
    確定足だけを表示するサンプルロジック
    """

    async def initialize(self):

        # 自炊ローソク足を作るクラスを起動、自炊してローソク足が更新されたら logic関数を呼び出すよう設定します
        self.candlegen = self.CandleGenerator(timescale=self.parameters['timescale'],
                                              num_of_candle=self.parameters['num_of_candle'],
                                              callback=self.logic)

    async def logic(self):
        # ローソク足の表示
        self._logger.info( '-'*100 + "\n{}\n\n".format(self.candlegen.candle[:-1]) )
