# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy
import asyncio
import pandas as pd
pd.options.display.max_columns = None
pd.options.display.width = 2000

class MyStrategy(Strategy):
    """
    ローソク足が更新されたら表示するだけのロジック
    """
    async def initialize(self):

        self.candle1 = self.APICandle(timeframe=2, num_of_candle=500, callback=self.logic_loop1)
        self.candle2 = self.APICandle(timeframe=1, num_of_candle=500, callback=self.logic_loop2)

    async def logic_loop1(self):
        print( 'candle1--------\n',self.candle1.candle )

    async def logic_loop2(self):
        print( 'candle2--------\n',self.candle2.candle )
