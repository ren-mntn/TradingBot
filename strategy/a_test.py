# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy
from pprint import pprint

class MyStrategy(Strategy):
    """
    自作テスト
    """
    async def initialize(self):

        self.candle2 = self.APICandle(timeframe=1, num_of_candle=500, callback=self.logic_loop2)

    async def logic_loop2(self):
        print( 'candle2--------\n',self.candle2.candle )

