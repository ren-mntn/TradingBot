# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy

class MyStrategy(Strategy):

    async def initialize(self):

        candle = await self.exchange.get_candles(self.parameters['timescale'], self.parameters['num_of_candles'] )
        self._logger.info( "Fetch {} candles".format( len(candle) ) )

        filename = self.exchange.__class__.__name__ + "_" + self.exchange.symbol + ".csv"
        candle[['open','high','low','close','volume']].to_csv( filename )
        self._logger.info( "Save to {}".format( filename ) )

        await self.exit()
