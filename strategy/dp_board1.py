# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy

class MyStrategy(Strategy):
    """
    板情報が更新されたら表示し続けるサンプルロジック
    """
    async def initialize(self):
        # 板情報が更新されたら callbackで指定したboard_update関数を呼び出す
        self.AddBoardUpdateHandler( callback=self.board_update )

    async def board_update(self):
        # self.asks/self.bids に入っている板データ(上下5個)を表示
        self._logger.info( '-'*50 )
        for i in self.asks[5::-1] :
            self._logger.info( i )
        self._logger.info( "-----mid : {}".format(self.mid_price) )
        for i in self.bids[:5] :
            self._logger.info( i )
