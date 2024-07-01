# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy
import asyncio

class MyStrategy(Strategy):
    """
    2秒間隔で板情報を表示し続けるサンプルロジック
    """
    async def initialize(self):

        # 為替情報の取得クラス
        from libs.market import GaitameUSDJPY
        self._usdjpy = GaitameUSDJPY(self._logger, keepupdate=True)

        # 複数取引所への接続
        self._exchanges = [(self.ExchangeWebsocket(exchange_name, self._logger), jpy, symbol) for exchange_name, symbol, jpy in self.parameters['exchanges']]

        # websocketの購読スタート
        await asyncio.gather(*[exchange.start(subscribe={'execution': False, 'board': True}, symbol=symbol) for exchange,jpy,symbol in self._exchanges])

        # 初回の板を受信するまで待機
        await asyncio.gather(*[exchange.board.wait() for exchange,jpy,symbol in self._exchanges])

        # 表示用のヘッダ
        self._header = ""
        for exchange,jpy,symbol in self._exchanges :
            self._header += "{:=^22}+".format(exchange.exchange_name)

        # 2秒間隔で callback で指定した logic関数が呼び出されるように設定
        self.Scheduler(interval=2, callback=self.logic)

    async def logic(self):

        rows = self.parameters['rows']
        price = self.parameters['price']

        self._logger.info(self._header)
        usdjpy = await self._usdjpy.price
        boards = [exchange.board_info.get_price_group(splitprice= price*(usdjpy**jpy) ) for exchange,jpy,symbol in self._exchanges]

        for i in reversed(range(rows)):
            s = ''
            for b in boards:
                s += " (${:>+6}){:>10.3f}  |".format((i+1)*price,(b['ask']+[0]*rows)[i])
            self._logger.info( s )

        # mid値とbinance価格との差を表示
        s = ''
        for exchange,jpy,symbol in self._exchanges:
            s += "--  mid : {:>9.1f} --+".format(exchange.board_info.mid)
        self._logger.info( s )

        for i in range(rows):
            s = ''
            for b in boards:
                s += " (${:>+6}){:>10.3f}  |".format((-1-i)*price,(b['bid']+[0]*rows)[i])
            self._logger.info( s )

        self._logger.info('')
