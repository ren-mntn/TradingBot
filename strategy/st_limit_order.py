# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy
import talib
import numpy as np
import gc

class MyStrategy(Strategy):

    # --------------------------------------------
    # ロジック部（指値を出す位置を計算）
    def logic(self, df, **kwargs):
        entryLength = int(kwargs['entryLength'])
        entryPoint = float(kwargs['entryPoint'])

        df['sell'] = df['high'].shift(1).rolling(entryLength).max() * (1+entryPoint/100)
        df['buy'] = df['low'].shift(1).rolling(entryLength).min() * (1-entryPoint/100)
        return df
    # --------------------------------------------


    async def initialize(self):
        self._last_candle = 0

        # 初回のボード受信まで待機
        while self.exchange.board_info.best_bid==0 or self.exchange.board_info.best_ask==0:
            await self.exchange.board.wait()

        # APIから取得するローソク足が更新されたら candle_update関数を呼び出すよう設定します
        self.candlegen = self.APICandle( timeframe=int(self.parameters['params']['timescale']),
                                         num_of_candle=self.parameters['num_of_candle'], callback=self.candle_update )
        
        # loop_period間隔でself.status関数が呼び出されるように設定
        self._schedule = self.Scheduler(callback=self.status, interval=self.parameters['loop_period'])

    async def candle_update(self):

        # APIから取得されているローソク足
        self._candle = self.candlegen.candle.copy()

        # ローソク足が足りなければ何もしない
        if len(self._candle.index.values)<self.parameters['num_of_candle'] :
            return

        # ローソク足が更新されていなければ何もしない
        if self._last_candle == self._candle.index.values[-1] :
            return
        self._last_candle = self._candle.index.values[-1]

        # 最後の足を1本捨てる
        self._candle = self._candle[:-1].copy()

        # ロジック部で指値を出す位置を計算
        self._candle = self.logic( self._candle, **self.parameters['params'])

        # 最後の3本だけログ表示（チェック用）
        self._logger.info( "\n{}".format(self._candle[['interval','open','high','low','close','volume','sell','buy']+[i for i in self._candle.columns if i.startswith('idx')]].tail(3)) )

        # ロットサイズ・最大ポジション
        lotsize = self.parameters['lot']
        pyramiding = self.parameters['params']['pyramiding']

        # 買いエントリーのロットサイズ計算
        buysize =  min( lotsize*self.parameters['buy']+         # エントリーのためのサイズ
                        max(0,-self.current_pos),               # ドテンのための必要なサイズ
                        pyramiding*lotsize - self.current_pos ) # 許容最大pyramidingまでのサイズ

        # 売りエントリーのロットサイズ計算
        sellsize = min( lotsize*self.parameters['sell']+        # エントリーのためのサイズ
                        max(0, self.current_pos),               # ドテンのための必要なサイズ
                        pyramiding*lotsize + self.current_pos ) # 許容最大pyramidingまでのサイズ

        # エントリー（指値の変更）
        await asyncio.gather(
            asyncio.create_task( self.set_limit_order( "BUY",  buysize,  min(self._candle['buy'][-1],  self.exchange.board_info.best_bid) ) ),
            asyncio.create_task( self.set_limit_order( "SELL", sellsize, max(self._candle['sell'][-1], self.exchange.board_info.best_ask) ) )
        )

        # クローズシグナルがあるロジックの場合にはシグナルに沿ってクローズ処理
        if "longclose" in self._candle.columns: # クローズ条件のあるロジックの場合
            if self._candle['longclose'][-1] and self.current_pos >= self.minimum_order_size :
                await self.sendorder( "MARKET", "SELL",  size=self.current_pos)

            if self._candle['shortclose'][-1] and -self.current_pos >= self.minimum_order_size :
                await self.sendorder( "MARKET", "BUY",  size=-self.current_pos)

    async def set_limit_order(self, side, size, price):
        price = self.exchange.round_order_price( price )

        # 発注済みのリストから同一方向のオーダーを抜き出してキャンセル
        ids = [{'id':o['id'],'price':o['price'],'size':o['size']} for o in self.ordered_list if o['side']==side]
        for id in ids:
            await self.cancelorder( id['id'] )

        # 最低発注数に満たない場合には発注しない（発注済みのオーダーキャンセルだけ実施）
        if size < self.minimum_order_size :
            return

        # 新規発注
        await self.sendorder( "LIMIT", side,  size=size, price=price, minute_to_expire=self.parameters['params']['timescale'], 
                              auto_cancel_after=self.parameters['params']['timescale']*60-3 ) # 次の指値を出す3秒前にはキャンセル )

    async def status(self):
        # ループの秒数設定が変わっていたら変更
        if self._schedule.interval != self.parameters['loop_period'] :
            self._schedule.interval = self.parameters['loop_period']

        if self._last_candle == 0 : return

        buy_order_list = [d['price'] for d in self.ordered_list if d['side'].lower()=='buy']
        sell_order_list = [d['price'] for d in self.ordered_list if d['side'].lower()=='sell']
        self._logger.info( '[{}: SELL:{} BUY:{}]  LTP:{:.0f} Profit:{:>+.8f}({:+.8f}) Position:{:.3f} Delay:{:>4.0f}ms'.format(
            self._candle.index[-1],
            "{:.1f}({:+.1f})".format(sell_order_list[0],sell_order_list[0]-self.ltp) if len(sell_order_list)!=0 else "-"*13,
            "{:.1f}({:+.1f})".format(buy_order_list[0], buy_order_list[0]-self.ltp)  if len(buy_order_list)!=0  else "-"*13,
            self.ltp, self.current_profit, self.current_profit_unreal, self.current_pos, self.server_latency) )


        return False

