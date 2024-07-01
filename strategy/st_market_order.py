# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy
import numpy as np

class MyStrategy(Strategy):

    # --------------------------------------------
    # ロジック部（指値を出す位置を計算）
    def logic(self, df, **kwargs):
        short_sma = int(kwargs['short_sma'])
        long_sma = int(kwargs['long_sma'])

        def crossover(x,y):
            return ((x - y)>0) & ((x.shift(1) - y)<0)
        def crossunder(x,y):
            return ((x - y)<0) & ((x.shift(1) - y)>0)

        df['short_sma'] = df['close'].rolling(short_sma).mean()
        df['long_sma'] = df['close'].rolling(long_sma).mean()
        df['long'] = (df['long_sma'].diff(1)>0) & crossover(df['long_sma'],df['short_sma'])
        df['short'] = (df['long_sma'].diff(1)<0) & crossunder(df['long_sma'],df['short_sma'])
        return df
    # --------------------------------------------


    async def initialize(self):
        self._last_candle = 0

         # APIから取得するローソク足が更新されたら candle_update関数を呼び出すよう設定します
        self.candlegen = self.APICandle( timeframe=self.parameters['params']['timescale'],
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
        self._logger.info( "\n{}".format(self._candle.tail(3)) )

        # 最大ポジション
        pyramiding = self.parameters['params']['pyramiding']

        # 買いエントリーのロットサイズ計算
        buysize =  min( self.parameters['lot']+                                # エントリーのためのサイズ
                        max(0,-self.current_pos),                              # ドテンのための必要なサイズ
                        pyramiding*self.parameters['lot'] - self.current_pos ) # 許容最大pyramidingまでのサイズ

        # 売りエントリーのロットサイズ計算
        sellsize = min( self.parameters['lot']+                                # エントリーのためのサイズ
                        max(0, self.current_pos),                              # ドテンのための必要なサイズ
                        pyramiding*self.parameters['lot'] + self.current_pos ) # 許容最大pyramidingまでのサイズ

        # ロングシグナルが出ていてエントリー可能
        if self._candle['long'][-1] and buysize >= self.minimum_order_size :
            await self.sendorder( "MARKET", "BUY",  size=buysize)

        if self._candle['short'][-1] and sellsize >= self.minimum_order_size :
            await self.sendorder( "MARKET", "SELL",  size=sellsize)

        # クローズシグナルがあるロジックの場合にはシグナルに沿ってクローズ処理
        if "longclose" in self._candle.columns: # クローズ条件のあるロジックの場合
            if self._candle['longclose'][-1] and not self._candle['long'][-1] and self.current_pos >= self.minimum_order_size :
                await self.sendorder( "MARKET", "SELL",  size=self.current_pos)

            if self._candle['shortclose'][-1] and not self._candle['short'][-1] and -self.current_pos >= self.minimum_order_size :
                await self.sendorder( "MARKET", "BUY",  size=-self.current_pos)

    async def status(self):
        # ループの秒数設定が変わっていたら変更
        if self._schedule.interval != self.parameters['loop_period'] :
            self._schedule.interval = self.parameters['loop_period']

        if self._last_candle == 0 : return

        self._logger.info( 'LTP:{:.0f} Average:{:.0f} Position:{:.3f} Profit:{:>+.8f}({:+.8f}) Delay:{:>4.0f}ms'.format(
            self.ltp, self.current_average, self.current_pos, self.current_profit, self.current_profit_unreal, self.server_latency) )
