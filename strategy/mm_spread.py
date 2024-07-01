# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy
import time
import math

class MyStrategy(Strategy):

    async def initialize(self):

        self._last_ask = self._ask = self._last_bid = self._bid = self.ltp
        self._last_evented_time = time.time()

        # 約定データを受信したら callback で指定したexecutions関数が呼び出されるように設定、その際に約定データは self.exec_list に入れられる
        self.exec_list = self.ExecutionQueue( callback=self.executions )

        # 初回の約定履歴を受信するまで待機
        await self.exchange.execution_info.event.wait()

        # loop_period間隔で callback で指定したstatus関数が呼び出されるように設定
        self.Scheduler(callback=self.status, interval=self.parameters['loop_period'])


    async def executions(self):

        # 現在時刻の取得
        current_time = time.time()

        # self.exec_list に入っている約定データを取り出して処理する
        while len(self.exec_list)!=0:
            i = self.exec_list.popleft()

            current_price = int(i['price'])

            self._last_ask = self._ask
            self._last_bid = self._bid
            if i['side']=='BUY' : self._ask = current_price
            else :                self._bid = current_price

            lot = self.parameters['max_lot']
            buy_size  = round(math.tanh(1 - max(0,self.current_pos/lot)) * lot ,8)
            sell_size = round(math.tanh(1 + min(0,self.current_pos/lot)) * lot ,8)

            # スプレッド閾値を超えていたら売買イベントをセット
            if( self._last_evented_time + self.parameters['interval'] < current_time and # 前回のオーダーからinterval_time秒以上経っている
                self.parameters['spread'] < self._ask-self._bid ) :                      # スプレッドが閾値以上開いている

                if i['side'] =='BUY' :
                    if self._ask > self._last_ask :  # 買いによってaskが上昇していていればエントリー

                        if buy_size >= self.minimum_order_size :
                            price = int(self._bid + self.parameters['depth'])
                            await self.sendorder(order_type='LIMIT', side='BUY', size=buy_size, price=price)

                        self._last_evented_time = current_time

                else:
                    if self._bid < self._last_bid :  # 売りによってbidが下降していればエントリー

                        if sell_size >= self.minimum_order_size :
                            price = int(self._ask - self.parameters['depth'])
                            await self.sendorder(order_type='LIMIT', side='SELL', size=sell_size, price=price)

                        self._last_evented_time = current_time


    async def status(self):
        # botの稼働状況表示のみ
        self._logger.info( '    LTP:{:.0f}   Profit:{:>+8.0f}({:+4.0f}) API({}/{}) Average:{:.0f} Position:{:>9.5f} Delay:{:>4.0f}ms'.format(
                    self.ltp, self.current_profit, self.current_profit_unreal, self.api_remain1, self.api_remain2, self.current_average, self.current_pos, self.server_latency))
