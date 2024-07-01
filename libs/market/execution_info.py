# coding: utf-8
#!/usr/bin/python3

import asyncio
from collections import deque
from libs.utils.time_conv import TimeConv
import time
from threading import Event,Thread
import traceback

# 約定情報を管理するクラス
class ExecuionInfo(TimeConv):

    def __init__(self, logger):
        self._logger = logger
        self._executions = deque(maxlen=1000)  # 直近の平均価格算出用
        self.time = self._jst_now_dt()   # 直近のexecutions受信時刻 (datetime形式)

        self.avg_price_1s = 0             #　直近1秒の約定平均価格
        self.avg_latency_1s = 0           #　直近1秒の平均配信遅延
        self.last = self.best_ask = self.best_bid = 0
        self._exec_que_list = []
        self._latency = deque(maxlen=1000)
        self._event_time_lag = deque(maxlen=100)

        self.event = asyncio.Event() 
        self._event = Event() 
        event_drive = Thread(target=self._event_drive, args=())
        event_drive.daemon = True
        event_drive.start()
        self._logger.call_every1sec.append({'name':'update_ltp', 'handler':self._update_ltp, 'interval':1, 'counter':0})

    def _event_drive(self):
        while self._logger.running:
            self._event.wait()
            self._event.clear()
            self._event_time_lag.append((time.time()-self._logger.ws_timestamp)*1000)
            self.event.set()

            for exec_que, handler, args in self._exec_que_list :
                if len(exec_que)!=0 and self._logger.running :
                    try:
                        future = asyncio.run_coroutine_threadsafe(handler(*args), self._logger.event_loop)
                        result = future.result()

                    except Exception as e:
                        self._logger.error( e )
                        self._logger.info(traceback.format_exc())

    @property
    def event_time_lag(self):
        return self._mean(self._event_time_lag)

    def add_handler( self, exec_que, handler, args=() ):
        self._exec_que_list.append( [exec_que, handler, args] )

    def append_execution(self, price, size, side, exec_date, id='NONE'):
        self.last = price
        self._executions.append( price )

        # 最終売買履歴からbest_ask/best_bidを算出
        if side=="BUY" : self.best_ask = price
        else:            self.best_bid = price

        # 登録されているすべてのキューに約定履歴を突っ込む
        for exec_que, handler, args in self._exec_que_list :
            exec_que.append({'price':price, 'size':size, 'side':side, 'exec_date':exec_date, 'id':id})

    def append_latency(self, latency):
        self._latency.append( latency )

    def _mean(self,q) : return sum(q) / (len(q) + 1e-7)

    # 1秒ごとに平均価格と平均配信遅延を集計する
    async def _update_ltp(self):
        if len(self._executions)!=0 :
            self.avg_price_1s = self._mean(self._executions)
            self.avg_latency_1s = self._mean(self._latency)
            self._executions.clear()
            self._latency.clear()
