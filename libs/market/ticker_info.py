# coding: utf-8
#!/usr/bin/python3

from libs.utils.time_conv import TimeConv

# Tickerを管理するクラス
class TickerInfo(TimeConv):

    def __init__(self):
        self.last = 0
        self.time = self._jst_now_dt()   # 直近のexecutions受信時刻 (datetime形式)

