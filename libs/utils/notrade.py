# coding: utf-8
#!/usr/bin/python3

from datetime import datetime, timedelta
from datetime import time as datetime_time
from libs.utils.scheduler import Scheduler

# no_trade期間を判定するクラス
class NoTradeCheck(object):
    def __init__(self, logger, exchange):
        self._logger = logger
        self._exchange = exchange
        self.notrade = None
        self._logger.call_every1sec.append({'name':'check_no_trade_period', 'handler':self.check_no_trade_period, 'interval':3, 'counter':0})

    async def check_no_trade_period(self):
        # 現在時刻が範囲内かどうか　https://codeday.me/jp/qa/20190219/264470.html
        def time_in_range(start, end, x):
            """Return true if x is in the range [start, end]"""
            if start <= end:
                return start <= x <= end
            else:
                return start <= x or x <= end

        if not self.notrade :
            self._exchange.noTrade = False
            return

        now = datetime_time((datetime.utcnow()+timedelta(hours=9)).hour,
                            (datetime.utcnow()+timedelta(hours=9)).minute, 0)
        weekday = (datetime.utcnow()+timedelta(hours=9)).weekday()
        try:
            for p in self.notrade:
                # 時刻で指定
                if len(p['period']) <= 13:
                    start = datetime_time(
                        int(p['period'][0:2]), int(p['period'][3:5]),  0)
                    end = datetime_time(
                        int(p['period'][6:8]), int(p['period'][9:11]), 0)
                    if (len(p['period']) <= 11 or int(p['period'][12]) == weekday) and time_in_range(start, end, now):
                        self._logger.info('no_trade period : {}'.format(p['period']))
                        self._exchange.noTrade = True
                        return

                # 日付で指定
                elif len(p['period']) == 33:
                    now = datetime.now()
                    # Python3.6まで
                    start = datetime(
                        year   = int(p['period'][0:4]),
                        month  = int(p['period'][5:7]),
                        day    = int(p['period'][8:10]),
                        hour   = int(p['period'][11:13]),
                        minute = int(p['period'][14:16])
                    )
                    end = datetime(
                        year   = int(p['period'][17:21]),
                        month  = int(p['period'][22:24]),
                        day    = int(p['period'][25:27]),
                        hour   = int(p['period'][28:30]),
                        minute = int(p['period'][31:33])
                    )
                    # Python3.7以降ならfromisoformatが速い
                    #start = datetime.fromisoformat( p['period'][:16] )
                    #end   = datetime.fromisoformat( p['period'][17:] )

                    if time_in_range(start, end, now):
                        self._logger.info('no_trade period : {}'.format(p['period']))
                        self._exchange.noTrade = True
                        return

            self._exchange.noTrade = False
            return

        except Exception as e:
            self._logger.error('no_trade period is not correct: {}'.format(e))
            self._logger.info('no_trade : {}'.format(self.notrade))

