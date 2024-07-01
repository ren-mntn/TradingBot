# coding: utf-8
#!/usr/bin/python3

import asyncio
import time
import traceback

# 定期的なイベントの呼び出しを行うクラス
class Scheduler(object):
    def __init__(self, logger, interval=1, basetime=None, callback=None, args=()):
        self._logger = logger
        self._callback = callback
        self._interval = interval
        self._args = args
        self._basetime = time.time()+9*3600 if basetime==None else basetime
        self._event = asyncio.Event()

        asyncio.create_task(self._main_loop(), name=f"scheduler{callback.__name__}")

    async def _main_loop(self):
        while self._logger.running:
            next_time = (self._basetime-time.time()-9*3600) % self._interval

            # asyncio.wait_for(timeout) よりは時間が正確なので、10秒以下は asyncio.sleepを使う
            if next_time<10 :
                await asyncio.sleep(next_time)
            else:
                complete=False
                self._event.clear()
                try:
                    await asyncio.wait_for(self._event.wait(), timeout=next_time)
                except asyncio.TimeoutError:
                    complete=True
                if not complete:
                    continue

            try:
                if self._logger.running :
                    await self._callback(*self._args)
            except Exception as e:
                self._logger.error( "Function("+self._callback.__name__ + ") : error in scheduled job" )
                self._logger.error( e )
                self._logger.info(traceback.format_exc())

    @property
    def interval(self):
        return self._interval

    @interval.setter
    def interval(self, value):
        self._interval = value
        self._event.set()
