# coding: utf-8
#!/usr/bin/python3

import asyncio
import time
import traceback

class WebsocketExchange(object):

    def __init__(self):

        self._disconnect_handlers: List[Coroutine] = []
        self._disconnected_event = asyncio.Event()
        asyncio.create_task(self._check_disconnect(), name="websocket_disconnect_check")


    # ws の切断を監視するタスク
    async def _check_disconnect(self):
        while self._logger.running :
            while not self._disconnected_event.is_set():
                try:
                    await asyncio.wait_for(self._disconnected_event.wait(), timeout=1)
                except asyncio.TimeoutError:
                    pass
            self._disconnected_event.clear()
            await asyncio.gather(*self._disconnect_handlers)

    # ws が切断された場合に呼び出される
    async def _disconnected(self):
        self._disconnected_event.set()

    # 切断時のハンドラ登録
    def add_disconnect_handler(self, callback, args):
        self._disconnect_handlers.append(callback(*args))
        
