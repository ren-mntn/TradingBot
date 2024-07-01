# coding: utf-8
#!/usr/bin/python3

# ロックされている数を管理するクラス
import asyncio

class LockCounter(object):
    def __init__(self, logger, name=''):
        self._logger = logger
        self._name = name
        self._counter = 0

    def __enter__( self ) :
        self._counter += 1
        return self

    def __exit__(self, ex_type, ex_value, trace):
        self._counter -= 1

    def wait(self):
        if self._counter>0 :
            asyncio.run_coroutine_threadsafe(self._wait_until_zero(), self._logger.event_loop).result()

    async def _wait_until_zero(self):
        if self._counter==0:
            return

        self._logger.trace( "Lock counter[{}] : {}".format(self._name, self._counter))
        await asyncio.sleep(0.01)

        i = 0
        while self._counter>0:
            await asyncio.sleep(0.01)
            i += 1
            if i%100 == 0:
                self._logger.info( "Lock counter[{}] : {}".format(self._name, self._counter))

