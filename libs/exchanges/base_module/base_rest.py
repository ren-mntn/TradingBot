# coding: utf-8
#!/usr/bin/python3

import asyncio
import time
import traceback

class RestAPIExchange(object):

    def __init__(self):

        self.HttpAccessTime = time.time()

        # 取引制限
        self._noTrade = False
        self.close_while_noTrade = False
        self._close_event = asyncio.Event()

    # 自動キャンセル実現のためのタスク
    async def _cancel_task(self):
        while self._logger.running:
            await asyncio.sleep(0.3)

            # オーダーの自動キャンセル
            try:
                cancel = [o for o in self.my.order.order_dict.items() if o[1]['expire']<time.time()]
                for id,value in cancel:
                    await self.cancelorder(id)
                    self._logger.debug('        in orderlist : {}'.format(value))
                    self._logger.info('        Cancel automatically : [{}]'.format(id))
            except Exception as e:
                self._logger.error("Error occured at _cancel_thread: {}".format(e))
                self._logger.info(traceback.format_exc())

            # no_trade期間でのクローズ処理
            if self._close_event.is_set() :
                self._close_event.clear()
                await self.close_position()

    @property
    def noTrade(self):
        return self._noTrade

    @noTrade.setter
    def noTrade(self, value):
        if value == False:
            self._noTrade = False
            return
        elif self._noTrade == False and self.close_while_noTrade :
            # no_trade期間でのクローズ処理
            self._close_event.set()
        self._noTrade = True

    async def close_position(self):
        current_pos = self.my.position.size
        if round(current_pos,8)==0:
            return {'stat': 0, 'msg': ""}
        if current_pos>0 :
            res = await self.sendorder(order_type='MARKET', side='SELL', size=current_pos)
            self.PendingUntil = max(self.PendingUntil, time.time()+30)
            return res
        else :
            res = await self.sendorder(order_type='MARKET', side='BUY', size=-current_pos)
            self.PendingUntil = max(self.PendingUntil, time.time()+30)
            return res
