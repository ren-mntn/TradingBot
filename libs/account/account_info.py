# coding: utf-8
#!/usr/bin/python3

import asyncio
import time
from threading import Event
from collections import deque
from .orderlist import OrderList
from libs.utils.jsonfile import JsonFile

# 現在のポジション・注文リスト管理クラス
class AccountInfo(object):

    def __init__(self, logger, position_control_type, order_rate=1, order_currency='BTC'):
        self._logger = logger
        self.order = OrderList(logger)
        self.position = position_control_type(logger, self.update_profitfile, order_rate, order_currency)
        self.daily_exec_size = 0
        self._ltp = 0

        self._display_unit = str(order_rate)+order_currency

        self.file = JsonFile(self._logger)
        self._filename = ''
        self.update = Event()

        # 10秒ごとに損益を保存
        self._logger.call_every1sec.append({'name':'update_profitfile', 'handler':self.update_profitfile_async, 'interval':10, 'counter':0})

    def reload_profitfile(self, filename=None):
        self._filename = filename or self._filename
        tmp_prof = self.file.reload_file(self._filename)

        # 本日の0:00:05
        jst_zero = (time.time()+32400) // 86400 * 86400 - 32395
        prof_list = [p for p in tmp_prof if p['timestamp']>=jst_zero]

        for data in prof_list:
            self.position.realized = data.get('realized',self.position.realized)
            self.position.commission = data.get('commission',self.position.commission)

        self._logger.info( '-'*100 )
        self._logger.info( " realized = {} / commission = {}".format(self.position.realized, self.position.commission)) 
        self._logger.info( '-'*100 )

        # 過去のデータは削除して上書き
        self.file.renew_file( prof_list )

    async def update_profitfile_async(self):
        self.update_profitfile()

    def update_profitfile(self):
        self.file.add_data({'timestamp':time.time(), 'realized':self.position.realized, 'commission':self.position.commission, 'unreal':self.position.unreal} )
        self.update.set()

    def disp_stats(self):
        self.daily_exec_size += self.order.executed_size
        self._disp_str( "---------------------Order counts" )
        self._disp_str( "    ordered        : {}".format(self.order.ordered_count) )
        self._disp_str( "    order filled   : {}".format(self.order.filled_count) )
        self._disp_str( "    partial filled : {}".format(self.order.partially_filled_count) )
        self._disp_str( "    order cancelled: {}\n".format(self.order.canceled_count) )
        self._disp_str( "    executed volume /h  : {:.2f} x {}".format(self.order.executed_size, self._display_unit) )
        self._disp_str( "    exec volume today   : {:.2f} x {}".format(self.daily_exec_size, self._display_unit) )
        self._disp_str( "" )
        self.order.reset_counter()
        self._logger.discord.flush_message()

    def _disp_str(self, str):
        self._logger.info( str, send_to_discord=True)

