# coding: utf-8
#!/usr/bin/python3

import asyncio
from logging import getLogger, StreamHandler, FileHandler, Formatter, addLevelName, DEBUG, INFO, WARNING, ERROR, NOTSET
import os
import time
import zipfile
from libs.utils.discord import NotifyDiscord
from libs.utils.scheduler import Scheduler

# 日付ごとにロールバックして圧縮するロガークラス
class MyLogger(object):
    def __init__(self, version_str='', console_log_level='INFO' ):
        self._last_day = '00'
        self._current_console_output_flag = True
        self._current_console_log_level = self._conv_level_str(console_log_level)
        self._current_file_log_level = INFO
        self._current_log_filename = ''
        self._log_folder = "logs/"
        self._version_str = version_str
        self.running = False

        logger = getLogger(__name__+version_str)
        logger.setLevel(5)
        addLevelName(5, 'TRACE')
        handler = StreamHandler()
        handler.setFormatter(Formatter(fmt='%(asctime)s.%(msecs)03d:  %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
        handler.setLevel(self._current_console_log_level)
        logger.addHandler(handler)

        self._logger = logger

        self.discord = NotifyDiscord(self)

        # 1秒ごとに実行されるコルーチンの登録
        self.call_every1sec = [{'name':'logupdate', 'handler':self._keep_update, 'interval':1, 'counter':0}]
        self._every1sec = Scheduler(self, interval=1, callback=self._run_every_1sec)

        # 別プロセスを停止させるコルーチン
        self.stop_handler = []

    # self.stop_handler に登録されているプロセスを停止
    async def stop(self):
        await asyncio.gather(*[self.stop_with_timeout(col) for col in self.stop_handler])

    async def stop_with_timeout(self, handler, timeout=5):
        try:
            await asyncio.wait_for(handler, timeout=timeout)
        except asyncio.TimeoutError:
            self._logger.info( "stop procedure timeout {}: ".format(timeout, handler) )

    def _conv_level_str(self, level):
        if level.upper()=='TRACE' :   return 5
        if level.upper()=='DEBUG' :   return DEBUG
        if level.upper()=='INFO' :    return INFO
        if level.upper()=='WARNING' : return WARNING
        if level.upper()=='ERROR' :   return ERROR

        self._logger.error( "Unknown log level [{}]".format(level) )
        return INFO

    def set_param_fh(self, log_folder='logs/', console_output=True, console_log_level='INFO', file_log_level='DEBUG'):
        self._update_filehandler(log_folder=log_folder, console_output=console_output,
                                 console_log_level=self._conv_level_str(console_log_level),
                                 file_log_level=self._conv_level_str(file_log_level))
        self.running = True

    def trace(self,str):
        self._logger.log(5,str)

    def debug(self,str, send_to_discord=False):
        self._logger.debug(str)
        if send_to_discord :
            self.discord.add_message(str)

    def info(self,str, send_to_discord=False):
        self._logger.info(str)
        if send_to_discord :
            self.discord.add_message(str)

    def warning(self,str, send_to_discord=False):
        self._logger.warning(str)
        if send_to_discord :
            self.discord.add_message(str)

    def error(self,str, send_to_discord=False):
        self._logger.error(str)
        if send_to_discord :
            self.discord.add_message(str)

    def exception(self,str):
        self._logger.exception(str)

    # 1秒ごとに実行されるコルーチン
    async def _run_every_1sec(self):
        for h in self.call_every1sec:
            h['counter'] +=1
            if h['counter'] % h['interval'] == 0 :
                await h['handler']()

    # 1秒ごとに日付などの変更チェック
    async def _keep_update(self):
        self._update_filehandler(log_folder=self._log_folder, console_output=self._current_console_output_flag,
                                     console_log_level=self._current_console_log_level,
                                     file_log_level=self._current_file_log_level)

    # 日付が変わった際にファイルハンドラーを更新
    def _update_filehandler(self, log_folder='logs/', console_output=True, console_log_level=INFO, file_log_level=DEBUG):
        if not os.path.exists(log_folder):
            os.makedirs(log_folder)

        day = time.strftime("%d")
        if( self._last_day != day or
            self._log_folder != log_folder or
            self._current_console_output_flag != console_output or
            self._current_console_log_level != console_log_level or
            self._current_file_log_level != file_log_level ):

            # 登録されているlogger.handlersをすべて除去
            for h in self._logger.handlers[0:]:
                self._logger.removeHandler(h)

            # Consoleへの出力ハンドラ（console_output=Falseなら代わりにファイルへ出力する)
            if console_output:
                handler = StreamHandler()
                handler.setFormatter(Formatter(fmt='%(asctime)s.%(msecs)03d:  %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
                handler.setLevel(console_log_level)
                self._logger.addHandler(handler)
            else:
                if os.path.exists(log_folder+'console.txt'):
                    os.remove(log_folder+'console.txt')
                handler = FileHandler(log_folder+'console.txt')
                handler.setFormatter(Formatter(fmt='%(asctime)s.%(msecs)03d:  %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
                handler.setLevel(console_log_level)
                self._logger.addHandler(handler)

            # ログファイルへの出力ハンドラ
            previous_filename = self._current_log_filename
            self._current_log_filename = 'trade' + time.strftime('%Y-%m-%d')

            # ファイルの日付が変わっているときだけ以前のログの圧縮を行う
            if previous_filename == self._current_log_filename:
                previous_filename = ''

            fh = FileHandler(log_folder + self._current_log_filename + '.log')
            fh.setFormatter(Formatter(fmt='%(asctime)s.%(msecs)03d[%(levelname)s]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
            fh.setLevel(file_log_level)
            self._logger.addHandler(fh)

            self._last_day = day
            self._log_folder = log_folder
            self._current_console_output_flag = console_output
            self._current_console_log_level = console_log_level
            self._current_file_log_level = file_log_level

            self.info("Initialize logger : BFSX3 {}".format(self._version_str))

            # ログの圧縮を別タスクで起動
            if previous_filename != '':
                asyncio.create_task(self._ziplog(log_folder, previous_filename), name="log_zipper")

    # ログの圧縮
    async def _ziplog(self,log_folder, previous_filename):
        with zipfile.ZipFile(log_folder + previous_filename + '.zip', 'w') as log_zip:
            log_zip.write(log_folder + previous_filename + '.log', arcname=previous_filename + '.log', compress_type=zipfile.ZIP_DEFLATED)
        os.remove(log_folder + previous_filename + '.log')
