# coding: utf-8
#!/usr/bin/python3

import asyncio
from collections import deque
from ctypes import c_bool
import multiprocessing
import os
import queue
import signal
import traceback
import pybotters
import time
from threading import Thread
from logging import getLogger, StreamHandler, FileHandler, Formatter, INFO, DEBUG

class SubProc_WS():
    def __init__(self, event_queue, command_queue, proc_stop, connected, **argv ):
        self._event_queue = event_queue
        self._command_queue = command_queue
        self._endpoint = argv['endpoint']
        self._apis = argv['apis']
        self._send_json = argv.get('send_json')
        self._options = argv.get('options')
        self._subscribe_json = deque()
        self._connected = connected
        self._stop = proc_stop

    def start_ws(self, exchange_name, logger=None):
        # websocketロガーが指定されている場合にはロギングを行う
        self.logger = logger
        if self.logger :
            self.logger.setLevel(DEBUG)
            fh = FileHandler(exchange_name+'_websocket.log')
            fh.setFormatter(Formatter(fmt='%(asctime)s.%(msecs)03d: %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
            fh.setLevel(INFO)
            self.logger.addHandler(fh)
            self.logger.info( "Websocket Logging Start" )

        try:
            # Ctrl+Cのシグナルを無効にしておく。(メインからのstop()で終了させるので。)
            signal.signal(signal.SIGINT,  signal.SIG_IGN)

            asyncio.run(self._main())

        except Exception:
            self._put_event(('logger.error',traceback.format_exc()))

    def _onmessage_handler(self, msg, ws):
        asyncio.create_task(self._onmessage(msg, ws))

    async def _main(self):
        try:
            self._connected.value = False
            self._client = pybotters.Client(apis=self._apis)
            send_json = self._send_json
            self.ws = await self._client.ws_connect(self._endpoint, send_json=send_json, hdlr_json=self._onmessage_handler, heartbeat=10.0, **self._options)
            self._put_event( ('pid', os.getpid()) )
            self._put_event(('logger.info', "websocket connect to  : {}".format(self._endpoint)))
            self._put_event(('logger.info', "subscribe : {}".format(send_json)))

            self._stop.value = False
            while not self._stop.value:
                await asyncio.sleep(1)
                try:
                    command = self._command_queue.get(block=False)
                except queue.Empty:
                    if self.logger:
                        self.logger.info("connected : {}".format(self.ws.current_ws is not None))
                    if self._connected.value != (self.ws.current_ws is not None):
                        self._put_event(('status_change', self.ws.current_ws is not None))
                        self._connected.value = (self.ws.current_ws is not None)
                    else:
                        self._put_event(('wdt', self.ws.current_ws is not None))

                    continue

                if command[0] == 'subscribe' :

                    self._subscribe_json.append(command[1])

                    send_json.clear()
                    send_json += command[2]

        except Exception:
            self._put_event(('logger.error',traceback.format_exc()))

        self._put_event(('logger.info', "websocket stopped"))

    async def _onmessage(self, msg, ws):
        self._put_event(('msg', msg, time.time()))
        if self.logger :
            self.logger.info("{}".format(msg.keys()))

        if self._subscribe_json:
            send_json = self._subscribe_json.popleft()
            self._put_event(('logger.info', "subscribed : {}".format(send_json)))
            await ws.send_json(send_json)

    def _put_event(self,d):
        self._event_queue.put(d)

class ClientWebSocketResponse():
    def __init__(self):
        dummy = True

class MultiProc_WS():
    def __init__(self, logger, exchange_name, handler, endpoint, apis, send_json, lock, disconnect_handler=None, **options ):
        self._logger = logger
        self._exchange_name = exchange_name
        self._handler = handler
        self._endpoint = endpoint
        self._apis = apis
        self._send_json = send_json
        self._lock = lock
        self._disconnect_handler = disconnect_handler
        self._options = options
        self.pid = None
        self._time_lag = deque(maxlen=100)
        self._stop = False
        self._running = False
        self._disconnected = False
        self._discord_queue = deque(maxlen=10)
        self.wdt = 0

    def _mean(self,q) : return sum(q) / (len(q) + 1e-7)

    async def _start_websocket(self, logging=False):
        Thread(target=self._main_loop, daemon = True).start()

        # 停止ハンドラーの登録
        self._logger.stop_handler.append(self.stop())

        self._keep_connect = True
        while self._keep_connect :
            self._keep_connect = False
            self.wdt = 0

            # キューの再作成
            if hasattr(self,'_event_queue') :
                self._event_queue.put(('delete',))
                while hasattr(self,'_event_queue') :
                    await asyncio.sleep(1)
            self._event_queue = multiprocessing.Queue()
            self._command_queue = multiprocessing.Queue()
            self._proc_stop = multiprocessing.Value(c_bool, False)
            self._connected = multiprocessing.Value(c_bool, False)

            self._running = True
            ws_proc = SubProc_WS( self._event_queue, self._command_queue, self._proc_stop, self._connected,
                            endpoint=self._endpoint,
                            apis=self._apis,
                            send_json = self._send_json,
                            options = self._options)

            if logging :
                self._ws_logger = getLogger(__name__+self._exchange_name)
            else:
                self._ws_logger = None
            sub_process = multiprocessing.Process(target=ws_proc.start_ws, args=(self._exchange_name, self._ws_logger,), daemon=True)
            sub_process.start()

            self._stop = False
            while not self._stop:
                await asyncio.sleep(1)

                # ウォッチドックタイマー
                self.wdt += 1
                if logging :
                    self._logger.trace( "{} WDT : {}".format(self._exchange_name, self.wdt) )
                if self.wdt > 30:
                    # 切断時には切断処理を行うハンドラーを呼び出す
                    self._discord_queue.append( '@everyone\n{}Websocket: Freezed. Re-connect WS'.format(self._exchange_name) )
                    if self._disconnect_handler :
                        await self._disconnect_handler()
                    self._stop = True
                    self._keep_connect = True
                    asyncio.create_task(self.stop())

                # 切断時には切断処理を行うハンドラーを呼び出す
                if self._disconnected and self._disconnect_handler :
                    self._logger.info( "catch disconnect event" )
                    self._disconnected = False
                    await self._disconnect_handler()

                # Discord への通知処理
                while len(self._discord_queue)!=0 :
                    self._logger.discord.send( self._discord_queue.popleft() )
 
            self._proc_stop.value = True
            await asyncio.sleep(2)

            self._logger.info( "[{}:websocket] terminate process".format(self._exchange_name) )
            sub_process.terminate()
            sub_process.join()
            self._running = False
            await asyncio.sleep(5)            


    # プロセスの停止
    async def stop(self):
        self._logger.info( "[{}:websocket] stopping..".format(self._exchange_name) )
        self._stop = True
        timeout = 10
        while self._running and timeout>0:
            await asyncio.sleep(1)
            timeout -= 1
            self._logger.info( "[{}:websocket] waiting until stopped ({})".format(self._exchange_name, timeout) )
        self._logger.info( "[{}:websocket] process stopped".format(self._exchange_name) )

    def _main_loop(self):
        ws = ClientWebSocketResponse() # ダミー

        while True:
            while not hasattr(self,'_event_queue') :
                time.sleep(1)
            data = self._event_queue.get()

            # wsメッセージ受信
            if data[0] == 'msg' :
                self._logger.ws_timestamp = data[2]
                self._time_lag.append(time.time()-data[2])
                self._lock.wait()
                self._handler(data[1],ws)
                if self.wdt<30 :
                    self.wdt = 0

            # 起動確認
            elif data[0] == 'pid' :
                self.pid = int(data[1])
                self._logger.info( "[{}:websocket] pid = {}".format(self._exchange_name,self.pid) )

            # 起動確認
            elif data[0] == 'status_change' :
                if data[1] :
                    self._discord_queue.append( '{}Websocket: connected'.format(self._exchange_name) )
                    self._logger.info( '[{}:websocket] connected'.format(self._exchange_name) )
                else:
                    self._discord_queue.append( '@everyone\n{}Websocket: disconneccted.'.format(self._exchange_name) )
                    self._logger.info( '[{}:websocket] disconneccted'.format(self._exchange_name) )
                    self._disconnected = True

            # ウォッチドックタイマー
            elif data[0] == 'wdt' :
                if self.wdt<30 :
                    self.wdt = 0

            # ログへの出力を代行
            elif data[0] == 'logger.trace' :
                self._logger.trace( "[{}:websocket] {}".format(self._exchange_name,data[1]) )

            elif data[0] == 'logger.debug' :
                self._logger.debug( "[{}:websocket] {}".format(self._exchange_name,data[1]) )

            elif data[0] == 'logger.info' :
                self._logger.info( "[{}:websocket] {}".format(self._exchange_name,data[1]) )

            elif data[0] == 'logger.warning' :
                self._logger.warning( "[{}:websocket] {}".format(self._exchange_name,data[1]) )

            elif data[0] == 'logger.error' :
                self._logger.info( "[{}:websocket error] {}".format(self._exchange_name,data[1]) )

            elif data[0] == 'delete' :
                del self._event_queue

            else :
                self._logger.info( "[{}:websocket msg] {}".format(self._exchange_name,data) )

    @property
    def time_lag(self):
        return self._mean(self._time_lag)

    @property
    def connected(self):
        return self._connected.value

    def send_subscribe(self, data, args):
        self._command_queue.put(('subscribe',data, args))

    # 旧バージョンのpybottersとの互換のため
    @property
    def conneted(self):
        return self.connected
