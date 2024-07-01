# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy
from datetime import datetime, timedelta, timezone
from logging import getLogger,INFO,FileHandler
import asyncio
import os
from threading import Thread
import time
import zipfile

class MyStrategy(Strategy):

    # 初期化
    async def initialize(self):

        self.exchanges = []

        for exchange_name, symbol in self.parameters['exchanges']:
            # 指定された取引所とシンボルを取得するwebsocketクラスを作成
            websocket = self.ExchangeWebsocket(exchange=exchange_name, logger=self._logger)

            # 取引所クラスリストに追加
            self.exchanges.append( { 'websocket': websocket, 'name': websocket.exchange_name, 'symbol': symbol, 'counter':0 } )

        # websocketの購読スタート(非同期で一括スタート)
        await asyncio.gather(*[ex['websocket'].start(subscribe={'execution': True}, symbol=ex['symbol']) for ex in self.exchanges])

        for ex in self.exchanges:
            # 約定履歴受信のキューを作成
            ex['exec_list'] = self.ExecutionQueue( callback=self.executions, exchange=ex['websocket']) 

        # 定期的にカウンター表示
        self.Scheduler(callback=self.disp_stat, interval=10)


    # 約定データを処理
    async def executions(self):

        # ファイル名日付部を作成
        today_str = self.filename()

        for exchange in self.exchanges:

            # 初回は出力用ロガーを生成
            if'logger' not in exchange :
                exchange['logger'] = getLogger(exchange['name'] + exchange['symbol'])
                exchange['logger'].setLevel(INFO)
            logger = exchange['logger']

            # 日付が変わったらファイルハンドラを変更
            filename = exchange['name'] + '_' + exchange['symbol'].replace('/','-') + '_' + today_str
            if exchange.get('filename','') != filename :
                log_folder = self.parameters['folder'] + exchange['name'] + '/' + exchange['symbol'].replace('/','-') + '/'
                if not os.path.isdir(log_folder):
                    os.makedirs(log_folder)
                for h in logger.handlers :
                    logger.removeHandler(h)
                fh = FileHandler(log_folder+filename + '.csv')
                logger.addHandler(fh)

                # 前日のログ圧縮を別スレッドで起動
                if 'filename' in  exchange:
                    exchange['yesterday'] = exchange['filename']
                    Thread(target=self._ziplog, args=(log_folder, exchange['yesterday'])).start()
                exchange['filename'] = filename

            # 約定データの書き出し
            while len(exchange['exec_list'])!=0: 
                i = exchange['exec_list'].popleft() 
                logger.info("{},{},{},{},{},{}".format(i['exec_date'], i['side'], i['price'], i['size'], i['id'], exchange['websocket'].execution_info.avg_latency_1s))
                exchange['counter'] +=1


    # ファイルの圧縮
    def _ziplog(self,log_folder, previous_filename):
        self._logger.info( "Zipping {}.csv to {}.zip".format(previous_filename,previous_filename) )
        with zipfile.ZipFile(log_folder + previous_filename + '.zip', 'w') as log_zip:
            log_zip.write(log_folder + previous_filename + '.csv', arcname=previous_filename + '.csv', compress_type=zipfile.ZIP_DEFLATED)
        self._logger.info( "Zipped to {}.zip".format(previous_filename) )
        os.remove(log_folder + previous_filename + '.csv')
        self._logger.info( "Deleted {}.csv".format(previous_filename) )


    # 現在時刻のファイル名を生成（サーバー時刻によらずJSTで生成）
    def filename(self):
        return (datetime.utcfromtimestamp(time.time()) + timedelta(hours=9)).replace(tzinfo=timezone(timedelta(hours=9),'JST')).strftime('%Y-%m-%d')


    # 定期的にカウンター表示
    async def disp_stat(self):
        self._logger.info( '-'*50 )
        for exchange in self.exchanges:
            self._logger.info( "{:<25} : {}executions".format(exchange['name'] + ' / ' + exchange['symbol'], exchange['counter']) )
            exchange['counter'] = 0
