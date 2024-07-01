# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy
from libs.database.database import database
from libs.market.kawase import GaitameUSDJPY

from collections import deque
from datetime import datetime, timedelta
from datetime import time as datetime_time
from threading import Thread, Event
import time
import traceback
import socket

MAX_MESSAGE = 2048

class MyStrategy(Strategy):

    async def initialize(self):

        # グラフプロット用ハンドラの登録
        self.exchange.add_stats = self.add_stats
        self.exchange.daily_reset = self.daily_reset
        self._latest_summarize={'pos':0, 'profit':0, 'base':0}

        # 上位のpos_serverへ送る損益取得ハンドラの登録
        self._logger.get_size_handler = self.get_size
        self._logger.get_raw_size_handler = self.get_raw_size
        self._logger.get_profit_handler = self.get_profit

        # 為替情報の取得クラス
        from libs.market import GaitameUSDJPY
        self._kawase = GaitameUSDJPY(self._logger, keepupdate=True)
        self.usdjpy = await self._kawase.price

        # データ保管用
        self._database = {}

        # botごとの前回の損益額
        self._last_profit = {}

        # ずれの履歴
        self._diff_count = 0
        self._last_pos_diff = 0
        self._limit_try_count = 0

        # データ更新用キュー
        self._que = deque()

        # InfluxDBへの接続
        if 'influxdb' in self.parameters :
            influx_addr = self.parameters['influxdb']
            self.influxdb = database(self._logger, host=influx_addr[0], port=influx_addr[1], database='bots')
        else:
            self.influxdb = database(self._logger)

        # パケットの受信を別スレッドで起動
        comth = Thread(target=self.com_thread)
        comth.daemon = True
        comth.start()

        self.Scheduler(callback=self.delete_check, basetime=0.5, interval=1)
        self.Scheduler(callback=self.summarize_position, basetime=0, interval=30)

    def com_thread(self):

        while True:
            self.__com_read()

    def __com_read(self):

        # 通信の確立
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.settimeout(3)
        self._logger.info('Create UDP receiver : {}'.format(udp_sock))

        addr = self.parameters['recv_port'][0]
        if addr != 'localhost' and addr != '127.0.0.1':
            addr = '0.0.0.0'
        udp_sock.bind((addr, self.parameters['recv_port'][1]))

        stop_event = Event()
        recvth = Thread(target=self.__recv_thread, args=(stop_event,udp_sock))
        recvth.daemon = True
        recvth.start()

        # ウォッチドッグタイマーで監視 15 分受信が無ければソケットを廃棄・再作成
        self.udp_wdt = time.time()
        while time.time()-self.udp_wdt < 900:
            time.sleep( 1 )

        # 通信の終了
        stop_event.set()
        self._logger.error('Close UDP receiver : {}'.format(udp_sock))
        udp_sock.close()
        time.sleep( 30 )

    def __recv_thread(self, stop_event, udp_sock):
        while not stop_event.wait(0.1):
            # UDPパケットの受信
            try:
                packet, addr = udp_sock.recvfrom(MAX_MESSAGE)
            except socket.timeout as e :
                continue
            except Exception as e :
                self._logger.error('UDP socket error : {}\n{}'.format(e,traceback.print_exc()))
                break
            self.udp_wdt = time.time()

            # 受信データのデコード
            message = packet.decode('utf-8').split(':')
            symbol = message[0].strip()
            pos = float(message[1])
            base = float(message[2])
            profit = float(message[3])
            api1 = int(message[4])
            api2 = int(message[5])
            strategy = message[6].strip()

            # BFSX3 v1.50以降
            if len(message)==10:
                profit_currency = int(message[7])
                unitrate = float(message[8])
                raw_pos = float(message[9])

            # BFSX3 v1.42以前
            elif len(message)==9:
                profit_currency = int(message[7])
                unitrate = float(message[8])
                raw_pos = round(pos/self.exchange.units()['pos_rate'],8)

            # BFSX2
            else:
                profit_currency = self.exchange.units()['profit_currency']
                unitrate = 1
                raw_pos = float(message[1])

            # 自分自身は無視する
            if self.strategy_yaml_filename != strategy :
                data = {'pos': pos, 'base': base, 'profit': profit*unitrate, 'api1': api1, 'api2': api2,
                        'timestamp': time.time(), 'profit_currency': profit_currency, 'raw_pos': raw_pos}
                self._que.append( (strategy, data) )
                self._logger.debug("{} : {} : {}".format(symbol, strategy, data))

        self._logger.info('Exit recv_thread')

    # 別スレッドで受信しているデータをもとに更新
    async def __update_que(self):
        while len(self._que)!=0:
            strategy, data = self._que.popleft()
            self._database[strategy] = data

    # 300秒ポジション報告の無かったbotはリストから削除
    async def delete_check(self):
        await self.__update_que()
        for strategy, value in self._database.items():
            if time.time()-value['timestamp'] > 300:
                del self._database[strategy]
                break
        self.usdjpy = await self._kawase.price

    # API経由で実際のポジションを取得
    async def _check_current_pos(self):
        pos = await self.exchange.getpositions()
        long = 0
        short = 0
        for i in pos:
            self._logger.debug( "OpenPosition : {}".format(i) )
            if i['side'].upper()=='BUY' :
                long += float(i['size'])
            else :
                short += float(i['size'])
        total = round(long-short,8)
        self._logger.debug( "Long : {}  / Short : {}  = Total :{}".format(long,short,total))
        return total

    async def summarize_position(self):
        def time_in_range(start, end, x):
            if start <= end:
                return start <= x <= end
            else:
                return start <= x or x <= end

        await self.__update_que()

        summarize={'pos':0, 'raw_pos':0, 'profit':0}
        self._logger.info('\n\n')
        self._logger.info('-'*100)
        for strategy, value in self._database.items():
            self._logger.info("profit({:>+17.8f}){} : Pos({:>+12.8f})({:>+17.8f}) : Base({:>+10.3f}) : {:5.1f} : {}".format(
                        value['profit'],
                        'JPY' if value['profit_currency']==1 else 'USD',
                        value['pos'], value['raw_pos'], value['base'], time.time()-value['timestamp'], strategy))

            try:
                # dbへ保存する損益はJPYに統一
                if value['profit_currency']==1 :
                    profit = value['profit']
                else:
                    profit = value['profit']*self.usdjpy

                # 0:00～0:02はグラフに入れない
                now = datetime_time((datetime.utcnow()+timedelta(hours=9)).hour, (datetime.utcnow()+timedelta(hours=9)).minute, 0)
                if not time_in_range(datetime_time(0, 0, 0), datetime_time(0, 2, 0), now):

                    # 損益をInfluxに保存
                    self.influxdb.write( measurement="bfsx3",
                                         tags={'exchange': "{}_{}".format(self.exchange.exchange_name,self.symbol), 'bot': strategy},
                                         profit = profit,
                                         profit_diff = profit-self._last_profit.get(strategy,profit),
                                         position = value['pos'])
                    self._last_profit[strategy] = profit
                else:
                    self._last_profit[strategy] = 0
            except Exception as e:
                self._logger.exception("Error while exporting to InfluxDB : {}, {}".format(
                    e, traceback.print_exc()))

            summarize['pos'] += value['pos']
            summarize['raw_pos'] += value['raw_pos']
            if self.exchange.units()['profit_currency']==1 :
                summarize['profit'] += profit
            else:
                summarize['profit'] += (profit/self.usdjpy)

            if summarize.get('base',value['base'])!=value['base'] :
                self._logger.error('base_offset error')
            summarize['base'] = value['base']

        self._logger.info('-'*100)
        self._logger.info('         profit            position                      (     base            target  )            fromAPI             diff')

        # 実際のポジション取得
        actual = await self._check_current_pos() if self.exchange.auth else 0

        # 同じずれが繰り返すとカウントアップ
        pos_diff = round(actual-summarize['raw_pos']-summarize.get('base',0), 8)
        if self._last_pos_diff != pos_diff or abs(pos_diff)<self.minimum_order_size :
            self._diff_count = 0
        if abs(pos_diff)>=self.minimum_order_size and self._diff_count<5:
            self._diff_count += 1
        self._last_pos_diff = pos_diff
        if len(self._database)==0 :
            self._diff_count = 0

        self._logger.info('{:>+17.8f} {} : ({:>+12.8f}){:>+17.8f}  ({:>+10.3f} ={:>17.8f}) : {:>17.8f} : {:+17.8f} {}'.format(
                summarize['profit'],
                'JPY' if self.exchange.units()['profit_currency']==1 else 'USD',
                summarize['pos'], summarize['raw_pos'], summarize.get('base',0), summarize['raw_pos'] + summarize.get('base',0), actual, pos_diff,'*'*self._diff_count))
        self._latest_summarize = summarize

        # 4度続けてポジションがズレていれば成売買で補正行う
        if self.exchange.auth!=None and self.parameters.get('adjust_position',True) and self._diff_count>=4 :
            self._limit_try_count +=1
            maxsize = self.parameters.get('adjust_max_size',100)

            if self._limit_try_count> self.parameters.get('try_limit_order',0) :
                if pos_diff < 0:
                    await self.sendorder(order_type='MARKET', side='BUY', size=min(-pos_diff,maxsize))
                else:
                    await self.sendorder(order_type='MARKET', side='SELL', size=min(pos_diff,maxsize))
                self._diff_count = 0
            else:
                if pos_diff < 0:
                    await self.sendorder(order_type='LIMIT', side='BUY', size=min(-pos_diff,maxsize), price=self.ltp-self.parameters.get('limit_order_offset',0), auto_cancel_after=20)
                else:
                    await self.sendorder(order_type='LIMIT', side='SELL', size=min(pos_diff,maxsize), price=self.ltp+self.parameters.get('limit_order_offset',0), auto_cancel_after=20)
        else:
            self._limit_try_count =0


    def add_stats(self, keep=False):
        return {
               'timestamp': time.time(),

               'ltp': self.ltp,
               'current_pos': self._latest_summarize.get('raw_pos',0) ,
               'average': self.ltp,

               'realized': self._latest_summarize.get('profit',0)/self.exchange.units()['unitrate'],
               'commission': 0,
               'unreal': 0,

               'profit': self._latest_summarize.get('profit',0)/self.exchange.units()['unitrate'],
               'fixed_profit': self._latest_summarize.get('profit',0)/self.exchange.units()['unitrate'],

               'lantency': self.server_latency,
               'api1': self.api_remain1,
               'api2': self.api_remain2,

               'exec_vol': 0,
               'exec_vol_day': 0,

               'keep':keep,
               }

    def daily_reset(self):
        self._latest_summarize={'pos':self._latest_summarize.get('pos',0),
                                'raw_pos':self._latest_summarize.get('raw_pos',0),
                                'profit':0, 'base':0}

    def get_size(self):
        return self._latest_summarize.get('pos',0)

    def get_raw_size(self):
        return self._latest_summarize.get('pos',0)

    def get_profit(self):
        # BFSX2以前のpos_server互換のため、currencyレートに変換して送る
        return self._latest_summarize.get('profit',0)/self.exchange.units()['unitrate']
        