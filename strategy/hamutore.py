# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy
from collections import deque
import pickle
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import time
import csv
import matplotlib
import matplotlib.ticker as ticker
import pandas
import asyncio
from socket import socket, AF_INET, SOCK_DGRAM
import traceback

from threading import Thread, Event

from libs.utils.jsonfile import JsonFile

class MyStrategy(Strategy):

    # 最初に呼び出される関数
    async def initialize(self):

        self._order_list = deque(maxlen=50)        # 発注リスト
        self._ordered_list = {}                     # 発注済みリスト（約定履歴をキャッチして平均約定価格を算出するため）
        self._last_current_pos = self.current_pos
        self._last_date = datetime.now()
        self._last_exec = 0

        # 損益グラフプロット用の変数
        self._profit_history = {}
        self._profit_history_first_dt = 0
        self._market_history = []
        self._plot_start_offset = {}

        # Discord 通知用バッファ
        self._message = ''

        # SFD強制クローズ後のウェイト(SFD強制クローズ後300秒は再度のSFDクローズを行わない）
        self.sfd_wait = time.time()-300

        # 取引履歴を復元
        self._historyfile = JsonFile(self._logger)
        history = self._historyfile.reload_file(self.log_folder+'order_history.json')
#        print( history )
        if history:
            for h in history :
                self._append_to_profit_history(h)
        else:
            self._profit_history_first_dt = datetime.now()

        # ポジションファイルから各ロジックの現在ポジションと平均価格を復元
        self._posfile = JsonFile(self._logger)
        position = self._posfile.reload_file(self.log_folder+'logic_position.json')
        self._position_dict = {}
        for p in position:
            self._position_dict[p['logic']] = {'size':p['size'], 'price':p['price']}

        # 現在のポジションを表示
        self._send_position_info()

        # 取引履歴から累積したポジションと現在ポジの相違をロジックごとに計算
        for logic, pos in self._position_dict.items():
            if self._profit_history.get(logic) != None:
                self._plot_start_offset[logic] = round(pos['size']-self._profit_history[logic][-1]['pos'], 8)

        # オフセットを適用して取引履歴を再度読み直す
        self._profit_history = {}
        self._market_history = []
        history = self._historyfile.reload_file()
        if history:
            for h in history :
                # LTPを記録したデータとlogic_position.json に登録されいてるロジックだけ読み込む
                if h.get('logic')=='' or self._position_dict.get(h.get('logic')):
                    self._append_to_profit_history(h)

        # 売買イベントが発生したらrealtime_logic()を呼び出すスレッド
        self.order_signal_event = asyncio.Event()
        asyncio.create_task(self._wait_event(), name="hamutore_order")

        # 損益グラフをプロット
        self._plot_profit_graph(start_datetime=datetime(datetime.now().year, datetime.now().month, 1))

        # loop_period間隔で callback で指定したloss_cut_check関数が呼び出されるように設定
        self.Scheduler(callback=self.loss_cut_check, interval=5)

        # 定期的に LTP をファイルに保存しておく（後にグラフ化するため）
        self.Scheduler(callback=self._write_ltp, interval=60)

        # 約定を検知したら executions() を呼び出す
        self.AddExecutionHandler(callback=self.executions)


    # 売買イベントが発生したらrealtime_logic()を呼び出す
    async def _wait_event(self):
        while True:
            await self.order_signal_event.wait()
            self.order_signal_event.clear()            
            await self.realtime_logic()

    # トレードシグナルに応じた売買処理
    async def handle_trade_signal(self, signal_logic_name, signal_order_side, signal_order_size, signal_order_comment):
        try:
            # ロットサイズを決定（ロジック設定ファイル内に指定があればそのロット、なければデフォルトのlotsize）
            if signal_logic_name in self.parameters['logic_lot']:
                lot, maxlot = self.parameters['logic_lot'][signal_logic_name][0], self.parameters['logic_lot'][signal_logic_name][1]

                # SFDモードの場合 maxlot=0 (新規ポジ建て無し・クローズ方向のみ)
                if len(self.parameters['logic_lot'][signal_logic_name]) == 3 and self.parameters['logic_lot'][signal_logic_name][2] != 0 and 'sfd_entry_limit' in self.parameters and abs(self.sfd) > self.parameters['sfd_entry_limit']:
                    maxlot = 0

            else:
                lot, maxlot = self.parameters['lotsize'], self.parameters['max_lot']

            # 最大ポジを超えないように調整
            lot *= signal_order_size
            if signal_logic_name != "":
                if signal_logic_name not in self._position_dict.keys():
                    self._position_dict[signal_logic_name] = {'size': 0, 'price': 0}

                if signal_order_side == 'BUY':
                    lot = min(maxlot-self._position_dict[signal_logic_name]['size'], lot)

                elif signal_order_side == 'SELL':
                    lot = min(maxlot+self._position_dict[signal_logic_name]['size'], lot)

                elif signal_order_side == 'CLOSE':

                    if self._position_dict[signal_logic_name]['size']<0 :
                        signal_order_side = 'BUY'
                        lot = - self._position_dict[signal_logic_name]['size']

                    else:
                        signal_order_side = 'SELL'
                        lot = self._position_dict[signal_logic_name]['size']

            # 注文情報をオーダーリストに放り込んで売買シグナルセット
            if signal_order_side in ['BUY', 'SELL']:
                if lot >= self.minimum_order_size:
                    self._order_list.append({'type': 'MARKET', 'side': signal_order_side,
                                             'size': round(lot, 8), 'logic': signal_logic_name,
                                             'comment': signal_order_comment, 'time': time.time(), 'executed' : 0})
                    self.order_signal_event.set()   
                else:
                    raise ValueError(
                        "Lot size is too small or limited by maximum position setting.")
            else:
                raise ValueError("Side shoud be BUY or SELL!")

        except:
            self._message += traceback.format_exc(limit=0)
            self._logger.exception(Exception)


    # TradingViewからのトレードシグナルの解読
    async def parse_trade_signal(self, trade_signal):
        signal_message = trade_signal.split(',')                # カンマ区切りで分割
        self._logger.info("Catch Trade Signal Event")

        self._logger.info(signal_message)
        if len(signal_message) == 5:  # はむとれのシグナルはカンマ区切りで５個
            signal_logic_name = signal_message[1].replace(' ', '')  # ロジック名
            # 'BUY'/'SELL'
            if signal_message[2].replace('-', '').isnumeric():
                if   int(signal_message[2]) > 0 : signal_order_side = "BUY"
                elif int(signal_message[2]) < 0 : signal_order_side = "SELL"
                else                            : signal_order_side = "CLOSE"
            else:
                signal_order_side = signal_message[2].upper()
            signal_order_size = int(signal_message[3])              # 売買サイズ
            signal_order_comment = signal_message[4]                # 表示コメント
            await self.handle_trade_signal(signal_logic_name, signal_order_side, signal_order_size, signal_order_comment)

        elif len(signal_message) == 4:  # カンマ区切りで４個の場合には　旧タイプの　1 strategy 1 alert
            signal_logic_name = signal_message[1].replace(' ', '')   # ロジック名
            signal_order_side = 'BUY' if int(signal_message[2]) > 0 else 'SELL'
            signal_order_size = abs(int(signal_message[2]))          # 売買サイズ
            signal_order_comment = signal_message[3]                 # 表示コメント
            await self.handle_trade_signal(signal_logic_name, signal_order_side, signal_order_size, signal_order_comment)


    # Discordへ書き込みがあれば呼び出される関数
    async def discord_on_message(self, message):

        # カンマで始まるメッセージならトレードシグナル　（例　,MotuChaosMod_bF,SELL,1,SHORT2）
        if message.content.startswith(','):
            await self.parse_trade_signal(message.content)  # シグナルの処理

        elif message.content.startswith("/plotall"):   # /plotコマンドで損益グラフのプロット
            cmd_str = message.content.split(' ')
            if len(cmd_str) == 2:
                self._plot_profit_graph(logic=cmd_str[1].replace(' ', ''))

            elif len(cmd_str) == 3 and cmd_str[1].upper() == 'TOP':
                self._plot_profit_graph(top=int(cmd_str[2]))

            elif len(cmd_str) == 3 and cmd_str[1].upper() == 'BOTTOM':
                self._plot_profit_graph(bottom=int(cmd_str[2]))

            else:
                self._plot_profit_graph()

        # /plotコマンドで損益グラフのプロット(当月のみ)
        elif message.content.startswith("/plot"):
            cmd_str = message.content.split(' ')

            # /plot <startdate>
            if len(cmd_str) == 2 and cmd_str[1].isdecimal() and len(cmd_str[1]) == 8:
                self._plot_profit_graph(start_datetime=datetime(int(cmd_str[1][:4]), int(cmd_str[1][4:6]), int(cmd_str[1][6:])))

            # /plot <strategy>
            elif len(cmd_str) == 2:
                self._plot_profit_graph(logic=cmd_str[1].replace(' ', ''),
                                        start_datetime=datetime(datetime.now().year, datetime.now().month, 1))

            # /plot TOP <num>
            elif len(cmd_str) == 3 and cmd_str[1].upper() == 'TOP' and cmd_str[2].isdecimal():
                self._plot_profit_graph(top=int(cmd_str[2]),
                                        start_datetime=datetime(datetime.now().year, datetime.now().month, 1))

            # /plot BOTTOM <num>
            elif len(cmd_str) == 3 and cmd_str[1].upper() == 'BOTTOM' and cmd_str[2].isdecimal():
                self._plot_profit_graph(bottom=int(cmd_str[2]),
                                        start_datetime=datetime(datetime.now().year, datetime.now().month, 1))

            # /plot <startdate> <enddate>
            elif len(cmd_str) == 3 and cmd_str[1].isdecimal() and len(cmd_str[1]) == 8 and cmd_str[2].isdecimal() and len(cmd_str[2]) == 8:
                self._plot_profit_graph(start_datetime=datetime(int(cmd_str[1][:4]), int(cmd_str[1][4:6]), int(cmd_str[1][6:])),
                                        endt_datetime=datetime(int(cmd_str[2][:4]), int(cmd_str[2][4:6]), int(cmd_str[2][6:]), 23, 59))

            # /plot <strategy> <startdate>
            elif len(cmd_str) == 3 and cmd_str[2].isdecimal() and len(cmd_str[2]) == 8:
                self._plot_profit_graph(logic=cmd_str[1].replace(' ', ''),
                                        start_datetime=datetime(int(cmd_str[2][:4]), int(cmd_str[2][4:6]), int(cmd_str[2][6:])))

            # /plot TOP <num> <startdate>
            elif len(cmd_str) == 4 and cmd_str[1].upper() == 'TOP' and cmd_str[2].isdecimal() and cmd_str[3].isdecimal() and len(cmd_str[3]) == 8:
                self._plot_profit_graph(top=int(cmd_str[2]),
                                        start_datetime=datetime(int(cmd_str[3][:4]), int(cmd_str[3][4:6]), int(cmd_str[3][6:])))

            # /plot BOTTOM <num> <startdate>
            elif len(cmd_str) == 4 and cmd_str[1].upper() == 'BOTTOM' and cmd_str[2].isdecimal() and cmd_str[3].isdecimal() and len(cmd_str[3]) == 8:
                self._plot_profit_graph(bottom=int(cmd_str[2]),
                                        start_datetime=datetime(int(cmd_str[3][:4]), int(cmd_str[3][4:6]), int(cmd_str[3][6:])))

            # /plot <strategy> <startdate> <enddate>
            elif len(cmd_str) == 4 and cmd_str[2].isdecimal() and len(cmd_str[2]) == 8 and cmd_str[3].isdecimal() and len(cmd_str[3]) == 8:
                self._plot_profit_graph(logic=cmd_str[1].replace(' ', ''),
                                        start_datetime=datetime(int(cmd_str[2][:4]), int(cmd_str[2][4:6]), int(cmd_str[2][6:])),
                                        endt_datetime=datetime(int(cmd_str[3][:4]), int(cmd_str[3][4:6]), int(cmd_str[3][6:]), 23, 59))

            # /plot TOP <num> <startdate> <enddate>
            elif len(cmd_str) == 5 and cmd_str[1].upper() == 'TOP' and cmd_str[2].isdecimal() and cmd_str[3].isdecimal() and len(cmd_str[3]) == 8 and cmd_str[4].isdecimal() and len(cmd_str[4]) == 8:
                self._plot_profit_graph(top=int(cmd_str[2]),
                                        start_datetime=datetime(int(cmd_str[3][:4]), int(cmd_str[3][4:6]), int(cmd_str[3][6:])),
                                        endt_datetime=datetime(int(cmd_str[4][:4]), int(cmd_str[4][4:6]), int(cmd_str[4][6:]), 23, 59))

            # /plot BOTTOM <num> <startdate> <enddate>
            elif len(cmd_str) == 5 and cmd_str[1].upper() == 'BOTTOM' and cmd_str[2].isdecimal() and cmd_str[3].isdecimal() and len(cmd_str[3]) == 8 and cmd_str[4].isdecimal() and len(cmd_str[4]) == 8:
                self._plot_profit_graph(bottom=int(cmd_str[2]),
                                        start_datetime=datetime(int(cmd_str[3][:4]), int(cmd_str[3][4:6]), int(cmd_str[3][6:])),
                                        endt_datetime=datetime(int(cmd_str[4][:4]), int(cmd_str[4][4:6]), int(cmd_str[4][6:]), 23, 59))

            else:
                self._plot_profit_graph(start_datetime=datetime(datetime.now().year, datetime.now().month, 1))

        elif message.content == "/position":  # /positionコマンドでポジションリストの送信
            self._send_position_info()

        elif message.content == "/help":
            await message.channel.send("/plot\n" +
                                       "/plot <startdate>\n" +
                                       "/plot <startdate> <enddate>\n\n" +
                                       "/plot <strategy>\n" +
                                       "/plot <strategy> <startdate>\n" +
                                       "/plot <strategy> <startdate> <enddate>\n\n" +
                                       "/plot top <num>\n" +
                                       "/plot top <num> <startdate>\n" +
                                       "/plot top <num> <startdate> <enddate>\n\n" +
                                       "/plot bottom <num>\n" +
                                       "/plot bottom <num> <startdate>\n" +
                                       "/plot bottom <num> <startdate> <enddate>\n\n" +
                                       "/plotall\n" +
                                       "/plotall <strategy>\n" +
                                       "/plotall top <num>\n" +
                                       "/plotall bottom <num>\n" +
                                       "/position\n" +
                                       ",StrategyName,<BUY/SELL>,leverage,comment")


    # 約定を検知したら呼び出される関数
    async def executions(self):
        exec_list = self.executed_history

        self._logger.info(exec_list)
        # 今回追加された約定履歴だけを抜き出す
        at_this_time = [e for e in exec_list if e['exec_time']>self._last_exec]
        self._last_exec = exec_list[-1]['exec_time']

        # オーダー済み情報からロジック名などを見つけてDiscordへ通知
        for r in at_this_time:
            if self._ordered_list.get(r['id']) != None:
                order = self._ordered_list[r['id']]
                message = "(Execution for [{}]logic : side:{} size:{} price:{})".format(
                           order['logic'], order['side'],  r['size'], r['price'])
                self._logger.info(message)
                self._message += message
                self._message += '\n'


    # 売買シグナルがセットされたら呼び出される関数
    async def realtime_logic(self):
        self._logger.info(self._order_list)

        # オーダーリストに入っているのでオーダー時間が過ぎているものをすべてオーダー
        while len([o['time'] for o in list(self._order_list) if o['time'] < time.time()]) != 0:
            order = self._order_list.popleft()   # 一つ取り出してオーダー処理

            if order['time'] >= time.time():
                self._order_list.append(order)  # リトライ時間が来ていないものはキューに戻す
                continue

            self._logger.info("Trade Signal for [{}]logic : side:{} size:{} comment:{}".format(order['logic'], order['side'],  order['size'], order['comment']))

            if self.no_trade_period:
                response = {'msg': "No trade period. Pending : [{}]logic : side:{} size:{} comment:{}".format(order['logic'], order['side'],  order['size'], order['comment'])}

            else:
                response = await self.sendorder(order_type='MARKET', side=order['side'], size=order['size'])  # オーダー発出

            ids = response.get('ids')

            # レスポンスにidが割り当てられていればオーダー成功
            if ids:
                # オーダー発出時間に書き換え（この時間から120秒後までに約定通知が無ければリトライ）
                order['time'] = time.time()

                # 分割オーダーに対応して全IDを保存
                order['ids'] = ids

                # 発出済みのオーダーのidをキーにしてオーダー情報を控えておく
                self._ordered_list[ids[0]] = order

                if order['type'] == 'MARKET':
                    message = "{}: ordered id: {} side:{} size:{}".format(datetime.now().strftime('%H:%M:%S'), id, order['side'], round(order['size'], 8))
                else:
                    message = "{}: ordered id: {} side:{} price:{:.0f} size:{}".format(datetime.now().strftime('%H:%M:%S'), id, order['side'], order['price'], round(order['size'], 8))
                self._message += message      # Discordへ送信
                self._message += '\n'

            # オーダーエラー
            else:
                # responseをDiscordへ送信
                self._message += "response:{}\n".format(response)

                # ここにはオーダーしないエラーレスポンスを列記　↓↓↓↓
                if response.get('msg','') not in ['Margin amount is insufficient for this order.',
                                                  'The price is too low.',
                                                 ] :
                    # オーダー失敗は[retry_interval]秒後に再オーダーすることにして
                    order['time'] = time.time() + self.parameters['retry_interval']
                    self._order_list.append(order)    # self._order_list に戻す


    # 5秒に1回呼び出される関数
    async def loss_cut_check(self):
        self._logger.info('[LTP:{:9.1f}] Profit:{:+.8f}({:+.8f}) Position:{:.3f} API:{:>3} Delay:{:>4.0f}ms'.format(
            self.ltp, self.current_profit, self.current_profit_unreal, self.current_pos, self.api_remain1, self.server_latency))

        # 直近の約定履歴
        executed = self.executed_history

        # オーダー済みのリストがあれば、約定履歴の中にあるかチェック
        pos_updated = False
        if len(self._ordered_list.keys()) != 0:
            order_list = deepcopy(self._ordered_list)
            for key, value in order_list.items():

                # 約定履歴から当該IDの約定数(合計)を計算
                total_size = round(sum([e['size'] for e in executed if e['id'] in value['ids']]), 8)

                # すべて約定していたら
                if total_size >= round(value['size'], 8):
                    # 平均約定価格を計算
                    average_price = round(sum([e['size']*e['price'] for e in executed if e['id'] in value['ids']])/total_size, 1)

                    # オーダーリストから削除
                    del self._ordered_list[key]

                    message = "Filled id: {} size:{} average:{:.1f}  [{}]".format(value['ids'], round(value['size'], 8), average_price, value['logic'])
                    self._logger.info(message)    # ログへ表示
                    self._message += message      # Discordへ送信
                    self._message += '\n'

                    # 約定履歴リストに追加（損益計算用）
                    self._write_history({'time':time.time(), 'logic':value['logic'], 'id':key,
                                         'size':value['size'] if value['side'] == 'BUY' else -value['size'],
                                         'price':average_price})

                    # ロジックごとのポジション
                    if value['logic'] != "":
                        if value['logic'] not in self._position_dict.keys():
                            self._position_dict[value['logic']] = {'size': 0, 'price': 0}
                        self._position_dict[value['logic']]['size'] += (value['size'] if value['side'] == 'BUY' else -value['size'])
                        self._position_dict[value['logic']]['price'] = average_price
                        pos_updated = True

                # 部分約定
                elif total_size != 0:
                    self._logger.info("Partially filled id: {} size:{} [{}]".format(value['ids'], total_size, value['logic']))
                    self._ordered_list[key]['executed'] = total_size

        if pos_updated == True:
            # テキストファイルへ保存
            self._posfile.renew_file([dict({'logic':logic},**pos) for logic, pos in self._position_dict.items()])

        # オーダーして120秒以上残っているものはリトライ（一部でも約定通知を検知したものはリトライしない）
        failed_order = [[id, order] for id, order in self._ordered_list.items() if order['time'] < time.time()-120 and order['executed']==0]
        for order in failed_order:
            self._ordered_list[order[0]]['time'] = time.time()
            order[1]['time'] = time.time()      # 発注時刻を現在時刻に
            self._logger.info("Order retry [{}] [{}]".format(order[0], order[1]['logic']))
            del self._ordered_list[order[0]]             # リトライするものは発注済みリストから削除して
            self._order_list.append(deepcopy(order[1]))  # 新規オーダーを self._order_list に入れる

        # オーダーして10分以上経ったら捨てる
        expired_order = [[id, order] for id, order in self._ordered_list.items() if order['time'] < time.time()-600]
        for order in expired_order:
            del self._ordered_list[order[0]]    # 発注済みリストから削除して
            self._logger.info("Clear from ordered_list [{}] [{}]".format(order[0], order[1]['logic']))

        # ポジションが変わったらDiscordへ通知
        if self._last_current_pos != self.current_pos:
            self._message += "Position chaned {:.3f} --> {:.3f}\n".format(self._last_current_pos, self.current_pos)
            self._last_current_pos = self.current_pos
            self._send_position_info()

        # 約定メッセージがあればDiscordへ通知
        if self._message != '':
            self.send_discord(self._message)
            self._message = ''

        # １日に損益グラフをプロット(当月分のみ)
        if self._last_date.day != datetime.now().day:
            self._plot_profit_graph(start_datetime=datetime(datetime.now().year, datetime.now().month, 1))

        self._last_date = datetime.now()

        # SFDリミットチェック
        if 'sfd_close_limit' in self.parameters and abs(self.sfd) > self.parameters['sfd_close_limit'] and time.time()-self.sfd_wait > 300:
            self.sfd_wait = time.time()

            # SFD時にクローズするロジックのリスト（現在ポジがあり、SFD mode=1のもの）
            sfd_close_list = [{'logic': key, 'size': value['size']}
                              for key, value in self._position_dict.items()
                              if round(value['size'], 8) != 0 and
                              key in self.parameters['logic_lot'] and
                              len(self.parameters['logic_lot'][key]) == 3 and
                              self.parameters['logic_lot'][key][2] != 0]
            for s in sfd_close_list:
                self._message += 'SFD Close(Strategy:{} Size:{:.2f})\n'.format(
                    s['logic'], s['size'])
                self._order_list.append({'type': 'MARKET', 'side': 'BUY' if s['size'] < 0 else 'SELL', 'size': round(
                    abs(s['size']), 8), 'logic': s['logic'], 'comment': 'SFD Close', 'time': time.time()})
            self.order_signal_event.set()   

        # リトライするオーダーがあればシグナルセット
        retry_list = [o for o in list(self._order_list) if o['time'] < time.time()]
        if len(retry_list) != 0:
            self.order_signal_event.set()   

        return False



    # 定期的に LTP をファイルに保存しておく（後にグラフ化するため）
    async def _write_ltp(self):
        self._write_history({'time':time.time(), 'logic':'', 'id':None, 'size':0, 'price':self.ltp})

    # 取引ヒストリーファイルへ書き出し
    def _write_history(self, data):
        self._append_to_profit_history(data)
        self._historyfile.add_data(data)

    # 取引ヒストリーリストへ追加
    def _append_to_profit_history(self, data):
        try:
            dt, logic, id, size, price = data['time'], data['logic'], data['id'], float(data['size'])*self.exchange.units()['pos_rate']/self.collateral_rate, float(data['price'])

            if self._profit_history_first_dt == 0:
                self._profit_history_first_dt = dt

            if id and logic :
                # ノーポジからの新規ポジ構築
                if self._profit_history.get(logic) == None:
                    if self._plot_start_offset.get(logic) == None:
                        self._profit_history[logic] = [{'time': dt, 'pos': size, 'exec': size*price, 'ltp': price, 'pnl': 0}]
                    else:
                        self._profit_history[logic] = [{'time': dt, 'pos': size+self._plot_start_offset[logic],
                                                        'exec':(size+self._plot_start_offset[logic])*price, 'ltp':price, 'pnl':0}]
#                    print( "1:" + logic,self._profit_history[logic][-1] )

                # すでにポジ情報がある場合にはポジの増減
                else:
                    previous = self._profit_history[logic][-1]
                    new_size = previous['pos']+size
                    new_exec = previous['exec']+size*price
                    self._profit_history[logic].append({'time': dt, 'pos': new_size, 'exec': new_exec, 'ltp': price, 'pnl': (new_size*price)-new_exec})
#                    print( "2:" + logic,self._profit_history[logic][-1] )

            # 定期的に記録されるLTP情報によって含み損益を再計算
            else:
                for key, value in self._profit_history.items():
                    previous = value[-1]
                    value.append({'time': dt, 'pos': previous['pos'], 'exec': previous['exec'], 'ltp': price, 'pnl': (previous['pos']*price)-previous['exec']})
#                    print( "3:" + key,value[-1] )
            self._market_history.append({'time': dt, 'ltp': price})
        except Exception as err:
            self._logger.exception(err)


    # 現在のポジションを表示
    def _send_position_info(self):

        def _logic_lot_message(logic, pos):

            # yaml にロットが指定されているロジック
            if self.parameters['logic_lot'].get(logic) != None:
                self._logger.info("(Pos:{:>5.2f}/Price:{:>7.0f}):{:>20}  [Lot:{:.2f} Max:{:.2f}]{}".format(pos['size'], pos['price'], logic, self.parameters['logic_lot'][logic][0],
                           self.parameters['logic_lot'][logic][1], " ===== OVER" if abs(round(pos['size'], 8)) > self.parameters['logic_lot'][logic][1] else ""))
                return "(Pos:{:>5.2f}):{:>20}  [Lot:{:.2f}]{}\n".format(pos['size'], logic, self.parameters['logic_lot'][logic][0],
                       " ===== OVER" if abs(round(pos['size'], 8)) > self.parameters['logic_lot'][logic][1] else "")

            # yaml にロットが指定されていない野良ロジック
            else:
                self._logger.info("(Pos:{:>5.2f}/Price:{:>7.0f}):{:>20}".format(pos['size'], pos['price'], logic))
                return "(Pos:{:>5.2f}):{:>20}\n".format(pos['size'],logic)

        # 現在ポジを持っているロジック
        total = 0
        message = '\n' + '-'*20 + 'Current\n'
        self._logger.info('-'*20 + 'Current')
        for logic, pos in self._position_dict.items():
            if pos['size'] != 0:
                message += _logic_lot_message(logic, pos)
                total += pos['size']

            # Discordの送信上限2000文字を超えないように1500文字ごとに一旦送信
            if len(self._message+message) > 1500 :
                self.send_discord( self._message+message )
                self._message = ''
                message = ''

        message += '-'*20 + '\n'
        self._logger.info('-'*20)

        # 現在ポジを持っていないロジック
        for logic, pos in self._position_dict.items():
            if pos['size'] == 0:
                message += _logic_lot_message(logic, pos)

            # Discordの送信上限2000文字を超えないように1500文字ごとに一旦送信
            if len(self._message+message) > 1500 :
                self.send_discord( self._message+message )
                self._message = ''
                message = ''

        message += '-'*30
        self._logger.info('-'*30)
        message += '\nTotal {:.2f} , Current {:.2f} (Diff {:.2f})\n'.format(total, self.current_pos, self.current_pos-total)
        self._logger.info('Total {:.2f} , Current {:.2f} (Diff {:.2f})'.format(total, self.current_pos, self.current_pos-total))
        message += '-'*30
        self._logger.info('-'*30)
        self._message += message      # Discordへ送信


    # ロジックごとの損益グラフのプロット（手数料は含まれてない）
    def _plot_profit_graph(self, logic=None, top=-1, bottom=-1, start_datetime=datetime(1970, 4, 27), endt_datetime=datetime(2100, 12, 31)):
        def _epoc_to_dt(timestamp):
            return (datetime.utcfromtimestamp(timestamp) + timedelta(hours=9)).replace(tzinfo=timezone(timedelta(hours=9),'JST'))

        try:

            # プロット区間の指定
            starttime = start_datetime.replace(tzinfo=timezone(timedelta(hours=9),'JST')).timestamp()
            endtime = endt_datetime.replace(tzinfo=timezone(timedelta(hours=9),'JST')).timestamp()

            start = time.time()
            image_file = self.log_folder+'strategy_profit.png'
            fig = matplotlib.pyplot.figure(figsize=(12, 9))
            fig.autofmt_xdate()
            fig.tight_layout()
            gs = matplotlib.gridspec.GridSpec(nrows=2, ncols=1, height_ratios=[7, 3])
            ax = matplotlib.pyplot.subplot(gs[0])  # 0行0列目にプロット
            ax.set_title('Strategy Profit Graph', fontsize=15)

            plot_list = []
            total_profit_list = []
            for key, history in self._profit_history.items():

                # プロット区間だけを抜き出す
                value = [v for v in history if v['time']>= starttime and v['time'] <= endtime]

                if len(value) != 0:
                    pnl = [(v['pnl']-value[0]['pnl'])*(1 if self.collateral_rate == 1 else v['ltp']) for v in value]
                    timescale = [_epoc_to_dt(v['time']) for v in value]
                    total_profit_list += [{'time': v['time'], 'pnl':(v['pnl'] - value[0]['pnl'])*(1 if self.collateral_rate == 1 else v['ltp']), 'logic':key, 'ltp':v['ltp']} for v in value]
                    plot_list.append({'pnl': (value[-1]['pnl']-value[0]['pnl'])*(1 if self.collateral_rate == 1 else value[-1]['ltp']), 'logic': key, 'plotx': timescale, 'ploty': pnl})

            if top != -1:
                sorted_list = sorted(plot_list, key=lambda x: x['pnl'])[-top:]
            elif bottom != -1:
                sorted_list = sorted(plot_list, key=lambda x: x['pnl'])[:bottom]
            else:
                sorted_list = sorted(plot_list, key=lambda x: x['pnl'])

            plotted_item = []
            firsttime = time.time()
            for item in reversed(sorted_list):
                if logic == None or logic == item['logic']:
                    ax.plot(item['plotx'], item['ploty'], label="{:+7.2f} :{}".format(item['pnl'], item['logic']))
                    plotted_item.append(item['logic'])
                    if firsttime > item['plotx'][0].timestamp() :
                        firsttime = item['plotx'][0].timestamp()

            ax.axhline(y=0, color='k', linestyle='dashed')

            ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%m/%d\n%H:%M'))
            ax.grid(linestyle=':', which='both')
            ax.legend(loc='upper left', fontsize=(7 if len(sorted_list)<30 else (6 if len(sorted_list)<35 else 5)))
            bx2 = matplotlib.pyplot.subplot(gs[1])  # 1行0列目にプロット
            bx2.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.1f'))
            bx1 = bx2.twinx()
            price = [m['ltp'] for m in self._market_history if m['time'] >= firsttime and m['time'] <= endtime]
            bx1.plot([_epoc_to_dt(m['time']) for m in self._market_history if m['time'] >= firsttime and m['time'] <= endtime], price, color='red', label="LTP")
            bx1.grid(linestyle=':', which='both')
            if logic != None:
                # 個別ポジションのプロット
                bx2.fill_between([_epoc_to_dt(v['time']) for v in self._profit_history[logic] if v['time'] >= firsttime and v['time'] <= endtime], [
                                 v['pos'] for v in self._profit_history[logic] if v['time'] >= firsttime and v['time'] <= endtime], 0, color="blue", label="position", alpha=0.25)
                lim = max([abs(v['pos']) for v in self._profit_history[logic] if v['time'] >= firsttime and v['time'] <= endtime])*2
                bx2.set_ylim([-lim, lim])
                bx2.axhline(y=0, color='k', linestyle='dashed')

            else:
                # トータル損益のプロット
                profit_dict = {}
                total_plot = []
#                print( total_profit_list )
                for p in sorted(total_profit_list, key=lambda x: x['time']):
                    if p['logic'] in plotted_item:
                        profit_dict[p['logic']] = p['pnl']
                        if len(total_plot) == 0 or total_plot[-1]['time'] != p['time']:
                            total_plot.append({'time': p['time'], 'pnl': sum([d for d in profit_dict.values()])})
                        else:
                            total_plot[-1] = {'time': p['time'], 'pnl': sum([d for d in profit_dict.values()])}
                profit = [p['pnl'] for p in total_plot if p['time'] >= firsttime and p['time'] <= endtime]
                if total_plot :
                    bx2.plot([_epoc_to_dt(t['time']) for t in total_plot if t['time'] >= firsttime and t['time'] <= endtime], profit, color='blue', label="Total Pnl : {:.2f}".format(profit[-1]))

            bx1.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%m/%d\n%H:%M'))
            bx1.tick_params(axis = 'y', colors ='red')
            bx2.tick_params(axis = 'y', colors ='blue')

            h1, l1 = bx1.get_legend_handles_labels()
            h2, l2 = bx2.get_legend_handles_labels()
            bx1.legend(h1+h2, l1+l2, loc='upper left', fontsize=10)
            matplotlib.pyplot.savefig(image_file, bbox_inches='tight', pad_inches=0.1)
            matplotlib.pyplot.close()
            self._logger.info('Plot profit graph in {:.2f}secs'.format(time.time()-start))
            self.send_discord('Strategy Profit Graph', image_file)
        except Exception as err:
            self._message += traceback.format_exc(limit=0)
            self._logger.info(traceback.format_exc())
            self._logger.error(err)
