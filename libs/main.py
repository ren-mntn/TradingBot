# coding: utf-8
#!/usr/bin/python3

import asyncio
import importlib.machinery as imm
import os
import signal
from threading import Thread
import time
import traceback
from libs.utils import *
from libs.plot import *
import libs.version

class Trade(object):

    async def start(self, args):

        try:
            # キーボード割込みのハンドリング
            self.keyboard_interrupt = asyncio.Event()
            self._stop_event = asyncio.Event()

            self._logger = MyLogger(version_str=libs.version.version_str)
            self._logger.event_loop = asyncio.get_running_loop()

            # パラメータの読み込み
            self.trade_yaml = DynamicParams(self._logger, args.get('trade_yaml','trade.yaml'), callback=self._update_params )
            self.strategy_yaml = DynamicParams(self._logger, args.get('strategy_yaml', self.trade_yaml.params['strategy_yaml']), callback=self._update_params )
            self.log_folder = self.strategy_yaml.params['logging']['folder']
            self._update_params()      # 初回の読み込み
            self.trade_yaml.start()    # 自動更新スタート
            self.strategy_yaml.start() # 自動更新スタート

            # 取引所クラスの作成
            apikey = [self.trade_yaml.params.get('trade',{}).get('apikey',''),
                      self.trade_yaml.params.get('trade',{}).get('secret',''),
                      self.trade_yaml.params.get('trade',{}).get('passphase','')]
            exchange_name = self.trade_yaml.params.get('trade',{}).get('exchange')
            symbol = self.trade_yaml.params.get('trade',{}).get('symbol')
            if   exchange_name.lower() == 'bitflyer' :
                from libs.exchanges.bitflyer import Bitflyer as bfsx3_exchange

            elif exchange_name.lower() == 'gmo' :
                from libs.exchanges.gmo import GMOCoin as bfsx3_exchange

            elif exchange_name.lower() == 'phemex' :
                from libs.exchanges.phemex import Phemex as bfsx3_exchange

            elif exchange_name.lower() == 'bitget' :
                from libs.exchanges.bitget import Bitget as bfsx3_exchange

            elif exchange_name.lower() == 'bybit' and  symbol[-3:]=="USD" :
                from libs.exchanges.bybit_inverse import BybitInverse as bfsx3_exchange

            elif exchange_name.lower() == 'bybit' :
                from libs.exchanges.bybit_linear import BybitUSDT as bfsx3_exchange

            else:
                self._logger.error( "Not Supported yet" )
                return
            self.exchange = await bfsx3_exchange(logger=self._logger, candle_store=CandleCollector(self._logger), apikey=apikey,
                                                 testnet=self.trade_yaml.params.get('trade',{}).get('testnet',False)).start(subscribe=self.strategy_yaml.params.get('subscribe',{}), symbol=symbol)


            # グラフプロットのための定期的ステータス収集クラス
            self.stats = Stats(self._logger, exchange=self.exchange )

            # 前回のポジションと損益を再構築
            self.exchange.my.position.renew_posfile(self.log_folder + 'position_' + exchange_name + '.json')
            self.exchange.my.reload_profitfile(self.log_folder + 'profit_' + exchange_name + '.json')
            self.stats.reload_statsfile(self.log_folder + 'stats_' + exchange_name + '.json')

            # 動的に MyStrategy を読み込んでクラスを上書きする
            strategy_py_file = args.get('strategy_py', self.trade_yaml.params['strategy_py'])
            strategy_yaml_file = args.get('strategy_yaml', self.trade_yaml.params['strategy_yaml'])
            module_name = strategy_py_file.split('.')[0].replace('/', '.')
            module = imm.SourceFileLoader(module_name, strategy_py_file).load_module()
            self._logger.info( "Load MyStrategy class dynamically: module={}".format(module) )
            strategy_class = getattr(module, 'MyStrategy')
            self._strategy_class = strategy_class( logger=self._logger, exchange=self.exchange, symbol=symbol, strategy_yaml=strategy_yaml_file)
            self._strategy_class.set_parameters( trade_param=self.trade_yaml.params, strategy_param=self.strategy_yaml.params)
            self._logger.info('Succeeded setup strategy. logic={}, yaml={}'.format(strategy_py_file,strategy_yaml_file))

            # メインループ起動
            main_loop = asyncio.create_task( self.main_loop() )
            asyncio.create_task( self.wait_keyboard_interrupt() )

        except Exception as e:
            self._logger.error( e )
            self._logger.info(traceback.format_exc())

            # wsプロセスの停止
            await self._logger.stop()
            asyncio.get_running_loop().stop()
            os._exit(0)

        try :
            matplot_lock = LockCounter(self._logger, "matplot_lock")
            # ポジショングラフのプロットクラス
            self.posgraph = PositionGraph(self._logger, stats=self.stats, strategy=strategy_yaml_file, lock=matplot_lock,
                                      setting = self.strategy_yaml.params.get('plot',{}).get('setting',{}),
                                      terminate=self.strategy_yaml.params.get('discord_bot_token','')=='' )
            # 損益グラフのプロットクラス
            self.profgraph = ProfitGraph(self._logger, stats=self.stats, strategy=strategy_yaml_file, lock=matplot_lock,
                                        terminate=self.strategy_yaml.params.get('discord_bot_token','')=='')

            # 1時間ごとに現在のパラメータ表示
            Scheduler(self._logger, interval=3600, basetime=1, callback=self._disp_params)

            # ノートレード期間のチェッククラス
            self.notradechecker = NoTradeCheck(self._logger, self.exchange)

            # 最初のLTPはTicker(API)を使用して取得
            while True:
                data = await self.exchange.ticker_api()
                if data.get('stat',-1)==0 :
                    self.exchange.execution_info.last = float(data['ltp'])
                    break
                await asyncio.sleep(1)

            self._update_params()       # パラメータファイルの再適用

            # ロジックの初期化
            await self._strategy_class.initialize()

            # 一定時間ごとのプロット
            self.posplot_timer = Scheduler(self._logger, interval=self.strategy_yaml.params.get('plot',{}).get('pos_interval',60)*60,
                                        callback=self.posgraph.plot, basetime=0,
                                        args=(self.log_folder + 'position_' + exchange_name + '.png', self.exchange,))

            # 一時間ごとのプロット
            self.pnlplot_timer = Scheduler(self._logger, interval=self.strategy_yaml.params.get('plot',{}).get('pnl_interval',60)*60,
                                        callback=self.profgraph.plot, basetime=0,
                                        args=(self.log_folder + 'profit_' + exchange_name + '.png', self.exchange,))

            # 長期損益プロット
            plot_term = self.trade_yaml.params.get('plot_term',90)
            asyncio.create_task( self.profgraph.plot(self.log_folder + 'profit_all_' + exchange_name + '.png',
                                 self.exchange, days=plot_term, fmt='%m/%d %H:%M', rotate=45) )
            self.long_pnlplot_timer = Scheduler(self._logger, interval=86400, callback=self.profgraph.plot, basetime=10,
                                        args=(self.log_folder + 'profit_all_' + exchange_name + '.png', self.exchange,
                                              plot_term, '%m/%d %H:%M', 45,))

            # ポジションサーバーとの接続
            self.posclient = PositionClient(self._logger, exchange=self.exchange, strategy=strategy_yaml_file,
                                            pos_server=self.trade_yaml.params.get('pos_server',None))

            # ポジずれ補正
            if self.trade_yaml.params.get('adjust_position_with_api',False) and self.exchange.auth:
                self._diff_count = 0
                self._last_pos_diff = 0
                self._logger.call_every1sec.append({'name':'check_position', 'handler':self.check_position, 'interval':30, 'counter':0})

        except Exception as e:
            self._logger.error( e )
            self._logger.info(traceback.format_exc())

        # メインループ終了まで待機
        await main_loop


    async def main_loop(self):
        # メインループ
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=1)
            except asyncio.TimeoutError:
                pass

        # 発注中のオーダーをキャンセル
        ids = [o['id'] for o in self.exchange.my.order.list]
        self._logger.info( "candel orders : {}".format(ids) )
        await asyncio.gather(*[self.exchange.cancelorder( id ) for id in ids])

        timeout = 60
        while len(self.exchange.my.order.list)!=0 and timeout>0:
            await asyncio.sleep(1)
            timeout -= 1

        # wsプロセスの停止
        await self._logger.stop()

        for i in range(5):
            self._logger.info( "Wait {}secs...  Current Pos : {}".format(5-i, round(self.exchange.my.position.size,8)) )
            await asyncio.sleep(1)

        self._logger.info( "Stopped" )
        asyncio.get_running_loop().stop()
        self.keyboard_interrupt.clear()


    # 定期的に APIから建玉一覧を取得してポジずれをチェック
    async def check_position(self):

        pos = await self.exchange.getpositions()
        long = 0
        short = 0
        for i in pos:
            self._logger.debug( "OpenPosition : {}".format(i) )
            if i['side'].upper()=='BUY' :
                long += float(i['size'])
            else :
                short += float(i['size'])
        current = self.exchange.my.position.size

        # 同じずれが繰り返すとカウントアップ
        actual = round(long-short,8)
        pos_diff = round(actual-current-self.trade_yaml.params.get('base_position',0), 8)
        if self._last_pos_diff != pos_diff or abs(pos_diff)<self.exchange.minimum_order_size() :
            self._diff_count = 0
        if abs(pos_diff)>=self.exchange.minimum_order_size() :
            self._diff_count += 1
        self._last_pos_diff = pos_diff

        self._logger.info( "Long : {} - Short : {} - base :{} = Total :{} / est:{}.   Diff:{} {}".format(
            long, short, self.trade_yaml.params.get('base_position',0),
            round(long-short-self.trade_yaml.params.get('base_position',0),8),
            round(current,8), round(long-short-self.trade_yaml.params.get('base_position',0)-current,8),
            '*'*self._diff_count) )

        # 4度続けてポジションがズレていれば成売買で補正行う
        if self.exchange.auth!=None and self.trade_yaml.params.get('adjust_position_with_api',True) and self._diff_count>=4 :
            maxsize = self.trade_yaml.params.get('adjust_max_size',100)
            if pos_diff < 0:
                size = min(-pos_diff,maxsize)
                if size >= self.exchange.minimum_order_size():
                    self._logger.info( "Adjust position with 'MARKET BUY' : size:{}".format(size) )
                    await self.exchange.sendorder(order_type='MARKET', side='BUY', size=size, adjust_flag=True)
                    self.exchange.PendingUntil = max(self.exchange.PendingUntil, time.time()+30)
            else:
                size = min(pos_diff,maxsize)
                if size >= self.exchange.minimum_order_size():
                    self._logger.info( "Adjust position with 'MARKET SELL' : size:{}".format(size) )
                    await self.exchange.sendorder(order_type='MARKET', side='SELL', size=size, adjust_flag=True)
                    self.exchange.PendingUntil = max(self.exchange.PendingUntil, time.time()+30)
            self._diff_count = 0

    # 動的に適用するパラメータの更新
    def _update_params(self):
        self._logger.set_param_fh(log_folder=self.log_folder, console_output=self.trade_yaml.params['console_output'],
                                  console_log_level=self.trade_yaml.params.get('console_log_level','INFO'),
                                  file_log_level=self.strategy_yaml.params['logging'].get('level','INFO'))

        self._logger.discord.webhook = self.strategy_yaml.params.get('discord',{}).get('webhook','')

        if hasattr(self,'posgraph') :
            self.posgraph.setting = self.strategy_yaml.params.get('plot',{}).get('setting',{})

        if hasattr(self,'posplot_timer') :
            self.posplot_timer.interval = self.strategy_yaml.params.get('plot',{}).get('pos_interval',self.posplot_timer.interval/60)*60

        if hasattr(self,'pnlplot_timer') :
            self.pnlplot_timer.interval = self.strategy_yaml.params.get('plot',{}).get('pnl_interval',self.pnlplot_timer.interval/60)*60

        if hasattr(self,'_strategy_class') :
            self._strategy_class.set_parameters( trade_param=self.trade_yaml.params, strategy_param=self.strategy_yaml.params)

        if hasattr(self,'exchange') :
            self.exchange.my.position.base_position = self.trade_yaml.params.get('base_position',0.0)
            self.exchange.close_while_noTrade = self.strategy_yaml.params.get('close_while_noTrade',False)

        if hasattr(self,'notradechecker') :
            self.notradechecker.notrade = self.strategy_yaml.params.get('no_trade')


    async def _disp_params(self):

        # 1時間ごとにオーダーカウンターを表示
        if self.exchange.auth!=None:
            self.exchange.my.disp_stats()

        # strategy.yaml の 'parameters' 項目だけをリスト表示
        self.strategy_yaml.params['parameters']={}
        self.strategy_yaml.load_param()

#        self._logger.info( "Websocket time lag:{:.3f}ms".format(self.exchange.ws.time_lag*1000) )

    # Ctrl+Cが押されたときに行う処理
    async def wait_keyboard_interrupt(self):

        # キーボード待ち
        while not self.keyboard_interrupt.is_set():
            try:
                await asyncio.wait_for(self.keyboard_interrupt.wait(), timeout=1)
            except asyncio.TimeoutError:
                pass

        # タスクの一覧をログへ記録
        current_tasks =[task for task in asyncio.all_tasks() if not task.done()]
        for task in current_tasks:
            self._logger.debug( "{:<30} : {}".format(task.get_name(), task.get_stack()) )

        self._logger.info( "Ctrl+C" )
        self._logger.running = False
        self._stop_event.set()
