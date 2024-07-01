# coding: utf-8
#!/usr/bin/python3

import asyncio
from ctypes import c_uint, c_float, c_double
from datetime import datetime, timedelta
import matplotlib
matplotlib.use('Agg')
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from multiprocessing import Process, Queue
from multiprocessing.sharedctypes import RawArray
import numpy as np
import os
import platform
import psutil
import queue
import signal
from threading import Thread, Lock, Event
import time
import traceback

# ポジショングラフをプロットするグラフ
class PositionGraph(object):
    def __init__(self, logger, stats, strategy, lock, setting={}, terminate=True):
        self._logger = logger
        self._stats = stats
        self._strategy = strategy
        self._lock = lock
        self.setting = setting
        self._terminate = terminate
        self._plotting = Lock()
        self._run_plot = Event()
        self._args = {'image_file':'', 'exchange':None}
        pos_plot_thread = Thread(target=self._plot)
        pos_plot_thread.daemon = True
        pos_plot_thread.start()

    async def plot(self, image_file, exchange ):
        if self._plotting.locked() :
            return

        self._discord_message = ''
        if self._logger.discord.webhook!='' :
            self._args = {'image_file':image_file, 'exchange':exchange}
            self._run_plot.set()

            while not self._discord_message:
                await asyncio.sleep(0.5)
            self._logger.discord.send( self._discord_message, image_file )


    def _plot(self):
        while self._logger.running:
            try:
                self._run_plot.wait()
                self._run_plot.clear()
                image_file = self._args['image_file']
                exchange = self._args['exchange']
                if not exchange : continue
                if image_file=='' : continue

                with self._plotting:
                    org_stats = self._stats.get_stats()
                    if len(org_stats)==0 :
                        continue

                    t=0
                    stats=[]
                    l=0
                    p=0
                    for s in org_stats:
                        p=round(float(s['ltp']),8) if round(float(s['ltp']),8)!=0 else p
                        if p == 0 :
                            continue
                        s['ltp']=p
                        if s['timestamp']-t>20 and time.time()-s['timestamp']<self.setting.get('period',60)*60 :
                            s['lantency']=max(l,s['lantency'])
                            stats.append(s)
                            t=s['timestamp']
                            l=0
                        else:
                            l=max(l,s['lantency'])
                    stats.append(org_stats[-1])

                    self._lock.wait()
                    self._logger.info("[plot] Start plotting position graph" )

                    # ---------------------------- 共有データの作成
                    timestamp = RawArray(c_double, len(stats))
                    np.asarray(timestamp)[:] = [float(s['timestamp']) for s in stats]
                    ltp = RawArray(c_float, len(stats))
                    np.asarray(ltp)[:] = [round(float(s['ltp']),8) for s in stats]
                    average = RawArray(c_float, len(stats))
                    np.asarray(average)[:] = [(round(s['average'],8) if s['average']!=0 else float(s['ltp'])) for s in stats]
                    current_pos = RawArray(c_float, len(stats))
                    np.asarray(current_pos)[:] = [s['current_pos'] for s in stats]
                    leverage = RawArray(c_float, len(stats))
                    np.asarray(leverage)[:] = [abs(s['current_pos']) for s in stats]
                    profit = RawArray(c_float, len(stats))
                    if self.setting.get('plot_fixed_pnl',False) :
                        np.asarray(profit)[:] = [(1 if exchange.units()['unitrate']==1 else float(s['ltp']))*s['fixed_profit'] for s in stats]
                    else:
                        np.asarray(profit)[:] = [(1 if exchange.units()['unitrate']==1 else float(s['ltp']))*s['profit'] for s in stats]
                    commission = RawArray(c_float, len(stats))
                    np.asarray(commission)[:] = [(1 if exchange.units()['unitrate']==1 else float(s['ltp']))*s['commission'] for s in stats]
                    lantency_np = np.array([s['lantency'] for s in org_stats])
                    lantency_std = lantency_np.std()
                    lantency_mean = lantency_np.mean()
                    lantency_np = np.array([s['lantency'] for s in stats])
                    normal = RawArray(c_uint, len(stats))
                    very_busy = RawArray(c_uint, len(stats))
                    super_busy = RawArray(c_uint, len(stats))
                    np.asarray(normal)[:] = [0 if s['lantency'] < lantency_mean-lantency_std*0.3 else 100000000 for s in stats]
                    np.asarray(very_busy)[:] = [0 if s['lantency'] > lantency_mean+lantency_std*3 else 100000000 for s in stats]
                    np.asarray(super_busy)[:] = [0 if s['lantency'] > lantency_mean+lantency_std*10 else 100000000 for s in stats]
                    api1 = RawArray(c_uint, len(stats))
                    api2 = RawArray(c_uint, len(stats))
                    api3 = RawArray(c_uint, len(stats))
                    np.asarray(api1)[:] = [s['api1'] for s in stats]
                    np.asarray(api2)[:] = [s.get('api2',0) for s in stats]
                    np.asarray(api3)[:] = [s.get('api3',0) for s in stats]

                    self._lock.wait()
                    with self._lock :
                        start = time.time()

                        event_queue = Queue()
                        posplot_proc = MulitiprocessingPosPlot( event_queue,
                            timestamp = timestamp,
                            ltp = ltp,
                            average = average,
                            normal = normal,
                            very_busy = very_busy,
                            super_busy = super_busy,
                            api1 = api1,
                            api2 = api2,
                            api3 = api3,
                            profit = profit,
                            commission = commission,
                            leverage = leverage,
                            current_pos = current_pos,
                            )
                        sub_process = Process(target=posplot_proc._plot,
                            args=(image_file,
                                  self.setting.get('plot_fixed_pnl',False),
                                  self.setting.get('plot_commission',False)
                                  ))
                        sub_process.daemon = True
                        sub_process.start()

                        while True:
                            try:
                                event = event_queue.get(timeout=180)
                            except Queue.Empty:
                                break
                            if event[0] == 'done' :
                                break
                            elif event[0] == 'pid' :
                                proc = psutil.Process( int(event[1]) )
                                self._logger.info( "[plot] procid = {}".format(int(event[1])) )
                                if platform.system() == 'Windows':
                                    proc.nice( psutil.IDLE_PRIORITY_CLASS )
                                else:
                                    proc.nice(19)
                            elif event[0] == 'logger.info' :
                                self._logger.info(event[1])
                            elif event[0] == 'logger.error' :
                                self._logger.error(event[1])

                        if self._terminate :
                            sub_process.terminate()
                            sub_process.join(10)

                        self._logger.info("[plot] Finish plotting position graph in {:.2f}sec".format(time.time()-start) )

                    self._discord_message = '{} ポジション通知 {}'.format((datetime.utcnow()+timedelta(hours=9)).strftime('%H:%M:%S'), self._strategy)

            except Exception:
                self._logger.error( traceback.format_exc() )



class MulitiprocessingPosPlot():

    def __init__(self, event_queue, **argv ):
        self._event_queue = event_queue
        self._argv = argv

    def _plot(self, image_file, plot_fixed_pnl, plot_commission):
        # Ctrl+Cのシグナルを無効にしておく。(メインからのstop()で終了させるので。)
        signal.signal(signal.SIGINT,  signal.SIG_IGN)

        try:
            self._event_queue.put( ('pid', os.getpid()) )

            timestamp = [datetime.fromtimestamp(t) for t in self._argv['timestamp']]
            ltp = list(self._argv['ltp'])
            average = list(self._argv['average'])
            normal = list(self._argv['normal'])
            very_busy = list(self._argv['very_busy'])
            super_busy = list(self._argv['super_busy'])
            api1 = list(self._argv['api1'])
            api2 = list(self._argv['api2'])
            api3 = list(self._argv['api3'])
            profit = list(self._argv['profit'])
            commission = list(self._argv['commission'])
            leverage = list(self._argv['leverage'])
            current_pos = list(self._argv['current_pos'])

            fig = plt.figure()
            fig.autofmt_xdate()
            fig.tight_layout()

            # サブエリアの大きさの比率を変える
            gs = matplotlib.gridspec.GridSpec(nrows=2, ncols=1, height_ratios=[7, 3])
            ax1 = fig.add_subplot(gs[0])  # 0行0列目にプロット
            ax2 = ax1.twinx()
            ax2.tick_params(labelright=False)
            bx1 = fig.add_subplot(gs[1])  # 1行0列目にプロット
            bx1.tick_params(labelbottom=False)
            bx2 = bx1.twinx()

            # 上側のグラフ
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax1.set_ylim([min(ltp + average)*0.999 - (max(ltp + average) - min(ltp + average))/5, max(ltp + average)*1.001 + (max(ltp + average) - min(ltp + average))/10])

            ax1.plot(timestamp, ltp, label="market price")
            ax1.plot(timestamp, average, label="position average")

            ax1.fill_between(timestamp, normal, 100000000, color='green', alpha=0.01)
            ax1.fill_between(timestamp, very_busy, 100000000, color='red', alpha=0.1)
            ax1.fill_between(timestamp, super_busy, 100000000, color='red', alpha=0.5)

            if max(api1)!=min(api1) :
                ax2.plot(timestamp, api1, label="API1", color='red')
            if max(api2)!=min(api2) :
                ax2.plot(timestamp, api2, label="API2", color='orange')
            if max(api3)!=min(api3) :
                ax2.plot(timestamp, api3, label="API3", color='brown')
            ax2.axhline(y=0, color='k', linestyle='dashed')

            ax2.set_ylim([-100, 2800])
            ax2.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(500))

            # 損益の推移
            bx1.yaxis.set_major_formatter(ticker.ScalarFormatter())
            bx1.yaxis.get_major_formatter().set_scientific(False)
            bx1.yaxis.get_major_formatter().set_useOffset(False)

            if plot_fixed_pnl :
                bx1.plot(timestamp, profit, label="fixed profit", color='red')
            else:
                bx1.plot(timestamp, profit, label="profit", color='red')
            if max(commission)!=min(commission) and plot_commission :
                bx1.plot(timestamp, commission, label="commission", color='blue')
                bx1.set_ylim([min(profit+commission)*0.99, max(profit+commission)*1.01])
            else:
                if min(profit)!=max(profit) :
                    bx1.set_ylim([min(profit)*0.99, max(profit)*1.01])

            bx1.yaxis.get_major_formatter().set_useOffset(False)
            bx1.yaxis.set_major_locator(ticker.MaxNLocator(nbins=5, integer=True))

            # ポジション推移
            if max(leverage)!=0 :
                bx2.set_ylim([-max(leverage) * 1.2, max(leverage) * 1.2])
            bx2.fill_between(timestamp, current_pos, label="position")
            bx2.axhline(y=0, color='k', linestyle='dashed')

            bx2.yaxis.set_minor_locator(ticker.MaxNLocator(nbins=5))

            bx1.patch.set_alpha(0)
            bx1.set_zorder(2)
            bx2.set_zorder(1)

            # 凡例
            h1, l1 = ax1.get_legend_handles_labels()
            h2, l2 = ax2.get_legend_handles_labels()
            h3, l3 = bx1.get_legend_handles_labels()
            h4, l4 = bx2.get_legend_handles_labels()
            ax1.legend(h1, l1, loc='upper left', prop={'size': 8})
            ax2.legend(h2, l2, loc='lower left', prop={'size': 8})
            bx1.legend(h3+h4, l3+l4, loc='upper left', prop={'size': 8})

            ax1.grid(linestyle=':')
            bx1.grid(linestyle=':')

            bx1.tick_params(axis = 'y', colors ='red')
            bx2.tick_params(axis = 'y', colors ='blue')

            plt.savefig(image_file)
            plt.close()

        except Exception:
            self._event_queue.put(('logger.error',traceback.format_exc()))
            self._logger.error( traceback.format_exc() )

        self._event_queue.put(('done',))
