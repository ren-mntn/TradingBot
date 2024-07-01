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
import pandas
import platform
import psutil
import queue
import signal
from threading import Thread, Lock, Event
import time
import traceback

# 日次損益グラフをプロットするクラス
class ProfitGraph(object):
    def __init__(self, logger, stats, strategy, lock, terminate=True):
        self._logger = logger
        self._stats = stats
        self._strategy = strategy
        self._lock = lock
        self._terminate = terminate
        self._plotting = Lock()
        self._run_plot = Event()
        self._args = {'image_file':'', 'exchange':None, 'days':1, 'fmt':'%H:%M', 'rotate':0}
        prof_plot_thread = Thread(target=self._plot)
        prof_plot_thread.daemon = True
        prof_plot_thread.start()

    async def plot(self, image_file, exchange, days=1, fmt='%H:%M', rotate=0 ):
        self._discord_message = ''
        if self._logger.discord.webhook!='' :
            self._args = {'image_file':image_file, 'exchange':exchange, 'days':days, 'fmt':fmt, 'rotate':rotate}
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
                days = self._args['days']
                fmt = self._args['fmt']
                rotate = self._args['rotate']
                if not exchange : continue
                if image_file=='' : continue

                if days==1 :
                    if self._plotting.locked() :
                        continue
                    org_stats = self._stats.get_stats()
                else:
                    org_stats = self._stats.get_all_stats()
                if len(org_stats)==0 :
                    continue

                with self._plotting:
                    if days==1 :
                        # 日次モード
                        t=0
                        stats=[]
                        for s in org_stats:
                            if t+20 < s['timestamp'] and time.time()-s['timestamp']<86400 :
                                stats.append(s)
                                t=s['timestamp']
                        stats.append(org_stats[-1])
                    else:
                        # 長期モード
                        cumsum = 0
                        t=0
                        last_day = (datetime.utcfromtimestamp(org_stats[0]['timestamp'])+timedelta(hours=9)).day
                        last_profit = 0
                        stats=[]
                        last = org_stats[-1]['profit']
                        for s in org_stats:
                            if (t+600 <= s['timestamp'] or s.get('keep',False)) and time.time()-s['timestamp']<86400*days :
                                # 日付が変わったら変わる直前の最後の損益までを加算
                                day = (datetime.utcfromtimestamp(s['timestamp'])+timedelta(hours=9)).day
                                if last_day != day :
                                    cumsum += last_profit
                                last_day = day
                                last_profit = s['profit']
                                s['profit'] += cumsum
                                stats.append(s)
                                t=s['timestamp']
                        org_stats[-1]['profit'] = last+cumsum
                        stats.append(org_stats[-1])

                    price_history_raw = [(1 if exchange.units()['unitrate']==1 else float(s['ltp']))*(s['profit']-stats[0]['profit']) for s in stats]
                    price_history = list(pandas.Series(price_history_raw).rolling(window=20, min_periods=1).mean())
                    price_history[-1] = price_history_raw[-1]

                    self._lock.wait()
                    self._logger.info("[plot] Start plotting profit graph" )

                    history_timestamp = RawArray(c_double, len(stats))
                    np.asarray(history_timestamp)[:] = [float(s['timestamp']) for s in stats]
                    price_history_array = RawArray(c_double, len(price_history))
                    np.asarray(price_history_array)[:] = price_history

                    self._lock.wait()
                    with self._lock :
                        start = time.time()

                        event_queue = Queue()
                        pnlplot_proc = MulitiprocessingPnlPlot( event_queue,
                            history_timestamp = history_timestamp,
                            price_history = price_history_array,
                            )
                        sub_process = Process(target=pnlplot_proc._plot,
                            args=(image_file,
                                  rotate,
                                  fmt,
                                  exchange.units(round(price_history[-1],4))['title']
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

                        self._logger.info("[plot] Finish plotting profit graph in {:.2f}sec".format(time.time()-start) )

                    self._discord_message = '{} 損益通知 {} Profit:{:+.0f}'.format((datetime.utcnow()+timedelta(hours=9)).strftime('%H:%M:%S'), self._strategy, price_history[-1])

            except Exception:
                self._logger.error( traceback.format_exc() )


class MulitiprocessingPnlPlot():

    def __init__(self, event_queue, **argv ):
        self._event_queue = event_queue
        self._argv = argv

    def _plot(self, image_file, rotate, fmt, title):
        # Ctrl+Cのシグナルを無効にしておく。(メインからのstop()で終了させるので。)
        signal.signal(signal.SIGINT,  signal.SIG_IGN)

        try:
            self._event_queue.put( ('pid', os.getpid()) )

            history_timestamp = list(self._argv['history_timestamp'])
            price_history = list(self._argv['price_history'])

            fig = plt.figure(tight_layout=True)
            ax = fig.subplots(1,1)
            fig.autofmt_xdate()

            ax.set_facecolor('#fafafa')
            ax.spines['top'].set_visible(False)
            ax.spines['bottom'].set_visible(False)
            ax.spines['left'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.grid(which='major', linestyle='-', color='#101010', alpha=0.1, axis='y')
            if rotate == 0:
                ax.tick_params(width=0, length=0)
            else:
                ax.tick_params(width=1, length=5)
            ax.tick_params(axis='x', colors='#c0c0c0')
            ax.tick_params(axis='y', colors='#c0c0c0')
            if fmt != '':
                ax.xaxis.set_major_formatter(mdates.DateFormatter(fmt))
            ax.yaxis.set_major_formatter(ticker.ScalarFormatter())
            ax.yaxis.get_major_formatter().set_scientific(False)
            ax.yaxis.get_major_formatter().set_useOffset(False)

            green1 = '#10b285'
            green2 = '#9cdcd0'
            red1 = '#e25447'
            red2 = '#f0b8b8'

            if price_history[-1] >= 0:
                ax.set_title(title, color=green1, fontsize=28)
            else:
                ax.set_title(title, color=red1, fontsize=28)

            last = 0
            plus_times = []
            plus_price = []
            minus_times = []
            minus_price = []
            for i in range(0, len(history_timestamp)):
                if last * price_history[i] >= 0:
                    if price_history[i] >= 0:
                        plus_times.append(datetime.fromtimestamp(history_timestamp[i]))
                        plus_price.append(price_history[i])
                    if price_history[i] <= 0:
                        minus_times.append(datetime.fromtimestamp(history_timestamp[i]))
                        minus_price.append(price_history[i])
                else:
                    cross_point = price_history[i-1]/(price_history[i-1]-price_history[i])*(history_timestamp[i]-history_timestamp[i-1])+history_timestamp[i-1]
                    if price_history[i] < 0:
                        plus_times.append(datetime.fromtimestamp(cross_point))
                        plus_price.append(0)
                        ax.plot(plus_times, plus_price, color=green1, linewidth=0.8)
                        ax.fill_between(plus_times, plus_price, 0, color=green2, alpha=0.25)
                        plus_times = []
                        plus_price = []
                        minus_times = []
                        minus_price = []
                        minus_times.append(datetime.fromtimestamp(cross_point))
                        minus_price.append(0)
                        minus_times.append(datetime.fromtimestamp(history_timestamp[i]))
                        minus_price.append(price_history[i])
                    else:
                        minus_times.append(datetime.fromtimestamp(cross_point))
                        minus_price.append(0)
                        ax.plot(minus_times, minus_price, color=red1, linewidth=0.8)
                        ax.fill_between(minus_times, minus_price, 0, color=red2, alpha=0.25)
                        plus_times = []
                        plus_price = []
                        minus_times = []
                        minus_price = []
                        plus_times.append(datetime.fromtimestamp(cross_point))
                        plus_price.append(0)
                        plus_times.append(datetime.fromtimestamp(history_timestamp[i]))
                        plus_price.append(price_history[i])
                last = price_history[i]

            if len(plus_times) > 0:
                ax.plot(plus_times, plus_price, color=green1, linewidth=0.8)
                ax.fill_between(plus_times, plus_price, 0, color=green2, alpha=0.25)

            if len(minus_times) > 0:
                ax.plot(minus_times, minus_price, color=red1, linewidth=0.8)
                ax.fill_between(minus_times, minus_price, 0, color=red2, alpha=0.25)

            labels = ax.get_xticklabels()
            plt.setp(labels, rotation=rotate)

            plt.savefig(image_file, facecolor='#fafafa')
            plt.close()

        except Exception:
            self._event_queue.put(('logger.error',traceback.format_exc()))
            self._logger.error( traceback.format_exc() )

        self._event_queue.put(('done',))
