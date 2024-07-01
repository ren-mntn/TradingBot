# coding: utf-8
#!/usr/bin/python3

import asyncio
from collections import deque
from itertools import groupby
from sortedcontainers import SortedDict
import time
from threading import Event,Thread
import traceback
from libs.utils import LockCounter

# 板情報を管理するクラス
class BoardInfo(object):
    def __init__(self, logger):
        self._logger = logger
        self._clear()
        self._event_list = []
        self.event = Event() 
        self._event_time_lag = deque(maxlen=100)
        self._lock = LockCounter(self._logger, self.__class__.__name__)

        event_drive = Thread(target=self._event_drive, args=())
        event_drive.daemon = True
        event_drive.start()

    def _event_drive(self):
        while self._logger.running:
            self.event.wait()
            self.event.clear()
            self._event_time_lag.append((time.time()-self._logger.ws_timestamp)*1000)

            for handler, args in self._event_list:
                try:
                   if self._logger.running :
                        future = asyncio.run_coroutine_threadsafe(handler(*args), self._logger.event_loop)
                        result = future.result()

                except Exception as e:
                    self._logger.error( e )
                    self._logger.info(traceback.format_exc())

    @property
    def event_time_lag(self):
        return self._mean(self._event_time_lag)

    def _mean(self,q) : return sum(q) / (len(q) + 1e-7)

    def initialize_dict(self):
        self._lock.wait()
        self._clear()

    def _clear(self):
        self._asks = SortedDict()
        self._bids = SortedDict()

    # ---------------------------------------Type A
    # bitFlyer
    def update_bids(self, d):
        self._lock.wait()
        self._update(self._bids, d, -1)

    def update_asks(self, d):
        self._lock.wait()
        self._update(self._asks, d, 1)

    def _update(self, sd, d, sign):
        for i in d:
            if type(i)==dict :
                p, s = float(i['price']), float(i['size']) # binance/GMO
            else:
                p, s = float(i[0]), float(i[1])          # bitflyer/FTX/Coinbase/Kraken/Phemex
            if s == 0:
                s = sd.pop(p * sign, [0,0])[1]
            else:
                sd[p * sign] = [p, s]

    # ---------------------------------------Type B
    # bitmex / Btcmex / bybit(sign=-1)
    def insert(self, data, sign=1):
        self._lock.wait()
        for d in data:
            sd, key = self._sd_and_key(d)
            price, size = float(d['price']), d['size']
            sd[key*sign] = [float(price), float(size)/float(price)]

    def change(self, data, sign=1):
        self._lock.wait()
        for d in data:
            sd, key = self._sd_and_key(d)
            e = sd.get(key*sign,[0,0])
            e[1] = float(d['size']) / e[0] if e[0]!=0 else float(d['price'])

    def delete(self, data, sign=1):
        self._lock.wait()
        for d in data:
            sd, key = self._sd_and_key(d)
            sd.pop(key*sign, None)

    # ---------------------------------------Type C
    # bybit linear(sign=-1)
    def insert2(self, data, sign=1):
        self._lock.wait()
        for d in data:
            sd, key = self._sd_and_key(d)
            price, size = float(d['price']), d['size']
            sd[key*sign] = [float(price), float(size)]

    def change2(self, data, sign=1):
        self._lock.wait()
        for d in data:
            sd, key = self._sd_and_key(d)
            e = sd.get(key*sign,[0,0])
            e[1] = float(d['size'])

    def delete2(self, data, sign=1):
        self._lock.wait()
        for d in data:
            sd, key = self._sd_and_key(d)
            sd.pop(key*sign, None)

    def _sd_and_key(self, data):
        if data['side'] == 'Buy':
            return self._bids, int(data['id'])
        else:
            return self._asks, -int(data['id'])


    @property
    def bids(self): return self._bids.values() # bidsは買い板

    @property
    def asks(self): return self._asks.values() # askは売り板

    @property
    def best_bid(self):
        with self._lock:
            return float(self.bids[0][0]) if len(self._bids)!=0 else float(0)

    @property
    def best_ask(self):
        with self._lock:
            return float(self.asks[0][0]) if len(self._asks)!=0 else float(0)

    @property
    def mid(self): return (self.best_bid + self.best_ask)/2

    def add_handler( self, handler, args=() ):
        self._event_list.append( (handler, args) )

    # splitsizeごとに板を分割して値段リストを算出
    def get_size_group(self, splitsize, limitprice=1000000, limitnum=5, startprice=0):

        try:
            with self._lock:
                total = 0
                asks_pos = []
                for price, size in self.asks:
                    if price > startprice or startprice == 0:
                        if startprice == 0:
                            startprice = price
                        total += size
                        if total > splitsize :
                            asks_pos.append( price )
                            if len(asks_pos)>=limitnum :
                                break
                            total -= splitsize
                        if price > startprice+limitprice:
                            break
                total = 0
                bids_pos = []
                for price, size in self.bids:
                    if price < startprice or startprice == 0:
                        if startprice == 0:
                            startprice = price
                        total += size
                        if total > splitsize :
                            bids_pos.append( price )
                            if len(bids_pos)>=limitnum :
                                break
                            total -= splitsize
                        if price < startprice-limitprice:
                            break
        except :
            return {'ask': [self.mid]*limitnum, 'bid': [self.mid]*limitnum}

        if len(bids_pos)==0 : bids_pos = [self.mid]*limitnum
        if len(asks_pos)==0 : asks_pos = [self.mid]*limitnum

        return {'ask': asks_pos, 'bid': bids_pos}

    # splitsizeごとに板を分割して値段リストを算出
    def get_price_group(self, splitprice):

        mid = self.mid
        with self._lock:
            asks_group = [sum([i['size'] for i in items]) for index, items in groupby([{'group':int((b[0]-mid)/splitprice), 'size':b[1]} for b in self.asks], key=lambda x: x['group'])]
            bids_group = [sum([i['size'] for i in items]) for index, items in groupby([{'group':int((b[0]-mid)/splitprice), 'size':b[1]} for b in self.bids], key=lambda x: x['group'])]
        if len(asks_group)==0 : asks_group=[0]
        if len(bids_group)==0 : bids_group=[0]

        return {'ask': asks_group, 'bid': bids_group}

