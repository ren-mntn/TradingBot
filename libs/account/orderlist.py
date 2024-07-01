# coding: utf-8
#!/usr/bin/python3

import asyncio
import time
from copy import deepcopy
from collections import deque
from threading import Thread, Event, Lock
import traceback

# 注文リスト管理クラス
class OrderList(object):

    def __init__(self, logger):
        self._logger = logger
        self.order_dict = {}
        self._my_id = deque(maxlen=5000)
        self._executed_list = deque(maxlen=1000)
        self._canceled_list = deque(maxlen=1000)
        self.reset_counter()
        self._historical_counter={'ordered':deque([0],maxlen=86400), 'filled':deque([0],maxlen=86400),
                                  'partially_filled':deque([0],maxlen=86400), 'canceled':deque([0],maxlen=86400),
                                  'size':deque([0],maxlen=86400)}

        self._event_list = []
        self.event = Event() 
        event_drive = Thread(target=self._event_drive, args=())
        event_drive.daemon = True
        event_drive.start()
                
        # 1秒ごとに実行
        self._logger.call_every1sec.append({'name':'order_counter_update', 'handler':self._exec_every_sec, 'interval':1, 'counter':0})

    async def _exec_every_sec(self):
        self._shift_historical_counter()  # historical_counterのシフト
        self._delete_invalidate_order()   # オーダーリストのゴミを掃除 

    def _event_drive(self):
        while self._logger.running:
            self.event.wait()
            self.event.clear()

            for handler, args in self._event_list:
                try:
                   if self._logger.running :
                        future = asyncio.run_coroutine_threadsafe(handler(*args), self._logger.event_loop)
                        result = future.result()

                except Exception as e:
                    self._logger.error( e )
                    self._logger.info(traceback.format_exc())

    def add_handler( self, handler, args=() ):
        self._event_list.append( (handler, args) )

    def __len__(self):
        return len(self.order_dict)

    @property
    def ordered_count(self):
        return self._counter.get('ordered',0)

    @property
    def filled_count(self):
        return self._counter.get('filled',0)

    @property
    def partially_filled_count(self):
        return self._counter.get('partially_filled',0)

    @property
    def canceled_count(self):
        return self._counter.get('canceled',0)

    @property
    def executed_size(self):
        return self._counter.get('size',0)

    @executed_size.setter
    def executed_size(self, value):
        self._counter['size'] = value

    def reset_counter(self):
        self._counter={'ordered':0, 'filled':0, 'partially_filled':0, 'canceled':0, 'size':0}

    # historical_counterのシフト
    def _shift_historical_counter(self):
        self._historical_counter['ordered'].append(0)
        self._historical_counter['filled'].append(0)
        self._historical_counter['partially_filled'].append(0)
        self._historical_counter['canceled'].append(0)
        self._historical_counter['size'].append(0)

    def historical_counter(self, sec):
        return {'ordered': sum(list(self._historical_counter['ordered'])[-sec:]),
                'filled': sum(list(self._historical_counter['filled'])[-sec:]),
                'partially_filled': sum(list(self._historical_counter['partially_filled'])[-sec:]),
                'canceled': sum(list(self._historical_counter['canceled'])[-sec:]),
                'size': round(sum(list(self._historical_counter['size'])[-sec:]),8)}

    @property
    def list(self):
        return [dict({'id':o[0]},**o[1]) for o in self.order_dict.items()]

    def is_myorder(self, id):
        return id in list(self._my_id)

    # オーダーリストのゴミを掃除 
    def _delete_invalidate_order(self):
        invalidate = [id for id,value in self.order_dict.items() if value['invalidate']<time.time()]
        for id in invalidate:
            d = self.order_dict.pop(id)
            self._logger.info( "INVALIDATE : {}".format(d))
            self._canceled_list.append(dict({'id':id},**d))

    @property
    def executed_list(self):
        return deepcopy(list(self._executed_list))

    @property
    def canceled_list(self):
        return deepcopy(list(self._canceled_list))

    # 発注時にオーダーリストに登録する
    # new_order( id, size, side, price, closeid:optional )
    def new_order(self, **kwargs ):
        if id in self.order_dict :
            self._logger.error( '-'*50+"ID:{} is already in list".format(id) )

        item = dict({'ordered_time':time.time(), 'remain': float(kwargs['size'])}, **kwargs)

        self.order_dict[item['id']] = item
        self._my_id.append(item['id'])
        self._logger.info( "ORDERED [{}] {} {} price({}) size({}) {}".format(item['id'],item['symbol'], item['side'],round(item['price'],8),round(item['size'],8),
                "closeid({})".format(item['closeid']) if 'closeid' in item else "") )
        self._logger.debug( "{}".format(item) )
        self._counter['ordered'] += 1
        self._historical_counter['ordered'][-1] += 1

    # Private Websocket受信時にオーダーリストを更新する
    def update_order(self, id, side, price, size):
        if not self.is_myorder(id) :
            self._logger.debug( "ID:{} is not my order".format(id) )
            return False

        if id not in self.order_dict :
            self._logger.error( "ID:{} is not in order list (update error)".format(id) )

            if id in [d['id'] for d in list(self._executed_list)] :
                self._logger.error( "="*50 + "ID:{} already in executed_list".format(id) )
            if id in [d['id'] for d in list(self._canceled_list)] :
                self._logger.error( "="*50 + "ID:{} already in canceled_list".format(id) )
            return False

        if self.order_dict[id]['side']!=side or self.order_dict[id]['size']!=size :
            self._logger.error( '-'*50+"ID:{} data is not correcct\n{}".format(id,self.order_dict[id]) )
        self.order_dict[id]['price'] = float(price)
        self.order_dict[id]['accepted_time'] = time.time()
        self._logger.debug( "ACCEPTED ID:{}\n{}".format(id,self.order_dict[id]) )
        return True

    # 何らかの原因でキャンセルイベントが不達の場合でも30秒後にオーダーリストから削除するように登録
    def mark_as_invalidate(self, id, timeout=30):
        if not self.is_myorder(id) :
            self._logger.debug( "ID:{} is not my order".format(id) )
            return None

        if id not in self.order_dict :
            self._logger.debug( "ID:{} is not in order list (update error)".format(id) )

            if id in [d['id'] for d in list(self._executed_list)] :
                self._logger.debug( "="*50 + "ID:{} already in executed_list".format(id) )
            if id in [d['id'] for d in list(self._canceled_list)] :
                self._logger.debug( "="*50 + "ID:{} already in canceled_list".format(id) )
            return None

        # timeout秒後にオーダーリストから削除（ゴミ対策）
        self.order_dict[id]['invalidate']=min(self.order_dict[id].get('invalidate',9999999999),time.time()+timeout)
        self.order_dict[id]['expire']=max(self.order_dict[id].get('expire',9999999999),time.time()+timeout)

        return self.order_dict[id]

    # 約定を受信した際に部分約定の処理や全約定後のオーダーリストを削除する処理
    def executed(self, id, side, price, size, remain=-1):
        """
        約定処理
            id : orderID
            side : BUY/SELL
            price : 約定価格
            size : 約定サイズ（部分約定の場合今回の約定サイズ）
            remain : もし約定通知にremain通知があれば0以外で呼び出されて、オーダーリストの残数と念のためチェックを行う
        """
        if not self.is_myorder(id) :
            self._logger.debug( "ID:{} is not my order".format(id) )
            return None

        if not id in self.order_dict :
            self._logger.error( "ID:{} is not in order list (execution error)".format(id) )

            if id in [d['id'] for d in list(self._executed_list)] :
                self._logger.error( "="*50 + "ID:{} already in executed_list".format(id) )
                return None

            if id in [d['id'] for d in list(self._canceled_list)] :
                self._logger.error( "="*50 + "ID:{} already in canceled_list".format(id) )

            return None

        if self.order_dict[id]['side']!=side :
            self._logger.error( '='*50+"Side({}) is not correct :\n{}".format(side, self.order_dict[id]) )
            return None
        if self.order_dict[id]['remain']<size :
            self._logger.error( '='*50+"Size({}) is not correct (too much):\n{}".format(size, self.order_dict[id]) )
            return None
        if remain!=-1 and round(self.order_dict[id]['remain']-size-remain, 8)!=0 :
            self._logger.error( '='*50+"Remain({})-Size({}) is not match:\n{}".format(remain, size, self.order_dict[id]) )
            return None

        self.order_dict[id]['remain'] = round(self.order_dict[id]['remain']-size,8)
        if self.order_dict[id]['remain']==0 :
            d = self.order_dict.pop(id)
            d['price']=price
            self._logger.info( "EXECUTE ALL [{}]  {} {} price({}) size({}/{})".format(id, d.get('symbol'), side,price,size,d['size']) )
            self._logger.debug( "EXECUTE ALL : {}".format(d) )
            self._counter['filled'] += 1
            self._historical_counter['filled'][-1] += 1
#            self._executed_list.append(dict({'id':id, 'exec_time':time.time()},**d))
            self._executed_list.append(dict(d, **{'id':id, 'exec_time':time.time(), 'price':price, 'size':size}))
        else:
            d = self.order_dict[id]
            self._logger.info( "EXECUTE [{}]  {} {} price({}) size({}/{})".format(id, d.get('symbol'), side,price,size,d['size']) )
            self._logger.debug( "EXECUTE : {}".format(d) )
            self._executed_list.append(dict(self.order_dict[id], **{'id':id, 'exec_time':time.time(), 'price':price, 'size':size}))

        self._counter['size'] += size
        self._historical_counter['size'][-1] += size
        self.event.set()

        return d

    # Cancel / Expire などの受信でオーダーリストから削除
    def remove_order(self, id):
        """
        削除処理 (Cancel / Expire)
            id : orderID
        return : 削除成功時にはorder_dict
        """

        if not self.is_myorder(id) :
            return None

        if not id in self.order_dict :
            self._logger.debug( "ID:{} is not in order list".format(id) )

            if id in [d['id'] for d in list(self._executed_list)] :
                self._logger.debug( "="*50 + "ID:{} already in executed_list".format(id) )
            if id in [d['id'] for d in list(self._canceled_list)] :
                self._logger.debug( "="*50 + "ID:{} already in canceled_list".format(id) )
            return None

        d = self.order_dict.pop(id)
        self._canceled_list.append(dict({'id':id},**d))

        if d['size']!=d['remain'] :
            self._counter['partially_filled'] += 1
            self._historical_counter['partially_filled'][-1] += 1
        else:
            self._counter['canceled'] += 1
            self._historical_counter['canceled'][-1] += 1
        self._logger.info( "CANCELED (Canceld or Expired) ID:{}".format(id) )
        self._logger.debug( "canceld order : {}".format(d) )

        return d
