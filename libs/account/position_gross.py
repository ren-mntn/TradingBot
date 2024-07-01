# coding: utf-8
#!/usr/bin/python3

import math
import time
from libs.utils import JsonFile, LockCounter
# 建玉リスト管理クラス (ネットアウトではない管理 GMO)
class OpenPositionGross(object):

    def __init__(self, logger, update_profitfile, order_rate, order_currency):
        self._logger = logger
        self._long_position = {}
        self._short_position = {}
        self._position_lock = LockCounter(self._logger, self.__class__.__name__)
        self._update_profitfile = update_profitfile
        self.realized = 0   # 確定済み損益
        self.commission = 0 # コミッション(makerコミッション-takerコミッション, SFDなど)
        self.ref_ltp = 0    # 含み損益計算用
        self.base_position = 0

        self.file = JsonFile(self._logger)

    def renew_posfile(self, filename):
        first_load = (self.file.filename!=filename)

        for val in self._long_position.values():
            self._update_posfile(val)
        for val in self._short_position.values():
            self._update_posfile(val)

        pos_list = self.file.reload_file(filename)

        with self._position_lock :
            self._long_position={}
            self._short_position={}
            for data in pos_list :
                if 'position' in data :
                    d = data['position']
                    if d['side']=="BUY" :
                        position_dict = self._long_position
                    else:
                        position_dict = self._short_position

                    if d['size']==0 :
                        if d['posid'] in position_dict:
                            position_dict.pop(d['posid'])
                        else:
                            self._logger.error( "ID {} is not in position list".format(d['posid']))
                    else:
                        # 最初の読み込み時には決済オーダーはすべて無しとする
                        if first_load :
                            d['closeorder']=False
                        position_dict[d['posid']]=d

            poslist = []
            for p in self._long_position.values():
                self._logger.info( "LONG POS: "+str(p) )
                poslist.append({'position':p})
            for p in self._short_position.values():
                self._logger.info( "SHORT POS: "+str(p) )
                poslist.append({'position':p})

            self.file.renew_file( poslist )

        self._logger.info( '-'*100 )
        self._logger.info( " pos_size = {} / average_price = {}".format(self.size, self.average_price)) 
        self._logger.info( '-'*100 )

    def is_myorder(self, id):
        return (id in self._long_position) or (id in self._short_position)

    def _update_posfile(self, dict):
        self.file.add_data({'timestamp':time.time(), 'position':dict})

    def executed(self, posid, side, price, size, orderid, commission=0, settleType='OPEN'):
        assert size > 0
        org_side = self.side

        if settleType=='OPEN' :
            # 新規のポジション
            p = {'posid':posid, 'orderid':orderid, 'side': side, 'price': float(price), 'size': float(size), 'closeorder':False}
            if side=='BUY' :
                self._long_position[posid]=p
                self._update_posfile(p)

            elif side=='SELL' :
                self._short_position[posid]=p
                self._update_posfile(p)
        else:
            # 決済オーダー約定
            if side=='BUY' :
                if posid in self._short_position :

                    # オーダーサイズよりも多い決済オーダーは来るはずはない
                    if self._short_position[posid]['size']<size:
                        self._logger.error("Position size error!!!  [execsize={}/pos={}]".format(size, self._short_position[posid]))
                        size = self._short_position[posid]['size']

                    # エントリー価格から損益計算
                    profit = math.floor((self._short_position[posid]['price']-price)*size)
                    self.realized += profit

                    # 残数がゼロなら辞書から削除
                    if round(self._short_position[posid]['size']-size, 8)==0 :
                        p = self._short_position.pop(posid)
                        p['size']=0
                        self._update_posfile(p)

                    # まだオーダー残があるならサイズを減算
                    else:
                        self._short_position[posid]['size'] = round(self._short_position[posid]['size']-size, 8)
                        self._update_posfile(self._short_position[posid])

                # オーダーリストにない決済オーダーは来るはずはない
                else:
                    self._logger.error("Position error!!!  [{}/{}] can't find from order list".format(side, posid))
                    self._logger.debug( self._short_position )

            elif side=='SELL' :
                if posid in self._long_position :

                    # オーダーサイズよりも多い決済オーダーは来るはずはない
                    if self._long_position[posid]['size']<size:
                        self._logger.error("Position size error!!!  [execsize={}/pos={}]".format(size, self._long_position[posid]))
                        size = self._long_position[posid]['size']

                    # エントリー価格から損益計算
                    profit = math.floor((price-self._long_position[posid]['price'])*size)
                    self.realized += profit

                    # 残数がゼロなら辞書から削除
                    if round(self._long_position[posid]['size']-size, 8)==0 :
                        p = self._long_position.pop(posid)
                        p['size']=0
                        self._update_posfile(p)

                    # まだオーダー残があるならサイズを減算
                    else:
                        self._long_position[posid]['size'] = round(self._long_position[posid]['size']-size, 8)
                        self._update_posfile(self._long_position[posid])

                # オーダーリストにない決済オーダーは来るはずはない
                else:
                    self._logger.error("Position error!!!  [{}/{}] can't find from order list".format(side, posid))
                    self._logger.debug( self._long_position )

        if org_side != self.side and (len(self._long_position)!=0 or len(self._short_position)!=0) :
            self.renew_posfile(self.file.filename)

        self._update_profitfile()

    @property
    def side(self):
        size = self.size
        if size==0 :
            return 'NONE'
        elif size>0 :
            return 'BUY'
        else :
            return 'SELL'

    def _calc_average_price(self, price_dict):
        if len(price_dict)==0 : return 0, 0

        value = []
        size = []
        side = None
        self._position_lock.wait()
        d = price_dict.values()
        for p in d :
            value.append(p['price']*p['size'])
            size.append(p['size'])
            if not side:
                side = p['side']
            if side != p['side']:
                self._logger.error("Position list error!!!   {} != {}".format(side, p['side']))

        ave = round(sum(value) / sum(size), 3)
#        print( "average = ",ave )
        return ave, sum(size)


    # 損益計算
    @property
    def _calc_position(self):
        long_ave, long_size = self._calc_average_price(price_dict = self._long_position)
        short_ave, short_size = self._calc_average_price(price_dict = self._short_position)
        total_size = round(long_size-short_size,8)

        if total_size==0 :
            pos_profit = (short_ave-long_ave)*short_size # 相殺損益
            pos_average = 0
        elif total_size>0 :
            pos_profit = (short_ave-long_ave)*short_size # 相殺損益
            pos_average = long_ave
        else:
            pos_profit = (short_ave-long_ave)*long_size # 相殺損益
            pos_average = short_ave

        unreal = math.floor((self.ref_ltp-pos_average)*total_size) if self.ref_ltp!=0 else 0
#        print( "long_size={} , short_size={} , total_size={}".format(long_size,short_size,total_size) )
#        print( "pos_average={} , self.ref_ltp={} , unreal={}".format(pos_average,self.ref_ltp,unreal) )

        return total_size, pos_average, pos_profit+unreal

    @property
    def size(self):
        return self._calc_position[0]

    @property
    def average_price(self):
        return self._calc_position[1]

    # 損益計算 (フィアット建て)
    @property
    def unreal(self):
        return int(self._calc_position[2])

    @property
    def profit(self):
        return int(self.realized+self.commission+self.unreal)

    @property
    def fixed_profit(self):
        return int(self.realized+self.commission)


