# coding: utf-8
#!/usr/bin/python3

import time
from collections import deque
from libs.utils.jsonfile import JsonFile

# 建玉リスト管理クラス (先入れ先出しポジション管理 bitFlyer)
class OpenPositionFIFO(object):

    def __init__(self, logger, update_profitfile, order_rate, order_currency):
        self._logger = logger
        self._position = deque()
        self._update_profitfile = update_profitfile
        self.realized = 0   # 確定済み損益
        self.commission = 0 # コミッション(makerコミッション-takerコミッション, SFDなど)
        self.ref_ltp = 0    # 含み損益計算用
        self.base_position = 0

        self.file = JsonFile(self._logger)

    def renew_posfile(self, filename):
        self._logger.info( '-'*100 )
        
        pos_list = self.file.reload_file(filename)

        position_dict={}
        for data in pos_list :
            if 'position' in data :
                d = data['position']
                if d['size']==0 :
                    if d['id'] in position_dict:
                        position_dict.pop(d['id'])
                    else:
                        self._logger.error( "ID {} is not in position list".format(d['id']))
                else:
                    position_dict[d['id']]=d
        self._position.clear()
        for d in position_dict.values():
            self._logger.info( d )
            self._position.append(d)

        self.file.renew_file( [{'position':p} for p in list(self._position)] )

        self._logger.info( " pos_size = {} / average_price = {}".format(self.size, self.average_price)) 
        self._logger.info( '-'*100 )

    def _update_posfile(self, dict):
        self.file.add_data({'timestamp':time.time(), 'position':dict})

    def executed(self, id, side, price, size, commission=0):
        assert size > 0
        org_side = self.side
        remain = size
        self.commission += commission
        while remain>0 :
            if self.side in ['NONE',side] :
                # 同方向ポジションの買い増しまたは新規エントリー
                id_list = [d['id'] for d in list(self._position)] 
                if id in id_list :
                    # すでに同じIDのポジションがあればそこへ追加
                    idx = id_list.index(id)
                    p = self._position[idx]
                    p['price'] = round((p['price']*p['size']+price*remain)/(p['size']+remain), 8)
                    p['size'] = round(p['size']+remain, 8)
                    self._position[idx] = p
                else:
                    p = {'id':id,'side': side, 'price': float(price), 'size': float(remain)}
                    self._position.append(p)
                self._update_posfile(p)
                remain = 0
            else:
                # 逆方向でのポジションクローズ
                p = self._position.popleft()  # 一番古い建玉からクローズしていく
                if round(remain-p['size'], 8)>=0 :
                    profit = (p['price']-price)*p['size']*(1 if side=='BUY' else -1)
                    self.realized += profit

                    remain = round(remain-p['size'], 8)
                    p['size']=0
                    self._update_posfile(p)
                else:
                    profit = (p['price']-price)*remain*(1 if side=='BUY' else -1)
                    self.realized += profit

                    p['size'] = round(p['size']-remain, 8)
                    self._update_posfile(p)
                    self._position.appendleft(p)
                    remain = 0

        if org_side != self.side and len(self._position)!=0 :
            self.renew_posfile(self.file.filename)

        self._update_profitfile()

    @property
    def side(self):
        if len(self._position)==0 :
            return 'NONE'
        return self._position[-1]['side']

    @property
    def average_price(self):
        if len(self._position)==0 : return 0

        value = []
        size = []
        side = self.side
        positions = list(self._position)

        for p in positions:
            value.append(p['price']*p['size'])
            size.append(p['size'])
            if side != p['side']:
                self._logger.error("Position list error!!!   {} != {}".format(side, p['side']))

        return round(sum(value) / sum(size))

    @property
    def size(self):
        return round(sum(p['size'] for p in list(self._position)) * (-1 if self.side=='SELL' else 1), 8)

    # 損益計算 (フィアット建て)
    @property
    def unreal(self):
        return int((self.ref_ltp-self.average_price)*self.size if self.ref_ltp!=0 else 0)

    @property
    def profit(self):
        return int(self.realized+self.commission+self.unreal)

    @property
    def fixed_profit(self):
        return int(self.realized+self.commission)


