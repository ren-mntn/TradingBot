# -*- coding: utf-8 -*-
from libs.base_strategy import Strategy
import numpy as np
import tempfile
import os
import pickle
import json
from zipfile import ZipFile
import importlib.machinery as imm
import asyncio

class MyStrategy(Strategy):

    async def initialize(self):

        # モデルファイルの読み込み
        with tempfile.TemporaryDirectory() as temp_path:
            with ZipFile(os.path.dirname(__file__) + '/' + self.parameters['model_file'], 'r') as zf:
                fp = zf.open("model_info.txt")
                model_info = json.load(fp);

                # 特徴量作成ファイル
                model_filename =model_info['calclate_features_py']
                zf.extract(model_filename,temp_path)
                module_name = model_filename.split('.')[0].replace('/', '.')
                self._module = imm.SourceFileLoader(module_name, temp_path+'/'+model_filename).load_module()

                # パラメータの読み込み
                self._params = self._module.params()
                self._params.update(self.parameters['update_params'])
                self._logger.info( "logic params : {}".format(self._params) )

                # 学習済みモデルの読み込み
                zf.extract('model_b.pickle',temp_path)
                with open(temp_path+'/model_b.pickle', mode='rb') as fp:
                    self._b_model = pickle.load(fp)
                zf.extract('model_s.pickle',temp_path)
                with open(temp_path+'/model_s.pickle', mode='rb') as fp:
                    self._s_model = pickle.load(fp)

                # 選択特徴量
                self._features = model_info['features']
                self._nouse_columns = model_info['nouse_columns']

            self._logger.info( "Loaded model file : {}".format(self.parameters['model_file']) )
            self._logger.info( "timescale setting : {}".format(self._params['timescale']) )
            self._logger.info( "Features : {}".format(self._features) )

        self._last_candle = 0

        # APIから取得するローソク足が更新されたら candle_update関数を呼び出すよう設定します
        self.candlegen = self.APICandle( timeframe=int(self._params['timescale']/60), num_of_candle=self._params['num_of_candle'], callback=self.candle_update )

        # loop_period間隔でself.status関数が呼び出されるように設定
        self.schedule = self.Scheduler(callback=self.status, interval=self.parameters['loop_period'])

    async def candle_update(self):

        # APIから取得されているローソク足
        self._candle = self.candlegen.candle.copy()

        # ローソク足が足りなければ何もしない
        if len(self._candle.index.values)<self._params['num_of_candle'] :
            return

        # ローソク足が更新されていなければ何もしない
        if self._last_candle == self._candle.index.values[-1] :
            return
        self._last_candle = self._candle.index.values[-1]

        # 最後の足を1本捨てる
        self._candle = self._candle[:-1].copy()

        # ロジック部で指値を出す位置を計算
        self._candle = self._module.limit_price( self._candle, **self._params)

        # ロジック部で指値を出す位置を計算
        original_columns = list(self._candle.columns)
        self._candle, features = self._module.calculate_features( self._candle, self._nouse_columns )

        # ロジック部で指値を出す位置を計算
        self._candle['y_pred_buy'] = self._b_model.predict(self._candle[self._features])
        self._candle['y_pred_sell'] = self._s_model.predict(self._candle[self._features])

        # 最後の3本だけログ表示（チェック用）
        self._logger.info( "\n----------api\n{}".format(self._candle[original_columns+['y_pred_buy','y_pred_sell']].tail(10)) )

        self._candle[['open','high','low','close','volume']+self._features+['y_pred_buy','y_pred_sell']].to_csv("candle.csv")

        buy_entry =  (self._candle['y_pred_buy'][-1]>0  or self.parameters['always_entry']) and self.parameters['buy']
        sell_entry = (self._candle['y_pred_sell'][-1]>0 or self.parameters['always_entry']) and self.parameters['sell']

        # 買いエントリーのロットサイズ計算
        buysize =  min( self.parameters['lot']*buy_entry +                                     # エントリーのためのサイズ
                        max(0,-self.current_pos),                                              # クローズのために必要なサイズ
                        self.parameters['lot']*self._params['pyramiding'] - self.current_pos ) # 許容最大ロットまでのサイズ

        # 売りエントリーのロットサイズ計算
        sellsize = min( self.parameters['lot']*sell_entry+                                     # エントリーのためのサイズ
                        max(0, self.current_pos),                                              # クローズのために必要なサイズ
                        self.parameters['lot']*self._params['pyramiding'] + self.current_pos ) # 許容最大ロットまでのサイズ


        # エントリー（指値の変更）
        await asyncio.gather(
            asyncio.create_task( self.set_limit_order( "BUY",  buysize,  min(self._candle['buy_price'][-1],  self.best_bid) ) ),
            asyncio.create_task( self.set_limit_order( "SELL", sellsize, max(self._candle['sell_price'][-1], self.best_ask) ) )
        )

    async def set_limit_order(self, side, size, price):

        price = self.exchange.round_order_price( price )

        # 発注済みのリストから同一方向のオーダーを抜き出してキャンセル
        ids = [{'id':o['id'],'price':o['price'],'size':o['size']} for o in self.ordered_list if o['side']==side]
        for id in ids:
            await self.cancelorder( id['id'] )

        # 最低発注数に満たない場合には発注しない（発注済みのオーダーキャンセルだけ実施）
        if size < self.minimum_order_size :
            return

        # 新規発注
        await self.sendorder( "LIMIT", side,  size=size, price=price )

    async def status(self):

        # ループの秒数設定が変わっていたら変更
        if self.schedule.interval != self.parameters['loop_period'] :
            self.schedule.interval = self.parameters['loop_period']

        self._logger.debug( "order list : {}".format(self.ordered_list))

        if self._last_candle == 0 : return

        buy_order_list = [d['price'] for d in self.ordered_list if d['side'].lower()=='buy']
        sell_order_list = [d['price'] for d in self.ordered_list if d['side'].lower()=='sell']
        self._logger.info( '[{}: SELL:{} BUY:{}]  LTP:{:.0f} Profit:{:>+.8f}({:+.8f}) Position:{:.3f} Delay:{:>4.0f}ms'.format(
            self._candle.index[-1],
            "{:.1f}({:+.1f})".format(sell_order_list[0],sell_order_list[0]-self.ltp) if len(sell_order_list)!=0 else "-"*13,
            "{:.1f}({:+.1f})".format(buy_order_list[0], buy_order_list[0]-self.ltp)  if len(buy_order_list)!=0  else "-"*13,
            self.ltp, self.current_profit, self.current_profit_unreal, self.current_pos, self.server_latency) )

        return False
