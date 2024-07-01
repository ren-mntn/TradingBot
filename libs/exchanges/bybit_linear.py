# coding: utf-8
#!/usr/bin/python3

import asyncio
from datetime import datetime, timedelta, timezone
import time
import traceback
import pandas as pd

import pybotters

from libs.utils import TimeConv, LockCounter
from libs.market import *
from libs.exchanges.base_module import MultiProc_WS, RestAPIExchange, WebsocketExchange
from libs.account import AccountInfo, OpenPositionKeepAve, OpenPositionKeepAveLinear

from libs.exchanges.bybit import Bybit

# https://bybit-exchange.github.io/docs/linear/#t-introduction


# リニアUSDT契約
class BybitUSDT(pybotters.BybitUSDTDataStore, Bybit):

    # リニアUSDT契約
    @property
    def is_linear(self):
        return True

    def units(self,value=0):
        return {'unitrate' :1,                              # 損益額をプロフィットグラフに表示する単位に変換する係数
                'profit_currency' : 0,                      # 利益額の単位 0:USD 1:JPY
                'title':"USD {:+,.2f}".format(value),       # 表示フォーマット
                'pos_rate':1}                               # ポジションをcurrency単位に変換する係数

    def __init__(self, logger, candle_store, apikey=('',''), testnet=False):
        self._logger = logger
        self.candle_store = candle_store   # 分ローソク足変換クラス
        self.auth = (apikey[0]!='' and apikey[1]!='')
        self._apikey = apikey

        if testnet :
            self._api_url = "https://api-testnet.bybit.com"
            self._endpoint = "wss://stream-testnet.bybit.com"
            self.exchange_name = "bybit_testnet"
        else:
            self._api_url = "https://api.bybit.com"
            self._endpoint = "wss://stream.bybit.com"
            self.exchange_name = "bybit"

        self._client = pybotters.Client(apis={self.exchange_name: [apikey[0],apikey[1]]}, base_url=self._api_url)
        self._candle_api_lock = asyncio.Lock()
        self._datastore_lock = LockCounter(self._logger, self.__class__.__name__)
        self._order_dict_lock = LockCounter(self._logger, "order_dict")
        self._candle_subscribed = []
        pybotters.BybitUSDTDataStore.__init__(self)
        RestAPIExchange.__init__(self)
        WebsocketExchange.__init__(self)

        self.PendingUntil = time.time()
        self.RateLimitRemaining = 100


    async def start(self, subscribe={}, symbol='BTCUSDT'):
        self._param = subscribe
        self.symbol = symbol

        await self._get_market_info()

        # リニア契約
        self.my = AccountInfo(self._logger, OpenPositionKeepAveLinear)

        # データ保存用クラス
        self.board_info = BoardInfo( self._logger )
        self.execution_info = ExecuionInfo( self._logger )
        self.ticker_info = TickerInfo()
        self.ticker_info.open_interest = 0

        # 指定のチャンネルの購読
        self._handler = {'None':None}
        self._ws_args = []
        self._channels = []
        if self._param.get('execution', True) : self._subscribe(topic=f"trade.{self.symbol}", handler=self._on_executions)
        if self._param.get('ticker', False) :   self._subscribe(topic=f"instrument_info.100ms.{self.symbol}", handler=self._on_ticker)
        if self._param.get('board', False) :    self._subscribe(topic=f"orderBook_200.100ms.{self.symbol}", handler=self._on_board)

        # ローソク足受信処理タスクを開始
        asyncio.create_task(self._wait_candle_event(), name="bybit_usdt_candle")

        # WebSocketタスクを開始
        self.ws = MultiProc_WS( logger = self._logger, exchange_name = self.__class__.__name__,
                      handler = self.onmessage,
                      endpoint = self._endpoint+"/realtime_public",
                      apis = {self.exchange_name: ["",""]},
                      send_json = self._ws_args,
                      lock = self._datastore_lock,
                      disconnect_handler = self._disconnected)
        asyncio.create_task(self.ws._start_websocket(logging=False), name="bybit_usdt_websocket_public")

        if self.auth:
            self._handler["execution"]=self._on_my_account
            self._handler["position"]=self._on_my_positions
            self._handler["order"]=self._on_my_orders

           # WebSocket(private)タスクを開始
            self.ws_private = MultiProc_WS( logger = self._logger, exchange_name = self.__class__.__name__+'Private',
                          handler = self.onmessage,
                          endpoint = self._endpoint+"/realtime_private",
                          apis = {self.exchange_name: [self._apikey[0],self._apikey[1]]},
                          send_json = {"op": "subscribe", "args": ["execution", "position", "order", "wallet"]},
                          lock = self._datastore_lock,
                          disconnect_handler = self._disconnected)
            asyncio.create_task(self.ws_private._start_websocket(logging=False), name="bybit_usdt_websocket_private")

            # DataStoreの初期化
            await self.initialize(
                 self._client.get('/private/linear/order/search', params={'symbol': self.symbol}),
                self._client.get('/private/linear/position/list', params={'symbol': self.symbol}),
                self._client.get('/v2/private/wallet/balance', params={'coin': self._collateral_coin[self.symbol]}),
            )


        # 自動キャンセルタスクを開始
        asyncio.create_task(self._cancel_task(), name="bybit_usdt_cancel")

        if self.auth:
            while not (self.ws.pid) :
                await asyncio.sleep(1)
            self._logger.info("Websocket connected")

        return self

    def _onmessage(self, msg, ws):
        self._logger.debug("recv bybit(linear) websocket [{}]".format(msg.get('topic', 'None')))
        self._logger.trace("recv data [{}]".format(msg))

        topic_handler = self._handler.get(msg.get('topic', 'None'))
        if topic_handler != None:
            topic_handler(msg)

            if topic_handler in [self._on_ticker, self._on_my_account, self._on_my_positions, self._on_my_orders ] :
                pybotters.BybitUSDTDataStore._onmessage(self,msg,ws)

        else:
            pybotters.BybitUSDTDataStore._onmessage(self,msg,ws)

    def _on_board(self,msg):
        self.board_info.time = self._epoc_to_dt(int(msg['timestamp_e6'])/1000000)
        recept_data = msg.get('data')
        msg_type=msg.get('type')
        if msg_type=='snapshot' :
            self.board_info.initialize_dict()
            self.board_info.insert2(recept_data.get('order_book',[]),sign=-1)
        elif msg_type=='delta' :
            self.board_info.delete2(recept_data.get('delete',[]),-1)
            self.board_info.change2(recept_data.get('update',[]),-1)
            self.board_info.insert2(recept_data.get('insert',[]),-1)
        self.board_info.event.set()
        for event in self.board._events:
            event.set()

    def _on_my_account(self,message):
        '''
        '''
        self._logger.info("_on_my_account={}".format(message))

        self._order_dict_lock.wait()
        for d in message.get('data',[]) :
            if self.my.order.executed( id=d['order_id'], side=d['side'].upper(), price=float(d['price']), size=d['exec_qty'] ) :
                self.my.position.executed( id=d['order_id'], side=d['side'].upper(), price=float(d['price']), size=d['exec_qty'] )

    def _on_my_positions(self,message):
        '''
        {'topic': 'position', 'data': [{'user_id': 1473148, 'symbol': 'BTCUSD', 'size': 45000, 'side': 'Sell', 'position_value': '1.11996039', 'entry_price': '40179.99243705', 'liq_price': '44298.5', 'bust_price': '44544', 'leverage': '100', 'order_margin': '0.00140075', 'position_margin': '0.10972497', 'available_balance': '0.08964632', 'take_profit': '0', 'stop_loss': '0', 'realised_pnl': '0', 'trailing_stop': '0', 'trailing_active': '0', 'wallet_balance': '0.11173187', 'risk_id': 1, 'occ_closing_fee': '0.00060615', 'occ_funding_fee': '0', 'auto_add_margin': 1, 'cum_realised_pnl': '-1.80712242', 'position_status': 'Normal', 'position_seq': 4357475864, 'Isolated': False, 'mode': 0, 'position_idx': 0, 'tp_sl_mode': 'UNKNOWN', 'tp_order_num': 0, 'sl_order_num': 0, 'tp_free_size_x': 45000, 'sl_free_size_x': 45000}]}
        {'topic': 'position', 'data': [{'user_id': 1473148, 'symbol': 'BTCUSD', 'size': 45000, 'side': 'Sell', 'position_value': '1.11996039', 'entry_price': '40179.99243705', 'liq_price': '44359.5', 'bust_price': '44605.5', 'leverage': '100', 'order_margin': '0', 'position_margin': '0.11112656', 'available_balance': '0.09064774', 'take_profit': '0', 'stop_loss': '0', 'realised_pnl': '0', 'trailing_stop': '0', 'trailing_active': '0', 'wallet_balance': '0.11173187', 'risk_id': 1, 'occ_closing_fee': '0.00060531', 'occ_funding_fee': '0', 'auto_add_margin': 1, 'cum_realised_pnl': '-1.80712242', 'position_status': 'Normal', 'position_seq': 4357475864, 'Isolated': False, 'mode': 0, 'position_idx': 0, 'tp_sl_mode': 'UNKNOWN', 'tp_order_num': 0, 'sl_order_num': 0, 'tp_free_size_x': 45000, 'sl_free_size_x': 45000}]}
        '''
        self._logger.debug("_on_my_positions={}".format(message))

    def _on_my_orders(self,message):
        '''
        {'topic': 'order', 'data': [{'order_id': '4d559929-8814-4e4b-9969-750bd6a7dc89', 'order_link_id': '', 'symbol': 'BTCUSD', 'side': 'Buy', 'order_type': 'Limit', 'price': '40000', 'qty': 40000, 'time_in_force': 'GoodTillCancel', 'create_type': 'CreateByUser', 'cancel_type': '', 'order_status': 'New', 'leaves_qty': 40000, 'cum_exec_qty': 0, 'cum_exec_value': '0', 'cum_exec_fee': '0', 'timestamp': '2022-04-16T00:34:09.613Z', 'take_profit': '0', 'stop_loss': '0', 'trailing_stop': '0', 'last_exec_price': '0', 'reduce_only': False, 'close_on_trigger': False}]}
        {'topic': 'order', 'data': [{'order_id': '4d559929-8814-4e4b-9969-750bd6a7dc89', 'order_link_id': '', 'symbol': 'BTCUSD', 'side': 'Buy', 'order_type': 'Limit', 'price': '40000', 'qty': 40000, 'time_in_force': 'GoodTillCancel', 'create_type': 'CreateByUser', 'cancel_type': 'CancelByUser', 'order_status': 'Cancelled', 'leaves_qty': 0, 'cum_exec_qty': 0, 'cum_exec_value': '0', 'cum_exec_fee': '0', 'timestamp': '2022-04-16T00:40:24.625Z', 'take_profit': '0', 'stop_loss': '0', 'trailing_stop': '0', 'last_exec_price': '0', 'reduce_only': False, 'close_on_trigger': False}]}
        '''
        self._logger.debug("_on_my_orders={}".format(message))

        self._order_dict_lock.wait()
        for d in message.get('data',[]) :
            id = d.get('order_id')
            if d['order_status']=='New' :
                if self.my.order.update_order(id, side=d['side'].upper(), price=float(d['price']), size=d['qty']) :
                    self._logger.debug( self.__class__.__name__ + " : [order] New Order : " + str(d) )
            elif d['order_status']=='Cancelled' :
                if self.my.order.remove_order( id ) :
                    self._logger.debug( self.__class__.__name__ + " : [order] Canceled : " + str(d) )
                    self.my.position.executed( id, commission=-float(d['cum_exec_fee']) )  # ここまでに得たコミッションを適用
            elif d['order_status']=='Filled' :
                if self.my.order.is_myorder( id ) :
                    self._logger.debug( self.__class__.__name__ + " : [order] Filled : " + str(d) )
                    self.my.position.executed( id, commission=-float(d['cum_exec_fee']) )
            elif d['order_status']=='PartiallyFilled' :
                if self.my.order.is_myorder( id ) :
                    self._logger.debug( self.__class__.__name__ + " : [order] PartiallyFilled : " + str(d) )
            else:
                self._logger.debug( self.__class__.__name__ + " : [order] Unknown : " + str(d) )

    async def get_candles( self, timeframe, num_of_candle=500, symbol=None, since=None, need_result=True ):

        if need_result:
            self.create_candle_class('temp_candle')

        target_symbol = symbol or self.symbol

        # 初めて取得するシンボルであればwsを購読
        if target_symbol not in self._candle_subscribed:
            send_json = self._subscribe(topic=f"candle.1.{target_symbol}", handler=None)
            self._candle_subscribed.append(target_symbol)
            self.ws.send_subscribe( {"op": "subscribe", "args": [send_json]}, self._ws_args )

        await self._candle_api_lock.acquire()

        # 基本の分足を入れるDataStoreクラスをローソク足変換クラスへ登録
        exchange_symbol = self.__class__.__name__+target_symbol
        self.candle_store.set_min_candle(exchange_symbol,self.candle)

        api_timeframe = 60
        timeframe_sec = timeframe*api_timeframe

        endtime = int(datetime.now(tz=timezone.utc).timestamp()//api_timeframe*api_timeframe)
        starttime = since if since else (endtime//timeframe_sec*timeframe_sec)-timeframe_sec*num_of_candle-api_timeframe
        keep_running = True

        while keep_running and starttime<=endtime and num_of_candle!=0:

            self._logger.debug( "starttime: {}".format(datetime.fromtimestamp(starttime,tz=timezone(timedelta(hours=9),'JST'))) )
            self._logger.debug( "endtime  : {}".format(datetime.fromtimestamp(endtime,tz=timezone(timedelta(hours=9),'JST'))) )

            try:
                responce = ''
                res = await self._client.get("/public/linear/kline", params={'symbol': target_symbol, 'interval': '1', 'from':starttime})
                responce = await res.json()
                self._update_api_limit(responce)
                rows = responce['result']

                # 得られたデータが空っぽなら終了
                if len(rows)==0 :
                    keep_running = False
                    self._logger.debug( "{} : Fetch candles from API (request from {}) [empty]".format(target_symbol,
                        datetime.fromtimestamp(starttime,tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M')) )
                    break

                # 得られたデータが指定されたスタート時刻まで含まれていなかったらまだその時刻のローソクはできていないので終了
                if starttime>int(rows[-1]['open_time']) :
                    keep_running = False
                    break

                data = [ c for c in rows if starttime<=int(c['open_time'])<=endtime]
                self._logger.info( "{} : Fetch {} candles timeframe{} from API [{}~{}]".format(target_symbol,len(data),api_timeframe,
                    datetime.fromtimestamp(data[0]['open_time'],tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M'),
                    datetime.fromtimestamp(data[-1]['open_time'],tz=timezone(timedelta(hours=9),'JST')).strftime('%Y/%m/%d %H:%M')) )

                # 分足データストアに保管
                self.candle._onresponse(data)

                # データ取得用一時ストア
                if need_result:
                    self._stores.get('temp_candle')._onmessage({'symbol': target_symbol, 'interval': timeframe_sec, 'kline':
                                     [(int(c['start'])*1000,float(c['open']),float(c['high']),float(c['low']),float(c['close']),float(c['volume']),float(c['turnover'])) for c in data]})

                # 取得した分足が8000本を超えるようなら一度リサンプルを行う
                if len(self.candle)>8000:
                    with self._datastore_lock :
                        self.candle_store.resample(symbol=target_symbol)

                # 次回の取得開始時
                starttime = int(rows[-1]['start_at']+api_timeframe)
                endtime = int(datetime.now(tz=timezone.utc).timestamp()//api_timeframe*api_timeframe)

                # 得られたデータが目的のendtimeを超えていたら終了
                if endtime < rows[-1]['start_at']:
                    keep_running = False

            except Exception:
                if responce :
                    self._logger.info( responce )
                self._logger.error( traceback.format_exc() )
                self._candle_api_lock.release()
                return None

        # 取得した分足からリサンプルしてターゲットの足を生成
        with self._datastore_lock :
            self.candle_store.resample(symbol=target_symbol)

        # イベントが都度発生しないようにまとめてローソク足のデータストアへ入れる
        for c in self.candle_store._target_candle_list:
            pool = c.pop('pool')
            c['pool']=[]
            for p in pool:
                c['target_candle']._onmessage(p)

        # データを返す必要が無ければ何も生成せずにリターン
        if not need_result:
            self._candle_api_lock.release()
            return

        candles = self._stores.get('temp_candle').find({'symbol':target_symbol,  'interval':timeframe_sec})
        df = pd.DataFrame(candles)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        df = df.set_index('timestamp').sort_index()

        agg_param = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum', 'turnover': 'sum'}
        df = df.resample(f'{timeframe*60}S').agg(agg_param)
        df['close'].fillna(method='ffill', inplace=True)
        df['open'].fillna(df['close'], inplace=True)
        df['high'].fillna(df['close'], inplace=True)
        df['low'].fillna(df['close'], inplace=True)
        df['volume'].fillna(0, inplace=True)
        df['turnover'].fillna(0, inplace=True)

        self._candle_api_lock.release()

        return df.tz_convert(timezone(timedelta(hours=9), 'JST')).tail(num_of_candle)

    # APIからTickerを取得
    async def ticker_api(self, **kwrgs):
        try:
            res = await self._client.get('/public/linear/recent-trading-records', params={'symbol': kwrgs.get('symbol', self.symbol), 'limit':1})
            data = await res.json()
            self._update_api_limit(data)
            self.my.position.ref_ltp = data['result'][0]['price']
            return {'stat':data['ret_code'] , 'ltp':data['result'][0]['price'], 'msg':data}
        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

    # APIから現在ポジを取得
    async def getpositions(self, **kwrgs):
        try:
            r = await self._client.get("/private/linear/position/list", params={'symbol':kwrgs.get('symbol', self.symbol)})
            res = await r.json()
            self._update_api_limit(res)
        except Exception:
            self._logger.error(traceback.format_exc())
            return []

        if res['ret_code']==0 :
            return res.get("result")
        else:
            return []
    
    # ウォレット残高
    async def getbalance(self, coin):
        try:
            if coin : 
                r = await self._client.get("/v2/private/wallet/balance", params={'coin': coin,})
            else:
                r = await self._client.get("/v2/private/wallet/balance")
            res = await r.json()
            self._update_api_limit(res)
            return res
        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'ret_code': -999, 'msg': str(e)}

    # 発注
    async def sendorder(self, order_type, side, size, price=0, auto_cancel_after=2592000, **kwargs ):
        """
        sendorder( order_type='LIMIT',             # 指値注文の場合は "LIMIT", 成行注文の場合は "MARKET" を指定します。
                   side='BUY',                     # 買い注文の場合は "BUY", 売り注文の場合は "SELL" を指定します。
                   size=0.01,                      # 注文数量を指定します。
                   price=4800000,                  # 価格を指定します。order_type に "LIMIT" を指定した場合は必須です。
                   auto_cancel_after,              # 0 以外を指定すると指定秒数経過後キャンセルを発行します
                   symbol="BTCUSD"                 # 省略時には起動時に指定した symbol が選択されます
                   time_in_force="GoodTillCancel", # 執行数量条件 を "GoodTillCancel", "ImmediateOrCancel", "FillOrKill", "PostOnly"で指定します。
                   reduce_only,                    # True means your position can only reduce in size if this order is triggered
                  ) )

        return: { 'stat': エラーコード,            # オーダー成功時 0
                  'msg':  エラーメッセージ,
                  'ids' : オーダーIDのリスト,      # オーダー成功時 [id,]  オーダー失敗時 []
                }
        """

        pos_side = self.my.position.side
        if self.noTrade and (pos_side==side or pos_side=='NONE'):
            self._logger.info("No trade period" )
            return {'stat': -999, 'msg': "No trade period", 'ids': []}

        if self.RateLimitRemaining<10:
            self._logger.info("RateLimitRemaining : {}/120".format(self.RateLimitRemaining) )
            return {'stat': -999, 'msg': "RateLimitRemaining "+str(self.RateLimitRemaining)+"/120", 'ids': []}

        if self.PendingUntil > time.time() :
            self._logger.info("Order pending : {:.1f}sec".format(self.PendingUntil-time.time()) )
            return {'stat': -999, 'msg': "Order pending : {:.1f}sec".format(self.PendingUntil-time.time()), 'ids': []}

        adjust_flag = kwargs.get('adjust_flag', False)

        # 対応しているオプションだけにする
        params={}
        for k,v in kwargs.items():
            if k in ['symbol','time_in_force','reduce_only']:
                params[k]=v

        params['symbol']=params.get('symbol', self.symbol)
        params['time_in_force']=params.get('time_in_force', "GoodTillCancel")
        params['order_type']=order_type.capitalize()
        params['side']=side.capitalize()
        params['qty']=self._round_order_size(size, symbol=params['symbol'])
        params['price']=self.round_order_price(price, symbol=params['symbol'])

        # ノートレード期間はポジ以上のクローズは行わない
        if self.noTrade :
            params['qty'] = round(min(abs(self.my.position.size),params['qty']))
            self._logger.info("No trade period (change order size to {})".format(params['qty']) )

        # 最低取引量のチェック（無駄なAPIを叩かないように事前にチェック）
        if params['qty']<self.minimum_order_size(symbol=params['symbol']) :
            return {'stat': -153, 'msg': '最低取引数量を満たしていません', 'ids': []}

        # クローズオーダー可能なポジションを探す
        positions = self.positions.find()
        available=abs(sum([float(p['free_qty']) for p in positions if p['symbol']==params['symbol'] and p['side']==('Sell' if side.lower()=='buy' else 'Buy')]))
        available = min( params['qty'], available )
        remain = round(max(params['qty']-available,0 ),8)

        ordered_id_list = []

        if available>=self.minimum_order_size(symbol=params['symbol']) :
            params['qty'] = available
            params['position_idx'] = 2 if side.lower()=='buy' else 1
            params['reduce_only'] = True
            params['close_on_trigger'] = True

            self._logger.debug("[send closeorder] : {}".format(params) )
            with self._order_dict_lock :
                try:
                    r = await self._client.post('/private/linear/order/create', data=params)
                    res = await r.json()
                    self._update_api_limit(res)
                except Exception as e:
                    self._logger.error(traceback.format_exc())
                    return {'stat': -999, 'msg': str(e), 'ids':[]}

                ret_code = res.get('ret_code',-999)

                if ret_code==0 : 
                    r = res.get('result')
                    if self.my and not adjust_flag:
                        self.my.order.new_order( symbol=params['symbol'], id=r['order_id'] , side=r['side'].upper(), price=r['price'], size=r['qty'], expire=time.time()+auto_cancel_after, invalidate=time.time()+2592000 )
                    ordered_id_list.append(r['order_id'])

                # 証拠金不足
                # {'ret_code': 30031, 'ret_msg': 'oc_diff[61255], new_oc[61255] with ob[0]+AB[2194]', 'ext_code': '', 'ext_info': '', 'result': None, 'time_now': '1615409661.288081', 'rate_limit_status': 99, 'rate_limit_reset_ms': 1615409661286, 'rate_limit': 100}
                elif ret_code==30031 : 
                    self.PendingUntil = max(self.PendingUntil, time.time()+60)  # 60秒注文保留
                else: 
                    self._logger.error("Send close order param : {}".format(params) )
                    self._logger.error("Error response [send closeorder] : {}".format(res) )
                    return {'stat':ret_code , 'msg':res.get('ret_msg'), 'ids':[]}

        # 決済だけでオーダー出し終わった場合
        if remain==0 :
            return {'stat':0 , 'msg':"", 'ids':ordered_id_list}

        # 新規オーダー
        params['qty'] = remain
        params['reduce_only'] = False
        params['close_on_trigger'] = False
        params.pop('position_idx', None)

        self._logger.debug("[sendorder] : {}".format(params) )
        with self._order_dict_lock :
            try:
                r = await self._client.post('/private/linear/order/create', data=params)
                res = await r.json()
                self._update_api_limit(res)
            except Exception as e:
                self._logger.error(traceback.format_exc())
                return {'stat': -999, 'msg': str(e), 'ids':[]}

            ret_code = res.get('ret_code',-999)

            if ret_code==0 : 
                r = res.get('result')
                if self.my and not adjust_flag:
                    self.my.order.new_order( symbol=params['symbol'], id=r['order_id'] , side=r['side'].upper(), price=r['price'], size=r['qty'], expire=time.time()+auto_cancel_after, invalidate=time.time()+2592000 )
                ordered_id_list.append(r['order_id'])
                return {'stat':0 , 'msg':"", 'ids':ordered_id_list}

            # 証拠金不足
            # {'ret_code': 30031, 'ret_msg': 'oc_diff[61255], new_oc[61255] with ob[0]+AB[2194]', 'ext_code': '', 'ext_info': '', 'result': None, 'time_now': '1615409661.288081', 'rate_limit_status': 99, 'rate_limit_reset_ms': 1615409661286, 'rate_limit': 100}
            elif ret_code==30031 : 
                self.PendingUntil = max(self.PendingUntil, time.time()+60)  # 60秒注文保留
                return {'stat':ret_code , 'msg':res.get('ret_msg'), 'ids':[]}
            else: 
                self._logger.error("Send order param : {}".format(params) )
                self._logger.error("Error response [sendorder] : {}".format(res) )
                return {'stat':ret_code , 'msg':res.get('ret_msg'), 'ids':[]}

    # キャンセル
    async def cancelorder(self, id, **kwargs ):
        """
        cancelorder( id                      # キャンセルする注文の ID です。
                   symbol="BTCUSD"           # 省略時には起動時に指定した symbol が選択されます
                  ) )

        return: { 'stat': エラーコード,      # キャンセル成功時 0
                  'msg':  エラーメッセージ,
                }
        """

        if id not in self.my.order.order_dict :
            self._logger.info("ID:{} is already filled or canceld or expired".format(id) )
            return {'stat':-999 , 'msg':"Order is already filled or canceld or expired"}

        if self.RateLimitRemaining<10:
            self._logger.info("RateLimitRemaining : {}/120".format(self.RateLimitRemaining) )
            return {'stat': -999, 'msg': "RateLimitRemaining "+str(self.RateLimitRemaining)+"/120", 'ids': []}

        kwargs['order_id']=id
        kwargs['symbol'] = kwargs.get("symbol", self.my.order.order_dict[id]['symbol'] )

        self._logger.info("[cancelorder] : {}".format(kwargs) )

        self.my.order.mark_as_invalidate( id )

        try:
            r = await self._client.post('/private/linear/order/cancel', data=kwargs)
            res = await r.json()
            self._update_api_limit(res)
        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

        ret_code = res.get('ret_code',-999)

        if ret_code==0 : 
            return {'stat': 0, 'msg': ""}

        if ret_code==20001 : # 'ret_msg': 'order not exists or too late to cancel'
            self.my.order.remove_order( id )

        self._logger.error("Error response [cancelorder] : {}".format(res) )
        return {'stat':ret_code , 'msg':res.get('msg')}

    # 変更
    async def changeorder(self, id, price, *kwargs ):
        try:
            params = {'order_id':id, 'symbol':self.symbol, 'p_r_price':self.round_order_price(price)}
            self._logger.info("[changeorder] : {}".format(params) )
            r = await self._client.post('/private/linear/order/replace', data=params)
            res = await r.json()
            self._update_api_limit(res)
        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

        return {'stat':res.get('ret_code',-1) , 'msg':res.get('msg')}