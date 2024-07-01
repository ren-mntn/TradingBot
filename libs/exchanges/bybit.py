# coding: utf-8
#!/usr/bin/python3

from pybotters.store import DataStore

from libs.utils import TimeConv
from libs.market import *
from libs.exchanges.base_module import RestAPIExchange, WebsocketExchange

class Kline(DataStore):
    _KEYS = ['symbol', 'interval', 'timestamp']
    _MAXLEN = 3000000

    def _onmessage(self, message):
        symbol= message.get('symbol')
        interval= message.get('interval')
        self._insert([{'symbol':symbol, 'interval':interval,
                       'timestamp':int(item[0]/1000), 'open':float(item[1]), 'high':float(item[2]), 'low':float(item[3]), 'close':float(item[4]), 'volume':item[5], 'turnover':float(item[6])}
                       for item in message.get('kline',[])])

# インバースタイプ契約とリニアタイプ契約の共通部分
class Bybit(TimeConv, RestAPIExchange, WebsocketExchange):

    # APIリミットの管理
    def _update_api_limit(self,d):
        self.RateLimitRemaining = int(d.get('rate_limit_status',self.RateLimitRemaining))

    @property
    def api_remain1(self):
        return self.RateLimitRemaining

    @property
    def api_remain2(self):
        return 500

    @property
    def api_remain3(self):
        return 300

    # データストアへの接続を共通名に変換
    @property
    def ticker(self):
        return self._stores.get('instrument')

    @property
    def board(self):
        return self._stores.get('orderbook')

    @property
    def candle(self):
        return self._stores.get('kline')

    @property
    def positions(self):
        return self._stores.get('position')

    @property
    def orders(self):
        return self._stores.get('order')

    @property
    def account(self):
        return self._stores.get('wallet')

    # オーダー単位の管理
    async def _get_market_info(self):
        # マーケット情報の取得
        res = await self._client.get('/v2/public/symbols')
        data = await res.json()
        market_info = data.get('result',[])

        # シンボルごとの価格呼び値
        self._price_unit_dict = dict([(s['name'],float(s['price_filter']['tick_size'])) for s in market_info])

        # 最小取引単位
        self._minimum_order_size_dict = dict([(s['name'],float(s['lot_size_filter']['min_trading_qty'])) for s in market_info])

        # 証拠金通貨
        self._collateral_coin = dict([(s['name'],s['base_currency'] if s['name'][-3:]=='USD' else s['quote_currency']) for s in market_info])

        # オーダー通貨
        self._currency = dict([(s['name'], s['quote_currency'] if s['name'][-3:]=='USD' else s['base_currency']) for s in market_info])

    def minimum_order_size(self, symbol=None):
        return self._minimum_order_size_dict.get(symbol or self.symbol, 1)

    def round_order_price(self, price, symbol=None):
        price_unit = self._price_unit_dict.get((symbol or self.symbol),1)
        return round(round(price/price_unit)*price_unit, 8)

    def _round_order_size(self, value, symbol=None):
        size_unit = self.minimum_order_size(symbol)
        return round(round(value/size_unit)*size_unit, 8)

    def _subscribe(self, topic, handler):
        self._logger.debug( "subscribe : {}".format(topic) )
        self._handler[topic]=handler
        self._channels.append(topic)
        self._ws_args = [{"op": "subscribe", "args": self._channels}]
        return topic

    def _on_executions(self,msg):
        recept_data = msg.get('data')
        self.execution_info.time = self._epoc_to_dt(int(recept_data[-1]['trade_time_ms'])/1000)
        self.execution_info.append_latency(self._jst_now_dt().timestamp()*1000 - int(recept_data[-1]['trade_time_ms']))
        for i in recept_data:
            self.execution_info.append_execution(float(i['price']),float(i['size']),i['side'].upper(),self._epoc_to_dt(int(i['trade_time_ms'])/1000),i['trade_id'])
        self.my.position.ref_ltp = self.execution_info.last
        self.execution_info._event.set()

    def _on_ticker(self,msg):
        self.ticker_info.time = self._epoc_to_dt(int(msg['timestamp_e6'])/1000000)
        for d in msg.get('data',{'update':{}}).get('update',[]):
            self.ticker_info.last = float(d.get('last_price_e4',self.ticker_info.last*10000))/10000
            self.ticker_info.open_interest = d.get('open_interest',self.ticker_info.open_interest)
        self.my.position.ref_ltp = self.ticker_info.last

    def create_candle_class(self,id):
        self.create(id, datastore_class=Kline)

    # wsでローソク足を受信したら登録されたターゲット足を生成
    async def _wait_candle_event(self):
        while self._logger.running:
            await self.candle.wait()
            with self._datastore_lock :
                self.candle_store.resample()

            # イベントが都度発生しないようにまとめてローソク足のデータストアへ入れる
            for c in self.candle_store._target_candle_list:
                pool = c.pop('pool')
                c['pool']=[]
                for p in pool:
                    c['target_candle']._onmessage(p)

    # 証拠金残高
    async def getcollateral(self, coin=None):

        # 取引するシンボルに使用する証拠金通貨 (リストにない物はUSDT)
        target_coin = self._collateral_coin.get(self.symbol,'USDT')

        try:
            res = await self.getbalance(coin or target_coin)
            if res.get('ret_code',-1)!=0 :
                return {'stat': res.get('ret_code',-1), 'msg': res}
            return {'stat': 0, 'collateral': res['result'][coin or target_coin]['equity'], 'msg': res}
        except Exception as e:
            self._logger.error(traceback.format_exc())
            return {'stat': -999, 'msg': str(e)}

