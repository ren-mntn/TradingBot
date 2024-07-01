# coding: utf-8
#!/usr/bin/python3

import asyncio
import os
from threading import Event
import time


# それぞれのメンバ・関数は、親クラス(backtestまたはtrade)を呼び出す

class Strategy:

    _exec_que_list = []

    def __init__(self, logger, exchange, symbol, strategy_yaml):
        self._logger = logger
        self.exchange = exchange
        self._symbol = symbol
        self.strategy_yaml_filename = strategy_yaml

    def set_parameters(self, trade_param, strategy_param):
        self._trade_param = trade_param
        self._strategy_param = strategy_param
        self.parameters = self._strategy_param['parameters']

    def ExchangeWebsocket(self, exchange, logger, **kwargs):
        from libs.utils.min_candle import CandleCollector

        if exchange.lower()== 'bitflyer' :
            from libs.exchanges.bitflyer import Bitflyer
            ex = Bitflyer(logger, candle_store=CandleCollector(self._logger), **kwargs)

        elif exchange.lower() == 'gmo' :
            from libs.exchanges.gmo import GMOCoin
            ex = GMOCoin(logger, candle_store=CandleCollector(self._logger), **kwargs)

        elif exchange.lower() == 'phemex' :
            from libs.exchanges.phemex import Phemex
            ex = Phemex(logger, candle_store=CandleCollector(self._logger), **kwargs)

        elif exchange.lower() == 'bitget' :
            from libs.exchanges.bitget import Bitget
            ex = Bitget(logger, candle_store=CandleCollector(self._logger), **kwargs)

        elif exchange.lower() == 'bybit' :
            from libs.exchanges.bybit_inverse import BybitInverse
            ex = BybitInverse(logger, candle_store=CandleCollector(self._logger), **kwargs)

        elif exchange.lower() == 'binance' :
            from libs.exchanges.binance import Binance
            ex = Binance(logger, candle_store=CandleCollector(self._logger), **kwargs)

        elif exchange.lower() == 'bitmex' :
            from libs.exchanges.bitmex import Bitmex
            ex = Bitmex(logger, candle_store=CandleCollector(self._logger), **kwargs)

        elif exchange.lower() == 'okx' :
            from libs.exchanges.okx import Okx
            ex = Okx(logger, candle_store=CandleCollector(self._logger), **kwargs)

        elif exchange.lower() == 'coinbase' :
            from libs.exchanges.coinbase import Coinbase
            ex = Coinbase(logger, candle_store=CandleCollector(self._logger), **kwargs)

        else:
            raise ValueError(f"{exchange} is not support yet")
            return None

        return ex


    def Scheduler(self, interval=1, basetime=None, callback=None, args=()):
        from libs.utils.scheduler import Scheduler
        return Scheduler(self._logger, interval=interval, basetime=basetime, callback=callback, args=args)

    def ExecutionQueue(self, callback, args=(), exchange=None):
        from collections import deque
        exec_que = deque(maxlen=10000)
        self._exec_que_list.append([exchange,exec_que])
        (exchange or self.exchange).execution_info.add_handler( exec_que=exec_que, handler=callback, args=args )
        return exec_que

    def AddBoardUpdateHandler(self, callback, args=(), exchange=None):
        (exchange or self.exchange).board_info.add_handler(callback, args)

    def AddExecutionHandler(self, callback, args=(), exchange=None):
        (exchange or self.exchange).my.order.add_handler(callback, args)

    def AddDisconnectedHandler(self, callback, args=(), exchange=None):
        (exchange or self.exchange).add_disconnect_handler(callback, args)

    def APICandle(self, timeframe, num_of_candle=500, callback=None, exchange=None, symbol=None, args=()):
        target_exchange = (exchange or self.exchange)
        return target_exchange.candle_store.CreateCandleCollectTask(exchange=target_exchange, symbol=(symbol or self._symbol), timeframe=timeframe, num_of_candle=num_of_candle, callback=callback, args=args)

    def CandleGenerator(self, timescale, num_of_candle=500, update_current=False, callback=None, exchange=None, args=()):
        from libs.utils.sec_candle import CandleGenerator
        return CandleGenerator(self._logger, (exchange or self.exchange), timescale=timescale, num_of_candle=num_of_candle, update_current=update_current, callback=callback, args=args,
                               server=(self._trade_param.get('candle_server'),self._trade_param.get('candle_server_user'),self._trade_param.get('candle_server_pass')) )


    async def sendorder(self, order_type, side, size, **kwargs):
        if self.exchange.ws.connected:
            return await self.exchange.sendorder(order_type=order_type, side=side, size=round(size,8),
                                **dict(self._strategy_param.get('order',{}).get('option',{}), **kwargs))
        else:
            self._logger.error( "Private websocket is not connected!" )
            return {'stat': -999, 'msg': "Private websocket is not connected!", 'ids': []}

    async def cancelorder(self, id, **kwargs):
        if self.exchange.ws.connected:
            return await self.exchange.cancelorder(id=id, **kwargs)
        else:
            self._logger.error( "Private websocket is not connected!" )
            return {'stat': -999, 'msg': "Private websocket is not connected!", 'ids': []}

    async def close_position(self):
        if self.current_pos >= self.minimum_order_size:
            res = await self.sendorder(order_type='MARKET', side='SELL', size=self.current_pos)
            if res.get('ids') :
                self._logger.info('        Emergency SELL!!! ({})'.format(res.get('ids')))
                return True
            self._logger.info('        Close Position Error ({})'.format(res))

        elif self.current_pos <= -self.minimum_order_size:
            res = await self.sendorder(order_type='MARKET', side='BUY', size=-self.current_pos)
            if res.get('ids') :
                self._logger.info('        Emergency BUY!!! ({})'.format(res.get('ids')))
                return True
            self._logger.info('        Close Position Error ({})'.format(res))
        return False

    async def getcollateral_api(self, coin=None):
        return await self.exchange.getcollateral(coin)

    @property
    def collateral_rate(self):
        return self.exchange.units()['unitrate']

    def get_size_group(self, splitsize, limitprice=1000000, limitnum=5, startprice=0):
        return self.exchange.board_info.get_size_group(splitsize=splitsize, limitprice=limitprice, limitnum=limitnum, startprice=startprice)

    def get_price_group(self, splitprice):
        return self.exchange.board_info.get_price_group(splitprice=splitprice)

    @property
    def symbol(self):
        return self.exchange.symbol

    @property
    def current_pos(self):
        return round(self.exchange.my.position.size, 8)

    @property
    def ltp(self):
        return self.exchange.execution_info.last

    @property
    def best_ask(self):
        return self.exchange.execution_info.best_ask

    @property
    def best_bid(self):
        return self.exchange.execution_info.best_bid

    @property
    def server_latency(self):
        return self.exchange.execution_info.avg_latency_1s

    @property
    def asks(self):
        return self.exchange.board_info.asks

    @property
    def bids(self):
        return self.exchange.board_info.bids

    @property
    def mid_price(self):
        return self.exchange.board_info.mid

    @property
    def ordered_list(self):
        return self.exchange.my.order.list

    @property
    def current_average(self):
        return self.exchange.my.position.average_price

    @property
    def current_profit(self):
        return round(self.exchange.my.position.realized+self.exchange.my.position.unreal+self.exchange.my.position.commission,8)

    @property
    def current_fixed_profit(self):
        return round(self.exchange.my.position.realized+self.exchange.my.position.commission,8)

    @property
    def current_profit_unreal(self):
        return self.exchange.my.position.unreal

    @property
    def commission(self):
        return self.exchange.my.position.commission

    @property
    def ltp(self):
        return self.exchange.execution_info.last

    @property
    def api_remain1(self):
        return self.exchange.api_remain1

    @property
    def api_remain2(self):
        return self.exchange.api_remain2

    @property
    def api_remain3(self):
        return self.exchange.api_remain3

    @property
    def minimum_order_size(self):
        return self.exchange.minimum_order_size()

    @property
    def sfd(self):
        return self.exchange.sfd if hasattr(self.exchange,'sfd') else 0

    @property
    def executed_history(self):
        return self.exchange.my.order.executed_list

    @property
    def canceled_history(self):
        return self.exchange.my.order.canceled_list

    def get_historical_counter(self, sec):
        return self.exchange.my.order.historical_counter(sec)

    @property
    def log_folder(self):
        return self._strategy_param['logging']['folder']

    def send_discord(self, message, image_file=None, webhook=None ):
        return self._logger.discord.send( message=message, image_file=image_file, webhook=webhook)

    @property
    def no_trade_period(self):
        return self.exchange.noTrade

    async def exit(self):
        self._logger.running = False
        try:
            await asyncio.wait_for(self._logger.stop(), timeout=10)
        except asyncio.TimeoutError:
            pass
        os._exit(0)


