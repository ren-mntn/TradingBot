# coding: utf-8
#!/usr/bin/python3

from threading import Thread
from socket import socket, AF_INET, SOCK_DGRAM, SOCK_STREAM

# ポジションサーバーに接続して損益を送るクラス
class PositionClient(object):
    def __init__(self, logger, exchange, strategy, pos_server=None):
        if not pos_server:
            return

        self._logger = logger
        self._pos_server = pos_server
        self._exchange = exchange
        self._strategy=strategy
        if not hasattr( self._logger, 'get_size_handler' ) :
            self._logger.get_size_handler = self.get_size
        if not hasattr( self._logger, 'get_raw_size_handler' ) :
            self._logger.get_raw_size_handler = self.get_raw_size
        if not hasattr( self._logger, 'get_profit_handler' ) :
            self._logger.get_profit_handler = self.get_profit

        # ポジションをUDPで送信するスレッドを起動
        position_thread = Thread(target=self.send_position)
        position_thread.daemon = True
        position_thread.start()

    def send_position(self):
        self._logger.info("Start position thread (connected to {})".format(self._pos_server) )
        self.socket = socket(AF_INET, SOCK_DGRAM)
        while self._logger.running:
            self._exchange.my.update.wait(60)
            self._exchange.my.update.clear()
            message = "{:>10} : {:>15.8f} : {:>15.8f} :{:>11g}: {:>3} : {:>3} : {} : {} : {} : {:>15.8f}".format(
                self._exchange.symbol[:10],
                self._logger.get_size_handler(),
                self._exchange.my.position.base_position,
                self._logger.get_profit_handler(),
                self._exchange.api_remain1 if hasattr(self._exchange,'api_remain1') else 300 ,
                self._exchange.api_remain2 if hasattr(self._exchange,'api_remain2') else 300 ,
                self._strategy,
                self._exchange.units()['profit_currency'],
                self._exchange.units()['unitrate'],
                self._logger.get_raw_size_handler(),
                )
            self.socket.sendto(message.encode('utf-8'), (self._pos_server[0], self._pos_server[1]))

    def get_size(self):
        # ポジションを currency 単位に換算して送る
        return round(self._exchange.my.position.size*self._exchange.units()['pos_rate'],8)

    def get_raw_size(self):
        # ポジションを そのまま送る
        return self._exchange.my.position.size

    def get_profit(self):
        # 損益を fiat 単位に換算して送る
        return self._exchange.my.position.profit
