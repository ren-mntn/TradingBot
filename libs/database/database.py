# coding: utf-8
#!/usr/bin/python3

import traceback

# InfluxDBへ接続してレコードを書き出すクラス
class database:
    def __init__(self, logger, host='', port=8086, database=''):

        self._logger = logger
        self.__last_value = {}
        
        # Influx DBとの接続（もしインストールされていれば）
        try:
            from influxdb import InfluxDBClient
            import urllib3
            from urllib3.exceptions import InsecureRequestWarning
            urllib3.disable_warnings(InsecureRequestWarning)
        except Exception as e:
            self._logger.exception("Influxdb module import error : {}, {}".format(e, traceback.print_exc()))

        if host!='' and database!='' :
            try:
                self.__client = InfluxDBClient(host=host, port=port, database=database, timeout=5)
                self.__client.query('show measurements')  # 接続テスト
            except Exception as e:
                self._logger.error("Influxdb connection error : {}, {}".format(e, traceback.print_exc()))
                self.__client = None
        else:
            self._logger.info("Skip connecting for Influxdb")
            self.__client = None


    def write( self, measurement, tags='', **kwargs ):
        try:
            if tags=='':
                data = [{"measurement": measurement, "fields": kwargs}]
            else:
                data = [{"measurement": measurement, "tags": tags, "fields": kwargs}]

            if self.__client != None :
                self.__client.write_points(data)
#            else:
#                self._logger.info( data )

        except Exception as e:
            self._logger.exception("Influxdb write error : {}, {}".format(e, traceback.print_exc()))
            return ""

        return kwargs




from logging import getLogger, StreamHandler, INFO
if __name__ == "__main__":

    logger = getLogger(__name__)
    handler = StreamHandler()
    handler.setLevel(INFO)
    logger.setLevel(INFO)
    logger.addHandler(handler)

    db = database(logger=logger, host='localhost', port=8086, database='bots')
#    db = database(logger=logger)

    db.write( measurement="test", binance_mid=100000, bitflyer_mid=100000 )
