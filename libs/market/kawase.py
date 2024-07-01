# coding: utf-8
#!/usr/bin/python3

import requests
import time
from libs.utils.scheduler import Scheduler

try:
    from .update import market_info
except ImportError:
    from update import market_info


class GaitameUSDJPY(market_info):

    def __init__(self, logger, interval=30, keepupdate=False):

        self.__table = {'name': 'USDJPY', 'price':0, 'update_time':time.time()-100, 'update_handler': self.__read}

        self._logger = logger
        super().__init__(logger, interval)

        self.__read()

        self.__keepupdate = keepupdate
        if keepupdate :
            Scheduler(self._logger, interval=1, callback=self._update, args=(self.__table,))

    def __read(self):
        # Get JPYUSD from Gaitame-online
        res = requests.get('https://www.gaitameonline.com/rateaj/getrate').json()
        for q in res['quotes']:
            if (q['currencyPairCode'] == 'USDJPY'):
                self.__table['price'] = (float(q['bid']) + float(q['ask'])) / 2.0
                self.__table['update_time'] = time.time()
                break
        self._logger.debug( "update USDJPY={:.1f}".format(self.__table['price']) )

    @property
    async def price(self):
        if self.__keepupdate :
            while self.__table['price']==0:
                time.sleep(1)
            return self.__table['price']
        else:
            return await self._get_price(self.__table)




# テストコード
if __name__ == "__main__":
    from logging import getLogger, StreamHandler, INFO

    logger = getLogger(__name__)
    handler = StreamHandler()
    handler.setLevel(INFO)
    logger.setLevel(INFO)
    logger.addHandler(handler)

    kawase = usdjpy(logger, interval=30, keepupdate=True)

    while True:
        try:
            logger.info( kawase.price )
            time.sleep(1)

        except KeyboardInterrupt:
            kawase.stop()
            break