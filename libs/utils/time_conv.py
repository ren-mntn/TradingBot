# coding: utf-8
#!/usr/bin/python3

from datetime import datetime,timedelta,timezone
from dateutil import parser

# 時刻関連変換のクラス
class TimeConv(object):
    def _epoc_to_dt(self, timestamp):
        return (datetime.utcfromtimestamp(timestamp) + timedelta(hours=9)).replace(tzinfo=timezone(timedelta(hours=9),'JST'))

    def _utcstr_to_dt(self, date_line):
        try:
            exec_date = date_line.replace('T', ' ')[:-1]
            exec_date = exec_date + '00000000'
            d = datetime(int(exec_date[0:4]), int(exec_date[5:7]), int(exec_date[8:10]),
                        int(exec_date[11:13]), int(exec_date[14:16]), int(exec_date[17:19]), int(exec_date[20:26]),
                         tzinfo=timezone(timedelta(hours=9), 'JST')) + timedelta(hours=9)
        except Exception as e:
            self._logger.error("Error while parsing date str : exec_date:{}  {}".format(exec_date, e))
            d = (parser.parse(date_line) + timedelta(hours=9)).replace(tzinfo=timezone(timedelta(hours=9),'JST'))
        return d

    # 稼働サーバーのタイムゾーンが何になっていてもJSTの時刻を生成
    def _jst_now_dt(self):
        return (datetime.utcnow() + timedelta(hours=9)).replace(tzinfo=timezone(timedelta(hours=9),'JST'))





# テストコード
if __name__ == "__main__":

    timeconv = TimeConv()
    print( timeconv._epoc_to_dt(1613695833.307) )
    print( timeconv._utcstr_to_dt("2021-02-19T00:50:33.324Z") )
    print( timeconv._utcstr_to_dt("2021-02-19T00:50:32.388204+00:00") )
    print( timeconv._jst_now_dt() )


