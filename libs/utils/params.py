# coding: utf-8
#!/usr/bin/python3

import asyncio
import os
import time
import traceback
import yaml

# パラメータの動的読み込みをサポートするクラス
class DynamicParams(object):
    def __init__(self, logger, filename, callback=None):
        self._logger = logger
        self._filename = filename
        self._callback = callback

        # yaml ファイルの読み込み
        self.params = yaml.safe_load(open(self._filename, 'r', encoding='utf-8_sig'))

    def start(self):
        self.load_param()
        asyncio.create_task(self._check_update(), name="params_update_check")

    async def _check_update(self):
        while True:
            if self.params.get('created_at',0) != os.path.getmtime(self._filename):
                if self.load_param() and self._callback!=None:
                    self._callback()
            await asyncio.sleep(1)

    def _dict_diff(self, name, old, new):
        diff = []
        for new_key, new_val in new.items():
            if new_key not in old:
                if type(new_val)==dict :
                    diff += self._dict_diff( name + " / " + new_key, {}, new_val)
                else:
                    diff.append( ("Parameter [{} / {}]", name, new_key, None, new_val) )
            elif new_val != old[new_key] :
                if type(new_val)==dict and type(old[new_key])==dict:
                    diff += self._dict_diff( name + " / " + new_key, old[new_key], new_val)
                else:
                    diff.append( ("Changed [{} / {}]", name, new_key, old[new_key], new_val ) )
        for old_key, old_val in old.items():
            if old_key!='created_at' and old_key not in new :
                diff.append( ("Deleted [{} / {}]", name, old_key, old_val, None ) )
        return diff


    def load_param(self):

        if 'created_at' not in self.params :
            self.params.clear()

        base_filename = os.path.basename(self._filename)
        try:
            tmp_params = yaml.safe_load(open(self._filename, 'r', encoding='utf-8_sig'))
        except Exception as e:
            self._logger.error("Error occured at load_param: {}".format(e))
            self._logger.info(traceback.format_exc())
            return False

        diff = self._dict_diff(base_filename, self.params, tmp_params)
        for d in diff:
            if d[2]=='apikey' or d[2]=='secret' or d[2]=='passphase' or 'discord_bot_token' in d[2]:
                continue
            if d[3]==None:
                self._changed("{:<50} : {}".format(d[0].format(d[1],d[2]), d[4]))
            elif d[4]==None:
                self._changed("{:<50} : ({}) -> none".format(d[0].format(d[1],d[2]), d[3]))
            else:
                self._changed("{:<50} : {} -> {}".format(d[0].format(d[1],d[2]), d[3], d[4]))

        self.params = tmp_params
        self.params['created_at'] = os.path.getmtime(self._filename)

        self._logger.discord.flush_message()

        return len(diff)!=0

    def _changed(self,msg):
        if 'https' not in msg and 'candle_server' not in msg:
            m = msg
            self._logger.discord.add_message(m)
        else:
            l = msg.split(':')
            if len(l)>1:
                m = l[0] + ': ' + '*'*(len(l[1])-1)
        self._logger.info(m)
