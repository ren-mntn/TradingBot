# coding: utf-8
#!/usr/bin/python3

import json
import os

# json形式でログフォルダに保存・レストアを行うクラス
class JsonFile(object):
    def __init__(self, logger):
        self._logger = logger
        self.filename = ''

    def reload_file(self, filename=None):
        self.filename = filename or self.filename
        datas = []
        if os.path.isfile(self.filename) :
            with open(self.filename, 'r') as fp:
                for p in fp:
                    try :
                        if not p.startswith('{') :
                            continue
                        data = json.loads(p)
                        datas.append(data)
                    except Exception as e:
                        self._logger.error("JSON file error : {}[{}]  : {}".format(self.filename,p,e))
        return datas

    def add_data(self, data):
        if self.filename != '' :
            with open(self.filename, 'a') as fp:
                fp.write(json.dumps(data)+'\n')

    def renew_file(self, datas):
        dumped = [json.dumps(p) for p in datas]
        with open(self.filename, 'w') as fp:
            fp.write('\n'.join(dumped))
            fp.write('\n\n')
