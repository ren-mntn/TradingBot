# coding: utf-8
#!/usr/bin/python3

import asyncio
from collections import deque
import aiohttp
import traceback

# Discordへの送信を行うクラス
class NotifyDiscord(object):
    def __init__(self, logger, webhook=''):
        self._logger = logger
        self.webhook = webhook
        self._message = deque(maxlen=100)

    async def _send_async(self, message, image_file=None, webhook=None):
        try:
            url = webhook or self.webhook
            if url != '':
                payload = {'content': ' {} '.format(message)}
                async with aiohttp.ClientSession() as session:
                    if image_file :
                        with open(image_file, 'rb') as image:
                            payload['key']=image
                            s = await session.post(url, data=payload)
                            s.close() # to resolve "Unclosed connection" warning
                    else:
                        s = await session.post(url, data=payload)
                        s.close() # to resolve "Unclosed connection" warning
        except Exception as e:
            self._logger.error('Failed sending to Discord: {}'.format(e))
            self._logger.info(traceback.format_exc())

    async def _flush_message(self):
        buff = ""
        while len(self._message)!=0 :
            message = self._message.popleft()
            if len(buff)+len(message)>1900 :
                await self._send_async(buff)
                buff = message
            else:
                buff = buff + '\n' + message
        if len(buff)!=0:
            await self._send_async(buff)

    def send(self, message, image_file=None, webhook=None):
        asyncio.create_task( self._send_async(message,image_file,webhook), name="discord_send" )


    def add_message(self, msg):
        self._message.append(msg)
        if len(self._message)>1900 :
            self.flush_message()

    def flush_message(self):
        asyncio.create_task( self._flush_message(), name="discord_flush" )

