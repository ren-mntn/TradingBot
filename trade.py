# coding: utf-8
#!/usr/bin/python3

import asyncio
import os
import signal
import sys
import time
from threading import Thread
import traceback
from libs.main import Trade

# Discordライブラリの初期化（もしインストールされていれば）
try:
    import discord
    discord_client = discord.Client()
except:
    discord_client = None

try:
    @discord_client.event
    async def on_ready():

        # ^Cが押されたときのハンドラを再登録
        signal.signal(signal.SIGINT, stop)
        try:
            broker._logger.info('Logged to Discord in as {}'.format(discord_client.user.name))
        except Exception as e:
            broker._logger.exception("Error in on_ready routine : {}, {}".format(e, traceback.print_exc()))
except:
    pass

# メッセージとリアクションのハンドラ
try:
    @discord_client.event
    async def on_message(message):
        try:
            # 自分自身の書き込みには反応させない
            if message.author == discord_client.user:
                return
            if hasattr(broker,'_strategy_class') and hasattr(broker._strategy_class,'discord_on_message') :
                await broker._strategy_class.discord_on_message(message)
        except Exception as e:
            broker._logger.exception("Error in on_message routine : {}, {}".format(e, traceback.print_exc()))

    @discord_client.event
    async def on_reaction_add(reaction, user):
        try:
            # 自分自身の書き込みには反応させない
            if user == discord_client.user:
                return
            if hasattr(broker,'_strategy_class') and hasattr(broker._strategy_class,'discord_on_reaction_add') :
                await broker._strategy_class.discord_on_reaction_add(reaction, user)
        except Exception as e:
            broker._logger.exception("Error in on_reaction_add routine : {}, {}".format(e, traceback.print_exc()))
except:
    pass

def bot_main_loop(broker, args):
    try:
        asyncio.run(broker.start(args))
    except RuntimeError :
        pass

def stop(signal=signal.SIGINT, frame=0):
    if broker_thread.is_alive() :
        broker.keyboard_interrupt.set()
        timeout=100
        while broker.keyboard_interrupt.is_set():
            time.sleep(1)
            timeout -= 1
            if timeout<=90 :
                print( "wait" )
            if timeout<=0 :
                break
    if discord_client and discord_token and discord_token!='':
        asyncio.create_task( discord_client.close() )
    os._exit(0)

if __name__ == "__main__":
    # python trade.py
    if len(sys.argv) == 1:
        args={}

    # python trade.py trade.yaml
    elif len(sys.argv) == 2:
        args={'trade_yaml': sys.argv[1]}

    # python trade.py strategy/strategy.py strategy/strategy.yaml
    elif len(sys.argv) == 3:
        args={'strategy_py': sys.argv[1], 'strategy_yaml': sys.argv[2], }

    # python trade.py trade.yaml strategy/strategy.py strategy/strategy.yaml
    elif len(sys.argv) == 4:
        args={'trade_yaml': sys.argv[1], 'strategy_py': sys.argv[2], 'strategy_yaml': sys.argv[3], }

    # ^Cが押されたときのハンドラを再登録
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    # メイン部初期化
    broker = Trade()
    broker_thread = Thread(target=bot_main_loop, args=(broker, args), daemon=True)
    broker_thread.start()

    # メイン部初期化待ち
    while not hasattr(broker,'strategy_yaml') :
        time.sleep(1)

    discord_token = broker.strategy_yaml.params.get('discord_bot_token')
    try:
        if discord_client and discord_token and discord_token!='':
            broker._logger.info('Start Discord bot')
            discord_client.run(discord_token)
            broker._logger.info('Stop Discord bot')
        else:
            while True:
                time.sleep(100)
    except RuntimeError :
        pass
