'''
//      #####   Sample_Server.py   #####
//
// Independent RTD generator server for WsRtd AmiBroker Data Plugin ( TESTING / DEMO )
//
// Python Program that runs as a Fake data generator Server.
// WsRTD data plugin connects to this server via specified IP:Port
//
// This program is NOT meant for PRODUCTION USE. IT is just a tester script.
//
///////////////////////////////////////////////////////////////////////
// Author: NSM51
// https://github.com/ideepcoder/Rtd_Ws_AB_plugin/
// https://forum.amibroker.com/u/nsm51/summary
//
// Users and possessors of this source code are hereby granted a nonexclusive,
// royalty-free copyright license to use this code in individual and commercial software.
//
// AUTHOR ( NSM51 ) MAKES NO REPRESENTATION ABOUT THE SUITABILITY OF THIS SOURCE CODE FOR ANY PURPOSE.
// IT IS PROVIDED "AS IS" WITHOUT EXPRESS OR IMPLIED WARRANTY OF ANY KIND.
// AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOURCE CODE,
// INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
// IN NO EVENT SHALL AUTHOR BE LIABLE FOR ANY SPECIAL, INDIRECT, INCIDENTAL, OR
// CONSEQUENTIAL DAMAGES, OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS,
// WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION,
// ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOURCE CODE.
//
// Any use of this source code must include the above notice,
// in the user documentation and internal comments to the code.
'''

import asyncio
import time
import websockets
import json
import sys
import math
import copy
import os
import ccxt.pro as ccxtpro
import logging
import datetime
import pytz
from AmiData import DataManager
import re


data_man = DataManager()
class PubSub:
    def __init__(self):
        self.waiter = asyncio.Future()
        self.stop_threads = False    ## global flag to send term signal

    def publish(self, value):
        waiter, self.waiter = self.waiter, asyncio.Future()
        waiter.set_result((value, self.waiter))

    async def subscribe(self):
        waiter = self.waiter
        while not self.stop_threads:
            value, waiter = await waiter
            yield value

    def __del__(self):
        return

    __aiter__ = subscribe

class RTDServer(PubSub):
    def __init__(self):
        super().__init__()

        try:
            sleep_time = float(sys.argv[1])
            print(f'Frequency={sleep_time} secs')
        except:
            sleep_time = 0.9
            print(f'Frequency={sleep_time} secs')

        ''' Settings '''
        self.DataFiles_dir = data_man.DataFiles_dir
        self.timeframe       = 5             ## base time interval in min (periodicity)
        self.websocket_port  = 10102         ## Websocket port  10102
        self.sleep_time      = sleep_time    ## simulate ticks generated every "n" seconds. SET IN MAIN()
        self.ticker_count    = 10            ## incl default 3 tickers, increase max_tickers accordingly
        self.inc_sym          = 10           ## set greater than 0, to simulate new quotes by this amount. IF 0, no new tickers added
        self.max_tickers        = 1000       ## maximum concurrent ticks Try 1000 :) no problem

        self.lib = data_man.lib
        self.qb = data_man.qb

        self.keep_running     = True   ## True=Server remains running on client disconnect, False=Server stops on client disconnect
        self.initial_sym_add  = False


        self.websocket_subscription_list = []        ## simulate add and remove symbol command
        self.data_list = []
        self.queue = asyncio.Queue()

        self.backfill_size_allowance = 50000
        self.backfill_paginate_gap = 4

        self.timezone = pytz.timezone('Australia/Perth')

        self.amibroker_path = "C:\\Program Files\\AmiBroker\\"
        self.databases_path = os.path.join(self.amibroker_path,"Databases")
        self.database = "MasterData"
        self.historic_data_path_root = os.path.join(self.amibroker_path,"DataFiles")
        self.watchlists_path = os.path.join(self.databases_path,self.database,"WatchLists")
        self.watchlist_suffix = ".tls"
        self.watchlist = []

        self.exchange_instances = {
            "binance": ccxtpro.binance(),
            "bybit": ccxtpro.bybit(),
            "coinbase": ccxtpro.coinbase(),
            "okx": ccxtpro.okx(),
            "mexc":ccxtpro.mexc(),
            "kucoin":ccxtpro.kucoin(),
            "gateio":ccxtpro.gateio(),
            "kraken":ccxtpro.kraken()
        }

    async def recv(self, websocket ):
                try:
                    while( not self.stop_threads ):
                        try:
                            async with asyncio.timeout(300):
                                message = await websocket.recv()
                                try:
                                    json_message = json.loads(message)
                                    print(json_message)
                                    if 'cmd' in json_message:
                                        if "bfsym" in json_message['cmd']:
                                            if "arg" in json_message:
                                                if ' ' in json_message['arg']:
                                                    try:
                                                        command = json_message['arg'].split()
                                                        try:
                                                            backfill_length = int(command[2])
                                                            if backfill_length == 1:
                                                                print(json_message['arg'])
                                                                await self.get_historic_data_from_fetch_api(json_message)
                                                                await self.add_symbol(json_message)
                                                            elif backfill_length == 5:
                                                                print(json_message['arg'])
                                                                symbol_and_exchange = command[1]
                                                                await self.backfill_full_check_length(symbol_and_exchange)
                                                        except Exception as e:
                                                            print(f"Exception in bfsym inner: {e}")
                                                    except Exception as e:
                                                        print(f"Exception in bfsym outer: {e}")
                                        elif "bfall" in json_message['cmd']:
                                            try:
                                                print(json_message['arg'])
                                                if "arg" in json_message:
                                                    if json_message['arg']=='x':
                                                        tasks = [self.backfill_full_check_length(symbol_and_exchange) for symbol_and_exchange in self.watchlist]
                                                        await asyncio.gather(*tasks)
                                            except Exception as e:
                                                print("Exception bfall:", e)

                                        elif "bffull" in json_message['cmd']:
                                            try:
                                                print(json_message['arg'])
                                                symbol_and_exchange = json_message['arg']
                                                await self.backfill_full_check_length(symbol_and_exchange)
                                            except Exception as e:
                                                print("Exception bffull:", e)


                                        elif "bfauto" in json_message['cmd']:
                                            try:
                                                print(json_message['arg'])
                                                message = json_message['arg']
                                                symbol_and_exchange, limit = self.define_symbol_and_limit(message)
                                                await self.backfill_symbols_from_arctic_db_from_end(symbol_and_exchange, limit = limit)
                                                await self.get_historic_data_from_fetch_api(json_message)
                                            except Exception as e:
                                                print("Exception auto:", e)


                                        elif "addsym" in json_message['cmd']:
                                            await self.add_symbol(json_message)

                                        elif "remsym" in json_message['cmd']:
                                            await self.rem_symbol(json_message)

                                        else:
                                            print("Unknown command in message")

                                    else:
                                        print( f"json_message={message}")
                                    await asyncio.sleep(self.sleep_time)

                                except ValueError as e:
                                    print(f"Value error from {message}\n{e}")

                            if self.stop_threads:
                                raise websockets.ConnectionClosed( None, None )

                        except TimeoutError: pass

                except websockets.ConnectionClosed as wc:
                    print(f"Connection closed: {wc}")
                    if not self.keep_running:
                        self.stop_threads = True
                except Exception as e:
                    return repr(e)
                return

    def check_if_initilising_symbols(self):
        initialising = input("Initilising symbols? (Yes/No):")
        if "y" in initialising.lower():
            print("When client connects, symbols will be added to plugin ready for retrieval")
            self.initial_sym_add = True
        else:
            print("No new symbols will be added")

    async def add_symbols_from_arctic_db_to_watchlist(self):
        print("Adding symbols from ArcticDB to watchlist")
        list = self.lib.list_symbols()
        for item in list:
            if ("USDT.binance" in item) or ("USDC.binance" in item) or ("FDUSD.binance" in item):
                self.watchlist.append(item)

    async def add_symbol(self, json_message):
        try:
            json_copy = copy.deepcopy(json_message)
            symbol_and_exchange = None
            """
            The regex pattern below ([A-Z]+-[A-Z]+\.\w+) looks for:
            Uppercase letters ([A-Z]+), followed by a dash (-),
            More uppercase letters ([A-Z]+), followed by a dot (.),
            Finally, alphanumeric exchange name (\w+).
            """
            match = re.search(r'([A-Z]+-[A-Z]+\.\w+)', json_copy['arg'])
            if match:
                symbol_and_exchange = match.group(1)

            print(f"Subscribing to websocket for {symbol_and_exchange}")
            if symbol_and_exchange not in self.websocket_subscription_list:
                self.websocket_subscription_list.append(symbol_and_exchange)
                asyncio.create_task(
                    self.subscribe_to_exchange_websocket(symbol_and_exchange)
                )
                json_copy['code'] = 200
                json_copy['arg']  = symbol_and_exchange + " subscribed ok"
            else:
                json_copy['code'] = 400
                json_copy['arg']  = symbol_and_exchange + " already subcribed"
            print(json.dumps(json_copy, separators=(',', ':')))
            return json.dumps( json_copy, separators=(',', ':') )
        except:
            pass

    async def rem_symbol(self, json_message):
        json_copy = copy.deepcopy(json_message)
        sym = json_copy['arg']
        if sym not in self.websocket_subscription_list:
            json_copy['code'] = 400
            json_copy['arg']  = sym + " not subscribed"
        else:
            self.websocket_subscription_list.remove(sym)
            symbol_and_exchange = sym
            await self.unsubscribe_from_exchange_websocket(symbol_and_exchange)
            json_copy['code'] = 200
            json_copy['arg']  = sym + " unsubcribed ok"

        return json.dumps( json_copy, separators=(',', ':') )

    async def pull_data_from_exchange_websockets(self):
        print("Setting up exchange websocket response lister")
        try:
            while not self.stop_threads:
                await asyncio.sleep(self.sleep_time)
                if self.data_list:
                    data_for_broadcast = json.dumps(self.data_list, separators=(',', ':'))
                    await self.queue.put(data_for_broadcast)
                    self.data_list.clear()
        except asyncio.CancelledError:
            print(f"Stopping pull_data_from_exchange_websockets gracefully...")
        finally:
            if self.data_list:
                print("Draining remaining data before shutdown...")
                data_for_broadcast = json.dumps(self.data_list, separators=(',', ':'))
                await self.queue.put(data_for_broadcast)
                self.data_list.clear()

    async def subscribe_to_exchange_websocket(self, symbol_and_exchange):
        try:
            print(f"Parsed value for websocket subscription: {symbol_and_exchange}")
            symbol = None
            exchange = None
            for exchange_name in self.exchange_instances.keys():
                if not exchange_name in symbol_and_exchange:
                    continue
                else:
                    exchange = self.exchange_instances[exchange_name.lower()]
                    symbol = symbol_and_exchange.split(".")[0]
                    break
            symbol = symbol.replace("-","/")
            print(f"Subscribing to websocket for {symbol_and_exchange}\nSymbol:{symbol}\nExchange: {exchange}\nExchange data type:{type(exchange)}")

            current_candle = exchange.fetch_ohlcv(symbol=symbol,timeframe=f"{self.timeframe}m", limit=1)
            current_ticker_vals = exchange.fetch_ticker(symbol=symbol)
            candle_data, ticker_data = await asyncio.gather(current_candle, current_ticker_vals)
                # Wait for both tasks to complete and get the results
            open_price = candle_data[0][1]
            high_price = candle_data[0][2]
            low_price = candle_data[0][3]
            volume = candle_data[0][5]
            prior_cumulative_volume = ticker_data['baseVolume']

            now = datetime.datetime.utcnow()
            epoch_time = int(now.timestamp())  # Current UTC time in seconds
            last_period = epoch_time - (epoch_time % (self.timeframe * 60))

            while True:
                data = await exchange.watch_ticker(symbol=symbol)
                timestamp = data['timestamp']
                dt_object = datetime.datetime.utcfromtimestamp(timestamp / 1000)
                epoch_time = int(dt_object.timestamp())

                current_period = epoch_time - (epoch_time % (self.timeframe * 60))

                date_string = datetime.datetime.fromtimestamp(timestamp / 1000).strftime("%Y%m%d")
                time_string = datetime.datetime.fromtimestamp(timestamp / 1000).strftime("%H%M%S")

                date_int = int(date_string)
                time_int = int(time_string)

                last_price = data['last']
                daily_vol = data['baseVolume']
                current_increment = abs(daily_vol - prior_cumulative_volume)
                prior_cumulative_volume = daily_vol
                volume += current_increment

                if current_period != last_period:
                    last_period = current_period  # Update minute tracker
                    volume = current_increment
                    open_price = high_price = low_price = last_price

                volume = round(volume, 5)

                high_price = max(high_price, last_price)
                low_price = min(low_price, last_price)

                bid_price = data['bid']
                ask_price = data['ask']
                bid_volume = data['bidVolume']
                ask_volume = data['askVolume']
                open_price = open_price
                high_price = high_price
                low_price = low_price
                day_high = data['high']
                day_low = data['low']
                day_open = data['open']
                prev_close = data['previousClose']

                rtd = {
                    "n": symbol_and_exchange,
                    "t": time_int,
                    "d": date_int,
                    "c": last_price,
                    "o": open_price,
                    "h": high_price,
                    "l": low_price,
                    "v": volume,
                    "oi": 0,
                    "bp": bid_price,
                    "ap": ask_price,
                    "s": daily_vol,
                    "bs": bid_volume,
                    "as": ask_volume,
                    "pc": prev_close,
                    "do": day_open,
                    "dh": day_high,
                    "dl": day_low
                }
                self.data_list.append(rtd)
        except Exception as e:
            print("Exception subscribing to websockets: ", e)

    async def unsubscribe_from_exchange_websocket(self,symbol_and_exchange):
        print(f"Determining symbol and exchange to unsubscribe: {symbol_and_exchange}")
        try:
            symbol_and_exchange = symbol_and_exchange.split()
            if len(symbol_and_exchange) > 1:
                symbol_and_exchange = symbol_and_exchange[1]
            else:
                symbol_and_exchange = symbol_and_exchange[0]
        except:
            pass
        print(f"Parsed value: {symbol_and_exchange}")
        try:
            symbol = None
            exchange = None
            for exchange_name in self.exchange_instances.keys():
                if not exchange_name in symbol_and_exchange:
                    continue
                else:
                    exchange = self.exchange_instances[exchange_name.lower()]
                    symbol = symbol_and_exchange.split(exchange_name)[0]
                    break
            print(f"Unsubscribing to websocket for {symbol_and_exchange}. Symbol:{symbol}\tExchange: {exchange}")
            await exchange.un_watch_ticker(symbol)
        except Exception as e:
            print(f"Exception: {e}")



    async def start_ws_server( self,aport ):
        print( f"Started RTD server: port={aport}\ntimeframe={self.timeframe}min\nsym_count={self.ticker_count}\nincrement_sym={self.inc_sym}")
        async with websockets.serve(self.handler, "localhost", aport ):
            await self.broadcast_messages_count()
        return

    async def handler(self, websocket):
        print(f"client connected")
        asyncio.create_task(self.recv( websocket ) )
        if self.initial_sym_add == True:
            asyncio.create_task(self.add_new_symbols_from_arcticdb_into_plugin())
        ## Send() task within handler
        try:
            while( not self.stop_threads ):
                async for message in self:
                    await websocket.send(message)         ## send broadcast RTD messages

                if( self.stop_threads ):
                    raise websockets.ConnectionClosed( None, None )

        except websockets.ConnectionClosed as wc:
            ## can check reason for close here
            print(f"Websocket connection in handler: {wc}")
            if not self.keep_running: stop_threads = True

        except ConnectionResetError:    pass
        print(f"client disconnected")
        return


    def broadcast(self, message ):
        print(message)
        self.publish(message)

    async def broadcast_messages_count(self):
        asyncio.create_task(self.pull_data_from_exchange_websockets())
        try:
            while not self.stop_threads:
                data = await self.queue.get()
                self.broadcast(data)
                await asyncio.sleep(self.sleep_time)    #simulate ticks in seconds

        except asyncio.CancelledError:  #raised when asyncio receives SIGINT from KB_Interrupt
            print(f"asyncio tasks: send stop signal, wait for exit...")
            self.stop_threads = True
            try:
                await asyncio.sleep( 3 )                          # these are not graceful exits.
                await asyncio.get_running_loop().stop()           # unrecco = asyncio.get_event_loop().stop()
            except: pass



    async def load_exchanges(self):
        tasks = [exchange.load_markets() for exchange in self.exchange_instances.values()]
        await asyncio.gather(*tasks)  # Run all load_markets() calls concurrently

    async def close_exchanges(self):
        tasks = [exchange.close() for exchange in self.exchange_instances.values()]
        await asyncio.gather(*tasks)  # Run all load_markets() calls concurrently

    async def get_historic_data_from_fetch_api(self, json_message):
        print(f"Fetching Data. Extracting symbol from message:{json_message}")
        symbol_and_exchange = None

        match = re.search(r'([A-Z]+-[A-Z]+\.\w+)', json_message['arg'])
        if match:
            symbol_and_exchange = match.group(1)

        print(f"Fetching Data for {symbol_and_exchange}")
        exchange = None
        symbol = None
        for exchange_name in self.exchange_instances.keys():
            if not exchange_name in symbol_and_exchange:
                continue
            else:
                exchange = self.exchange_instances[exchange_name.lower()]
                symbol = (symbol_and_exchange.split(".")[0]).replace("-","/").upper()
                break
        print(f"Making exchange data call from get_historic_data for {symbol} on {exchange}")
        await self.exchange_data_call(symbol_and_exchange,exchange,symbol,self.timeframe)

    async def exchange_data_call(self, symbol_and_exchange, exchange, symbol, timeframe):
        try:
            print(f"Calling {timeframe}min data for {symbol} from {exchange}")
            data = await exchange.fetch_ohlcv(
                                                symbol=symbol,
                                                timeframe = f"{timeframe}m",
                                                limit = 1000
                                                )

            if not data:  # Check if no data is returned
                print(f"No data received for {symbol} on {exchange} at {timeframe}")
                return

            formatted_data = [[row[0] // 1000, *row[1:]] for row in data]

            json_dict = {"hist":f"{symbol_and_exchange}","format":"uohlcv"}
            json_dict['bars'] = formatted_data
            json_string = json.dumps(json_dict)
            await self.queue.put(json_string)

        except Exception as e:
            print("Exception in exchange data call: ",e)


    async def add_new_symbols_from_arcticdb_into_plugin(self):
        try:
            await asyncio.sleep(2)
            print("Adding symbols from arcticdb into plugin...")
            data_man.send_telegram_msg("Adding symbols from arcticdb into plugin...")
            limit = 3
            tasks = [self.backfill_symbols_from_arctic_db_from_beginning(symbol_and_exchange, limit = limit) for symbol_and_exchange in self.watchlist]
            await asyncio.gather(*tasks)
            data_man.send_telegram_msg("Completed adding symbols. Retrieve from plugin")
        except Exception as e:
            print(f"Error creating distinct symlist in arcticdb: {e}")

    def milliseconds_for_timeframe(self):
        try:
            timeframes = {
                "1m": 60000,
                "5m": 300000,
                "1h": 3600000,
                "4h": 14400000,
                "1d": 86400000,
                "1w": 604800000
            }
            milliseconds = timeframes[f"{self.timeframe}m"]
            return milliseconds
        except Exception as e:
            print("Exception in milliseconds for timeframe", e)


    def define_symbol_and_limit(self, message):
        try:
            message_list = message.split()
            symbol_and_exchange = message_list[0]
            date_str = message_list[1]
            time_str = message_list[2]
            dt = datetime.datetime.strptime(f"{date_str} {time_str}", "%Y%m%d %H%M%S")
            last_amibroker_data_timestamp_ms = int(dt.timestamp() * 1000)
            current_timestamp_ms = int(time.time() * 1000)
            limit = (current_timestamp_ms - last_amibroker_data_timestamp_ms)/self.milliseconds_for_timeframe()
            print(symbol_and_exchange, limit)
            return symbol_and_exchange, limit

        except Exception as e:
            print("Exception defining",e)

    async def backfill_symbols_from_arctic_db_from_end(self, symbol_and_exchange, limit = None):
        try:
            qb = self.qb
            if not limit == None:
                limit = math.ceil(limit)
                query = qb.tail(n=limit)
                df = self.lib.read(symbol_and_exchange, query_builder=query).data
            else:
                df = self.lib.read(symbol_and_exchange).data
            bars = [
                [
                    int(idx // 1000),
                    float(row.open),
                    float(row.high),
                    float(row.low),
                    float(row.close),
                    float(row.volume)
                ]
                for idx, row in zip(df.index, df.itertuples(index=False))
            ]
            json_data = {
                "hist": symbol_and_exchange,
                "format": "uohlcv",
                "bars": bars
            }
            json_string = json.dumps(json_data)
            print(json_string)
            await self.queue.put(json_string)


        except Exception as e:
            print(f"Exception bananas: {e}")

    async def backfill_symbols_from_arctic_db_from_beginning(self, symbol_and_exchange, limit = None):
        try:
            qb = self.qb
            if not limit == None:
                limit = math.ceil(limit)
                query = qb.head(n=limit)
                df = self.lib.read(symbol_and_exchange, query_builder=query).data
            else:
                df = self.lib.read(symbol_and_exchange).data
            bars = [
                [
                    int(idx // 1000),
                    float(row.open),
                    float(row.high),
                    float(row.low),
                    float(row.close),
                    float(row.volume)
                ]
                for idx, row in zip(df.index, df.itertuples(index=False))
            ]
            json_data = {
                "hist": symbol_and_exchange,
                "format": "uohlcv",
                "bars": bars
            }
            json_string = json.dumps(json_data)
            print(json_string)
            await self.queue.put(json_string)


        except Exception as e:
            print(f"Exception: {e}")

    async def backfill_symbols_from_arctic_db_from_paginate(self, symbol_and_exchange, df, len_df):
        try:
            start = 0
            end = 50000
            step = 50000
            while start < len_df:
                df_chunk = df.iloc[start:end]
                bars = [
                    [
                        int(idx // 1000),
                        float(row.open),
                        float(row.high),
                        float(row.low),
                        float(row.close),
                        float(row.volume)
                    ]
                    for idx, row in zip(df_chunk.index, df.itertuples(index=False))
                ]
                json_data = {
                    "hist": symbol_and_exchange,
                    "format": "uohlcv",
                    "bars": bars
                }
                json_string = json.dumps(json_data)
                await self.queue.put(json_string)
                start = end
                end += step
                await asyncio.sleep(self.backfill_paginate_gap)
        except Exception as e:
            print(f"Exception: {e}")

    async def backfill_full_check_length(self, symbol_and_exchange):
        df = self.lib.read(symbol_and_exchange).data
        len_df = len(df)
        if len_df > self.backfill_size_allowance:
            print(f"Paginate data for {symbol_and_exchange}")
            time.sleep(2)
            await self.backfill_symbols_from_arctic_db_from_paginate(symbol_and_exchange,df, len_df)
        else:
            print("No pagination. Take whole set.")
            await self.backfill_symbols_from_arctic_db_from_beginning(symbol_and_exchange)

async def main():
    try:
        print("Creating server")
        server = RTDServer()
        server.check_if_initilising_symbols()
        await server.add_symbols_from_arctic_db_to_watchlist()
        print("Loading exchanges")
        await server.load_exchanges()
        print("Exchanges loaded")
        await asyncio.gather(server.start_ws_server(server.websocket_port))
        await server.close_exchanges()


    except KeyboardInterrupt:
        server.stop_threads = True
        await server.close_exchanges()
        print(f"Kill signal, Exit 1")
    except asyncio.CancelledError:
        pass
    except Exception:
        pass

if __name__ == "__main__":
    asyncio.run(main())
