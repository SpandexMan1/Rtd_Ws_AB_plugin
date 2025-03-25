import ccxt.async_support as ccxt
import ccxt.pro as ccxtpro
import json
import pandas as pd
import urllib3
import os
import asyncio
from datetime import datetime
import win32com.client
import logging
import traceback
import requests
import arcticdb
from arcticdb import QueryBuilder
import pytz

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
current_script_directory = os.path.dirname(os.path.abspath(__file__))
parent_directory = os.path.dirname(current_script_directory)
amibroker_directory = os.path.join("C:\\", "Program Files","Amibroker")


logging.basicConfig(level=logging.INFO, filename='logs.txt', filemode='a', format='%(asctime)s - %(message)s')
class DataManager:
    def __init__(self,
                 data_source: str = None,
                 asset_class: str = None,
                 exchange: str = None,
                 timeframe: str = None,
                 since: str = None,
                 limit: int = None
                 ) -> object:

        self.data_source = data_source
        self.asset_class = asset_class
        self.exchange = exchange
        self.timeframe = timeframe
        self.since = since
        self.limit = limit
        self.timezone = pytz.timezone("Europe/London")


        self.arcticdb_path =  os.path.join(current_script_directory,"ArcticDB","arctic_db")
        self.store = arcticdb.Arctic(f"lmdb://{self.arcticdb_path}?map_size=200GB")
        self.lib = self._init_arcticdb_lib()
        self.qb = QueryBuilder()

        self.sym_dict = {}
        self.all_ohlcv_queue = asyncio.Queue()
        self.exchange_spot_list = []
        self.arcticdb_syms_fetch_versions_list = []
        self.new_sym_list = []
        self.rest_api_df_list = []
        self.whole_market_list = []


        self.metadata_completion = 0

        self.amibroker_database_dir = os.path.join(amibroker_directory, "Databases")
        self.discord_bot_token = "PUT_DISCORD_BOT_TOKEN_HERE"

        self.base_telegram_url = "https://api.telegram.org/bot"
        self.chat_id = "-CHAT_ID_HERE"
        self.token = "PUT_TELEGRAM_CHAT_TOKEN_HERE"

        self.Scripts_dir = os.path.join(amibroker_directory,"Scripts")
        self.DataFiles_dir = os.path.join(amibroker_directory, "DataFiles")

        if not self.asset_class == None:
            self.asset_class_dir = os.path.join(self.DataFiles_dir, self.asset_class)
            self.data_source_dir = os.path.join(self.asset_class_dir, self.data_source)
            self.exchange_directory = os.path.join(self.data_source_dir,self.exchange)

            self.current_data_dir = os.path.join(self.exchange_directory, "Current")
            self.historic_data_dir = os.path.join(self.exchange_directory, "Historic")
            self.interim_data_dir = os.path.join(self.exchange_directory,"Interim")

            self.exceptions_file = os.path.join(self.Scripts_dir,"Exceptions.txt")
            self.lockout_open = False

            def create_dir(dir):
                if (not os.path.exists(dir)) and (not os.path.isdir(dir)):
                    os.mkdir(dir)
            create_dir(self.DataFiles_dir)
            create_dir(self.asset_class_dir)
            create_dir(self.data_source_dir)
            create_dir(self.exchange_directory)
            create_dir(self.current_data_dir)
            create_dir(self.historic_data_dir)
            create_dir(self.interim_data_dir)

    def exceptions_handler(self, msg, e):
        with open(self.exceptions_file, "a") as fh:
            logging.info("Exception logged")
            now_time = datetime.now()
            formatted_time_now = now_time.strftime("%Y-%m-%d %H:%M:%S")

            # Capture the full traceback
            tb_str = ''.join(traceback.format_exception(None, e, e.__traceback__))
            print(f"Exception; {tb_str}")
            # Write the timestamp and traceback to the file
            fh.write(f"Timestamp: {formatted_time_now}\n")
            fh.write(msg + "\n")
            fh.write("Exception Traceback:\n")
            fh.write(tb_str)
            fh.write("\n\n")


    ### ### ### ### ### ### ### ###
    ### ### Arctic DB connection
    ### ### ### ### ### ### ### ###

    def _init_arcticdb_lib(self):
        try:
            return self.store["market_data"]
        except arcticdb.exceptions.LibraryNotFound:
            self.store.create_library("market_data")
            return self.store["market_data"]



    ### ### ### ### ### ### ### ###
    ### ### Exchange Instances
    ### ### ### ### ### ### ### ###

    def return_exchange(self):
        try:
            exchange_instances = {
                "binance": ccxtpro.binance(),
                "bybit": ccxtpro.bybit(),
                "coinbase": ccxtpro.coinbase(),
                "okx": ccxtpro.okx(),
                "mexc": ccxtpro.mexc(),
                "kucoin": ccxtpro.kucoin(),
                "gateio": ccxtpro.gateio(),
                "kraken": ccxtpro.kraken()
            }
            return exchange_instances[self.exchange.lower()]
        except Exception as e:
            self.exceptions_handler("Exception returning exchange", e)

    def define_ticker_suffix_from_exchange(self):
            return f".{str(self.exchange).lower()}"



    ### ### ### ### ### ### ### ###
    ### ### Timestamp parsing functions
    ### ### ### ### ### ### ### ###

    def parse_datestamp_from_exchange(self, timestamp):
        try:
            stamp = datetime.fromtimestamp(timestamp / 1000)
            stamp_string = stamp.strftime('%Y-%m-%d %H:%M:%S')
            date = stamp_string.split()[0]
            return date
        except Exception as e:
            self.exceptions_handler("Exception parsing datestamp from exchange", e)

    def parse_timestamp_from_exchange(self, timestamp):
        try:
            stamp = datetime.fromtimestamp(timestamp / 1000)
            stamp_string = stamp.strftime('%Y-%m-%d %H:%M:%S')
            timestring = stamp_string.split()[1]
            return timestring
        except Exception as e:
            self.exceptions_handler("Exception parsing datestamp from exchange", e)

    def parse_full_timestamp_from_ms_integer(self, timestamp):
        try:
            stamp = datetime.fromtimestamp(timestamp / 1000)
            stamp_string = stamp.strftime('%Y-%m-%d %H:%M:%S')
            return stamp_string
        except Exception as e:
            self.exceptions_handler("Exception parsing timestamp from exchange", e)
            return None

    def parse_timestamp_to_unix_ms_from_string(self, latest_stamp):
        try:
            dt_object = datetime.strptime(latest_stamp, '%Y-%m-%d %H:%M:%S')
            unix_ms = (int(dt_object.timestamp())) * 1000
            return unix_ms
        except Exception as e:
            self.exceptions_handler("Exception convert timestamp to epoch", e)

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
            milliseconds = timeframes[self.timeframe]
            return milliseconds
        except Exception as e:
            self.exceptions_handler("Exception in return milliseconds", e)



    ### ### ### ### ### ### ### ###
    ### ### Data call task lists
    ### ### ### ### ### ### ### ###

    async def get_historic_crypto_data_from_ccxt_to_arcticdb_joblist(self, exchange):
        while (int(datetime.now().timestamp() - self.metadata_completion)) == int(datetime.now().timestamp()):
            print("waiting")
            await asyncio.sleep(1)
        symlist = [item for item in self.exchange_spot_list if item not in self.arcticdb_syms_fetch_versions_list]
        symlist.extend(self.arcticdb_syms_fetch_versions_list)
        semaphore = asyncio.Semaphore(1000)  # adjust this number as needed

        async def semaphore_wrapped_fetch(symbol):
            async with semaphore:
                await self.get_historic_crypto_spot_data_from_ccxt_to_arctic_db(symbol, exchange)

        tasks = [asyncio.create_task(semaphore_wrapped_fetch(symbol)) for symbol in symlist]
        await asyncio.gather(*tasks)

    ### ### ### ### ### ### ### ###
    ### ### Data calls
    ### ### ### ### ### ### ### ###

    async def get_historic_crypto_spot_data_from_ccxt_to_arctic_db(self, symbol, exchange):
        if not isinstance(symbol, str):
            print(f"Invalid symbol type: {symbol}")
            return
        arcticdb_symbol = symbol.upper().replace("/", "-") + "." + str(exchange).lower()
        print(f"\nCalling data for {symbol} on {exchange}, appending to ArcticDB as {arcticdb_symbol}")
        try:
            latest_timestamp = int(self.sym_dict[arcticdb_symbol]['latest_timestamp'])
            fetch_symbol = self.sym_dict[arcticdb_symbol]['fetch_api_vers']
        except KeyError:
            try:
                print(f"{arcticdb_symbol} not found in sym_dict; falling back to BTC-USDT.binance reference")
                latest_timestamp = int(self.sym_dict["BTC-USDT.binance"]['latest_timestamp'])
                fetch_symbol = symbol
            except:
                latest_timestamp = self.parse_timestamp_to_unix_ms_from_string(self.since)
                fetch_symbol = symbol
        except Exception as e:
            self.exceptions_handler(f"Error retrieving timestamp for {symbol}", e)
            return
        start_time = latest_timestamp + self.milliseconds_for_timeframe()
        params = {'paginate': True}

        try:
            # Fetch new OHLCV data
            ohlcv = await exchange.fetch_ohlcv(
                symbol=fetch_symbol,
                since=start_time,
                limit=self.limit,
                timeframe=self.timeframe,
                params=params
            )

            if not ohlcv:
                print(f"No new OHLCV data fetched for {symbol}")
                return

            df_new = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df_new = df_new[df_new['timestamp'] > start_time]

            if df_new.empty:
                print(f"No new rows to append for {arcticdb_symbol}")
                return

            # Set timestamp as index with guaranteed int64 dtype
            df_new.set_index('timestamp', inplace=True)
            df_new.index = pd.Index(df_new.index, dtype='int64')
            df_new = df_new[['open', 'high', 'low', 'close', 'volume']]
            df_new.sort_index(inplace=True)

            # Check and correct existing index type in ArcticDB
            try:
                existing_df = self.lib.read(arcticdb_symbol).data
                if existing_df.index.dtype != 'int64':
                    print(f"Converting existing index for {arcticdb_symbol} to int64 dtype")
                    existing_df.index = pd.Index(existing_df.index, dtype='int64')
                    self.lib.write(arcticdb_symbol, existing_df)
            except Exception as e:
                print(f"No existing data found or index check failed for {arcticdb_symbol}. {e}")

            # Append to ArcticDB
            self.lib.append(arcticdb_symbol, df_new)
            print(f"✅ Appended {len(df_new)} new rows to {arcticdb_symbol}")

        except Exception as e:
            self.exceptions_handler(f"Exception fetching/appending data for {symbol}", e)




    async def get_recent_crypto_spot_data_for_arcticdb(self, symbol, exchange):
        try:
            print(f"Fetching {symbol}")
            try:
                ohlcv = await exchange.fetch_ohlcv(symbol, self.timeframe, limit=self.limit)
                df_titled_cols_ohlcv = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                self.rest_api_df_list.append(df_titled_cols_ohlcv)
                await asyncio.sleep(0.03)
                print(f"Appended {symbol} dataframe to list of dataframes")
                await self.all_ohlcv_queue.put(ohlcv)
            except ccxt.NetworkError as e:
                print(f'Network error fetching data for {symbol}: {e}')
                await asyncio.sleep(10)  # Wait before retrying
            except ccxt.ExchangeError as e:
                print(f'Exchange error fetching data for {symbol}: {e}')
            except Exception as e:
                print(f'Unexpected error fetching data for {symbol}: {e}')
        except Exception as e:
            self.exceptions_handler("Exception in get_recent_crypto_spot_data", e)


    ### ### ### ### ### ### ### ###
    ### ### Metadata
    ### ### ### ### ### ### ### ###

    async def update_metadata_arctic_db(self, symbol):
        try:
            print(f"Symbol to update in dictionary: {symbol}")
            arctic_db_vers = symbol
            df = self.lib.read(symbol).data
            if 'timestamp' in df.columns:
                latest_ts = int(df['timestamp'].iloc[-1])
            else:
                latest_ts = int(df.index[-1])
            fetch_api_vers = (arctic_db_vers.split(".")[0]).replace("-","/")
            self.sym_dict[symbol] = {
                'latest_timestamp': latest_ts,
                'latest_human_readable': self.parse_timestamp_from_ms_integer(latest_ts),
                'last_updated' : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'arctic_db_vers' : arctic_db_vers,
                'fetch_api_vers': fetch_api_vers
            }
            self.arcticdb_syms_fetch_versions_list.append(fetch_api_vers)

        except Exception as e:
            self.exceptions_handler("Exception getting last timestamp", e)



    async def sym_dict_updater(self):
        try:
            symbols = self.lib.list_symbols()
            print(symbols)
            tasks = [asyncio.create_task(self.update_metadata_arctic_db(symbol)) for symbol in symbols]
            await asyncio.gather(*tasks)
            self.metadata_completion = int(datetime.now().timestamp())
        except Exception as e:
            self.exceptions_handler("Exception updating arcticdb metadata dictionary", e)



    ### ### ### ### ### ### ### ###
    ### ### CSV Helpers and sorters
    ### ### ### ### ### ### ### ###
    async def change_ticker_substring(self, prior_substring, new_substring):
        files = (os.listdir(self.historic_data_dir))
        for file in files:
            try:
                if not prior_substring.split(".")[0] in file:
                    print(f"Skipping {file}")
                    continue
                print(f"Changing substring in {file}.")
                full_path = os.path.join(self.historic_data_dir, file)
                df_ticker = pd.read_csv(full_path, header=None)
                df_ticker[0] = df_ticker[0].astype(str)
                df_ticker[0] = df_ticker[0].str.replace(prior_substring, new_substring)
                df_ticker.to_csv(full_path, header=False, index=False, mode="w")
            except Exception as e:
                print(f"Unable to change {file}: ", e)

    async def change_ticker_substring_by_file_conts(self, file_substring,prior_substring, new_substring):
        files = (os.listdir(self.historic_data_dir))
        for file in files:
            try:
                if not file_substring in file:
                    print(f"Skipping {file}")
                    continue
                full_path = os.path.join(self.historic_data_dir, file)
                df_ticker = pd.read_csv(full_path, header=None)
                df_ticker[0] = df_ticker[0].astype(str)
                df_ticker[0] = df_ticker[0].str.replace(prior_substring, new_substring)
                print(f"Changed substring in {file} from {prior_substring} to {new_substring}. Ticker: {df_ticker[0][0]}")
                df_ticker.to_csv(full_path, header=False, index=False, mode="w")

            except Exception as e:
                print(f"Unable to change {file}: ", e)



    ### ### ### ### ### ### ### ###
    ### ### Non Crypto
    ### ### ### ### ### ### ### ###

    def import_to_amibroker(self, full_path,file_format, database):
        try:
            print("Creating Amibroker object")
            ab = win32com.client.Dispatch("Broker.Application")
            database_path = os.path.join(self.amibroker_database_dir, database)
            print(f"Loading Database {database_path}")
            ab.LoadDatabase(database_path)
            print("Importing data. Wait.")
            ab.Import(0, full_path, file_format)
            print(f"Imported {full_path}\nAmibroker Database: {database}")
            ab.RefreshAll()
            print(f"Refreshed Database. Saving...")
            ab.SaveDatabase()
            print("Saved")
            print(f"Closing {database} database and quitting amibroker...")
            ab.Quit()
            print("End of import procedure \n")
        except Exception as e:
            self.exceptions_handler("Exception in import to amibroker", e)



    ### ### ### ### ### ### ### ###
    ### ### Messengers
    ### ### ### ### ### ### ### ###
    def send_discord_message(self, channel,message, file_path = None):
        if channel == "stock_market":
            CHANNEL_ID = "1280300772504109096"
        elif channel == "crypto_chat":
            CHANNEL_ID = "1276167841191694410"
        url = f'https://discord.com/api/v10/channels/{CHANNEL_ID}/messages'

        headers = {
            'Authorization': f'Bot {self.discord_bot_token}',
        }

        data = {
            'content': f"{message}",
        }
        if file_path != None:
            print(f"Attempting to send {file_path}")
            files = {
                'file': open(file_path, 'rb'),
            }
            response = requests.post(url, headers=headers, data=data, files=files)
            if response.status_code == 200:
                print('File uploaded successfully!')
            else:
                print(f'Failed to upload file: {response.status_code} - {response.text}')
        else:
            response = requests.post(url, headers=headers, data=data)
            if response.status_code == 200:
                print('Message sent successfully!')
            else:
                print(f'Failed to send message: {response.status_code} - {response.text}')

    def send_telegram_msg(self, message):
        try:
            endpoint = "sendMessage"
            params = "&".join(["chat_id=" + self.chat_id, "text=" + message])
            request_url = "".join([self.base_telegram_url, self.token, "/", endpoint, "?", params])
            requests.get(url=request_url, verify=False)
        except Exception as e:
            print("Exceptions send_telegram_msg: ", e)