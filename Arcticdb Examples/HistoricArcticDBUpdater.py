import asyncio
import pandas as pd
from datetime import datetime
from AmiData import DataManager

"""
The purpose of this script is to be called every hour and on startup write to the Arctice DB to keep that up to date.
This script is not for the purposes of updating an individual symbol from Amibroker. That is handled differently.
Call this script using windows task scheduler
"""

async def binance_updater():
    dm_binance = DataManager(
        data_source="ccxt",
        asset_class="Crypto",
        exchange="binance",
        timeframe="5m",
        since = "2018-12-25 13:00:00",
        limit=1000
    )
    print("Gathering metadata from arcticdb...")
    exchange = dm_binance.return_exchange()
    asyncio.create_task(dm_binance.sym_dict_updater())
    try:
        markets = await exchange.fetch_markets()
        df = pd.DataFrame(markets)
        df_active = df[df['active'] == True]
        dm_binance.whole_market_list = df_active['symbol'].to_list()
        for ticker in dm_binance.whole_market_list:
            if (not "-" in ticker) and (not ":" in ticker):
                dm_binance.exchange_spot_list.append(ticker)
        await dm_binance.get_historic_crypto_data_from_ccxt_to_arcticdb_joblist(exchange)
    except:
        await exchange.close()
    try:
        await exchange.close()
    except Exception as e:
        dm_binance.exceptions_handler("Exception in Binance Data updater")

if __name__ == "__main__":
    asyncio.run(binance_updater())
finish = datetime.now()