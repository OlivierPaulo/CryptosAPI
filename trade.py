from Kraken.api import KrakenAPI
from Binance.api import BinanceAPI
import os
import time
import pandas as pd
from Telegram.sendmarkdown import SendMarkdown
import json

DATA_PATH = "data/cryptos.csv"

def read_data():
    return pd.read_csv(DATA_PATH)

def buy_volume(crypto_balance):
    return round(float(crypto_balance*0.053), 3)

def buy_price(latest_trade_price):
    return round(float(latest_trade_price*0.9), 3)

def sell_volume(crypto_balance):
    return round(float(crypto_balance*0.05), 3)

def sell_price(latest_trade_price):
    return round(float(latest_trade_price*1.1), 3)

def modify_dataframe_values(df, index, crypto, latest_trade_price, volume_trade, trade_cost, trade_type, time, crypto_balance, next_buy_price, next_buy_volume, next_sell_price, next_sell_volume):
    df.loc[index, 'latest_trade_price'] = latest_trade_price
    df.loc[index, 'volume_trade'] = volume_trade
    df.loc[index, 'trade_cost'] = trade_cost
    df.loc[index, 'trade_type'] = trade_type
    df.loc[index, 'time'] = time
    df.loc[index, 'crypto_balance'] = crypto_balance
    df.loc[index, 'next_buy_price'] = next_buy_price
    df.loc[index, 'next_buy_volume'] = next_buy_volume
    df.loc[index, 'next_sell_price'] = next_sell_price
    df.loc[index, 'next_sell_volume'] = next_sell_volume
    
    return df


def append_dataframe(df, platform, crypto, pair, latest_trade_price, volume_trade, trade_cost, trade_type, time, crypto_balance, next_buy_price, next_buy_volume, next_sell_price, next_sell_volume):
    
    df = df.append({ 
        "platform": platform,
        "crypto": crypto,
        "pair": pair,
        "latest_trade_price": latest_trade_price,
        "volume_trade": volume_trade,
        "trade_cost": trade_cost,
        "trade_type": trade_type,
        "time": time,
        "crypto_balance": crypto_balance,
        "next_buy_price": next_buy_price,
        "next_buy_volume": next_buy_volume,
        "next_sell_price": next_sell_price,
        "next_sell_volume": next_sell_volume
    }, ignore_index=True)
    
    return df

def text_message(platform, crypto, trade_type, volume_trade, crypto_symbol, latest_trade_price, fiat, next_buy_volume, next_buy_price, next_sell_volume, next_sell_price):

    text = f"{platform} {crypto} trade : {trade_type.capitalize()} of {str(volume_trade).replace('.',',')} {crypto_symbol} @ {str(latest_trade_price).replace('.',',')} {fiat}\n"
    text += f"Next trade :\n"
    text += f"*Buy* {str(next_buy_volume).replace('.',',')} {crypto_symbol} @ _{str(next_buy_price).replace('.',',')}_ {fiat}\n"
    text += f"*Sell* {str(next_sell_volume).replace('.',',')} {crypto_symbol} @ _{str(next_sell_price).replace('.',',')}_ {fiat}\n"
    
    return text


if __name__ == '__main__':

    ## Telegram variables (chat & API token)
    chat_id = "509161525"
    telegram_token = os.environ.get('TELEGRAM_API_TOKEN')

    ## Read CSV data ##
    cryptos_df = read_data()
    #print(cryptos_df.shape)

####################
### KRAKEN TRADE ###
####################

    ## Declare Kraken Cryptos Symbol / Money
    
    with open("data/kraken_dict.json") as krakenfile:
        kraken_cryptos_dict = json.load(krakenfile)


    ### Requesting Kraken Balance ###
    kraken_balance_api = KrakenAPI(key=os.environ.get("KRAKEN_API_KEY"), secret=os.environ.get("KRAKEN_API_SECRET"))
    
    
    kraken_balance_request = kraken_balance_api.query_private(f'Balance')
    #print(kraken_balance_request)
    kraken_balance_api.close()


    ### Requesting Kraken Trade History ###
    kraken_request_api = KrakenAPI(key=os.environ.get("KRAKEN_API_KEY"), secret=os.environ.get("KRAKEN_API_SECRET"))

    data = {
        "start": int(time.time())-(3600*24*14),
        "end": int(time.time())
    }

    ### Requesting Trade History in Kraken ###
    kraken_trade_request = kraken_request_api.query_private(f'TradesHistory', data=data)
    #print(kraken_trade_request)
    kraken_request_api.close()

    ### Format Data to DataFrame ###
    trades_id = list(kraken_trade_request['result']['trades'].keys())

    #print(trades_id)


    for trade in trades_id:
        #print(kraken_trade_request['result']['trades'][trade])
        ### Check if the pair trade is inside the DataFrame of existing trades ###
        if kraken_trade_request['result']['trades'][trade]['pair'] in list(cryptos_df[cryptos_df['platform'] == 'Kraken']['pair']):

            ### Check if this trade is more recent than the one stored in the DataFrame ### 
            if int(kraken_trade_request['result']['trades'][trade]['time']) > int(cryptos_df[(cryptos_df['platform'] == 'Kraken') & (cryptos_df['pair'] == kraken_trade_request['result']['trades'][trade]['pair'])]['time']):
                index = (cryptos_df['platform'] == 'Kraken') & (cryptos_df['pair'] == kraken_trade_request['result']['trades'][trade]['pair'])

                crypto = str(kraken_cryptos_dict[kraken_trade_request['result']['trades'][trade]['pair'][0:4]])
                pair = str(kraken_trade_request['result']['trades'][trade]['pair'])
                latest_trade_price = round(float(kraken_trade_request['result']['trades'][trade]['price']), 3)
                volume_trade = round(float(kraken_trade_request['result']['trades'][trade]['vol']), 3)
                trade_cost = round(float(kraken_trade_request['result']['trades'][trade]['cost']), 3)
                trade_type = str(kraken_trade_request['result']['trades'][trade]['type'])
                time = int(kraken_trade_request['result']['trades'][trade]['time'])
                crypto_balance = round(float(kraken_balance_request['result'][kraken_trade_request['result']['trades'][trade]['pair'][0:4]]), 3)
                next_buy_price = buy_price(latest_trade_price)
                next_buy_volume = buy_volume(crypto_balance)
                next_sell_price = sell_price(latest_trade_price)
                next_sell_volume = sell_volume(crypto_balance)

                ### Store/Replace the most recent trade of this pair into the DataFrame ###
                cryptos_df = modify_dataframe_values(cryptos_df, index, crypto, latest_trade_price, volume_trade, trade_cost, trade_type, time, crypto_balance, next_buy_price, next_buy_volume, next_sell_price, next_sell_volume)
                
                ### Send a Telegram notifiction of this trade ###
                text = text_message("Kraken", crypto, trade_type, volume_trade, pair[1:4], latest_trade_price, pair[5:], next_buy_volume, next_buy_price, next_sell_volume, next_sell_price)
                SendMarkdown(chat_id=chat_id, text=text, token=telegram_token)

        else:
            
            ### This trade is not inside the DataFrame then store it as a new trade inside the DataFrame ### 
            crypto = str(kraken_cryptos_dict[kraken_trade_request['result']['trades'][trade]['pair'][0:4]])
            pair = str(kraken_trade_request['result']['trades'][trade]['pair'])
            latest_trade_price = round(float(kraken_trade_request['result']['trades'][trade]['price']), 3)
            volume_trade = round(float(kraken_trade_request['result']['trades'][trade]['vol']), 3)
            trade_cost = round(float(kraken_trade_request['result']['trades'][trade]['cost']), 3)
            trade_type = str(kraken_trade_request['result']['trades'][trade]['type'])
            time = int(kraken_trade_request['result']['trades'][trade]['time'])
            crypto_balance = round(float(kraken_balance_request['result'][kraken_trade_request['result']['trades'][trade]['pair'][0:4]]), 3)
            next_buy_price = buy_price(latest_trade_price)
            next_buy_volume = buy_volume(crypto_balance)
            next_sell_price = sell_price(latest_trade_price)
            next_sell_volume = sell_volume(crypto_balance)

            cryptos_df = append_dataframe(cryptos_df, "Kraken", crypto, pair, latest_trade_price, volume_trade, trade_cost, trade_type, time, crypto_balance, next_buy_price, next_buy_volume, next_sell_price, next_sell_volume)            
            text = text_message("Kraken", crypto, trade_type, volume_trade, pair[1:4], latest_trade_price, pair[5:], next_buy_volume, next_buy_price, next_sell_volume, next_sell_price)
            
            SendMarkdown(chat_id=chat_id, text=text, token=telegram_token)
            

#####################
### BINANCE TRADE ###
#####################

    with open("data/binance_dict.json") as binancefile:
        binance_cryptos_dict = json.load(binancefile)

    Binance_api = BinanceAPI(key=os.environ.get("BINANCE_API_KEY"), secret=os.environ.get("BINANCE_API_SECRET"))
    data = {
        #"recvWindow": 30000
        #"symbol": "CHZUSDT"
    }

    balances = Binance_api.query_private(f'account', data=data)
    
    for balance in balances['balances']:
        
        if (float(balance['free']) + float(balance['locked']) > 0.0) and (balance['asset'] not in ['USDT','WABI','NVT']):
            #print(f"{balance['asset']} : {float(balance['free']) + float(balance['locked'])}")

            data = {
                "symbol": str(f"{balance['asset']}USDT")
                #"startTime": int(1000*(time.time()-(3600*24*7)))
            }
            binance_trade_request = BinanceAPI(key=os.environ.get("BINANCE_API_KEY"), secret=os.environ.get("BINANCE_API_SECRET"))
            trades = binance_trade_request.query_private(f"myTrades", data=data)
            
            if trades:
                latest_trade = trades[-1]
                crypto = str(binance_cryptos_dict[str(balance['asset'])])
                pair = str(data['symbol'])
                latest_trade_price = round(float(latest_trade['price']), 3)
                volume_trade = round(float(latest_trade['qty']), 3)
                trade_cost = round(float(latest_trade['quoteQty']), 3)
                if latest_trade['isBuyer']:
                    trade_type = "buy"
                else:
                    trade_type = "sell"
                time = int(latest_trade['time'])
                crypto_balance = round(float(balance['free']) + float(balance['locked']), 3)
                next_buy_price = buy_price(latest_trade_price)
                next_buy_volume = buy_volume(crypto_balance)
                next_sell_price = sell_price(latest_trade_price)
                next_sell_volume = sell_volume(crypto_balance)

                ### Check if this trade pair is inside the DataFrame ###
                if data['symbol'] in list(cryptos_df[cryptos_df['platform'] == 'Binance']['pair']):

                    index = (cryptos_df['platform'] == "Binance") & (cryptos_df['pair'] == data['symbol'])
                    
                    ### Check if this trade is more recent than the one stored in the DataFrame ###
                    if int(latest_trade['time']) > int(cryptos_df[(cryptos_df['platform'] == "Binance") & (cryptos_df['pair'] == data['symbol'])]['time']):
                        
                        ### This pair trade is most recent so Store/Replace the trade line of this pair inside the DataFrame
                        cryptos_df = modify_dataframe_values(cryptos_df, index, crypto, latest_trade_price, volume_trade, trade_cost, trade_type, time, crypto_balance, next_buy_price, next_buy_volume, next_sell_price, next_sell_volume)
                        
                        ### Send a Telegram notifitcation of this new trade ###
                        text = text_message("Binance", crypto, trade_type, volume_trade, str(balance['asset']), latest_trade_price, "USDT", next_buy_volume, next_buy_price, next_sell_volume, next_sell_price)                
                        SendMarkdown(chat_id=chat_id, text=text, token=telegram_token)
                
                
                else:

                    ### This trade is not inside the DataFrame then store it as a new trade inside the DataFrame ###
                    cryptos_df = append_dataframe(cryptos_df, "Binance", crypto, pair, latest_trade_price, volume_trade, trade_cost, trade_type, time, crypto_balance, next_buy_price, next_buy_volume, next_sell_price, next_sell_volume)            
                    
                    ### Send a Telegram notification of this new trade ###
                    text = text_message("Binance", crypto, trade_type, volume_trade, str(balance['asset']), latest_trade_price, "USDT", next_buy_volume, next_buy_price, next_sell_volume, next_sell_price)
                    SendMarkdown(chat_id=chat_id, text=text, token=telegram_token)


### Store the final DataFrame into the CSV file as DataBase of most recent trades ###
cryptos_df.to_csv("data/cryptos.csv", index=False)