from Kraken.api import KrakenAPI
import os
import time
import pandas as pd
from Telegram.sendmarkdown import SendMarkdown





if __name__ == '__main__':

    ## Telegram variables (chat & API token)
    chat_id = "509161525"
    telegram_token = os.environ.get('TELEGRAM_API_TOKEN')

    ## Read CSV data ##
    cryptos_df = pd.read_csv("data/cryptos.csv")
    #print(cryptos_df.shape)

    ## Declare Kraken Cryptos Symbol / Money
    kraken_cryptos_dict = {
        "ZEUR":"Euro",
        "ZUSD":"Dollar",
        "XXBT":"Bitcoin",
        "XXRP":"Ripple",
        "XXLM":"Stellar",
        "XXMR":"Monero",
        "EOS":"Eos"
    }

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

    kraken_trade_request = kraken_request_api.query_private(f'TradesHistory', data=data)
    #print(kraken_trade_request)
    kraken_request_api.close()

    ### Format Data to DataFrame ###
    trades_id = list(kraken_trade_request['result']['trades'].keys())

    #print(trades_id)


    for trade in trades_id:
        #print(kraken_trade_request['result']['trades'][trade])
        if kraken_trade_request['result']['trades'][trade]['pair'] in list(cryptos_df['pair']):
            if int(kraken_trade_request['result']['trades'][trade]['time']) > int(cryptos_df[cryptos_df['pair'] == kraken_trade_request['result']['trades'][trade]['pair']]['time']):
                index = (cryptos_df['pair'] == kraken_trade_request['result']['trades'][trade]['pair'])

                crypto = str(kraken_cryptos_dict[kraken_trade_request['result']['trades'][trade]['pair'][0:4]])
                pair = str(kraken_trade_request['result']['trades'][trade]['pair'])
                latest_trade_price = round(float(kraken_trade_request['result']['trades'][trade]['price']), 3)
                volume_trade = round(float(kraken_trade_request['result']['trades'][trade]['vol']), 3)
                trade_cost = round(float(kraken_trade_request['result']['trades'][trade]['cost']), 3)
                trade_type = str(kraken_trade_request['result']['trades'][trade]['type'])
                time = int(kraken_trade_request['result']['trades'][trade]['time'])
                crypto_balance = round(float(kraken_balance_request['result'][kraken_trade_request['result']['trades'][trade]['pair'][0:4]]), 3)
                next_buy_price = round(float(round(float(kraken_trade_request['result']['trades'][trade]['price']), 3)*0.9), 3)
                next_buy_volume = round(float(round(float(kraken_balance_request['result'][kraken_trade_request['result']['trades'][trade]['pair'][0:4]]), 3)*0.053), 3)
                next_sell_price = round(float(round(float(kraken_trade_request['result']['trades'][trade]['price']), 3)*1.1), 3)
                next_sell_volume = round(float(round(float(kraken_balance_request['result'][kraken_trade_request['result']['trades'][trade]['pair'][0:4]]), 3)*0.05), 3)

                cryptos_df.loc[index, 'latest_trade_price'] = latest_trade_price
                cryptos_df.loc[index, 'volume_trade'] = volume_trade
                cryptos_df.loc[index, 'trade_cost'] = trade_cost
                cryptos_df.loc[index, 'trade_type'] = trade_type
                cryptos_df.loc[index, 'time'] = time
                cryptos_df.loc[index, 'crypto_balance'] = crypto_balance
                cryptos_df.loc[index, 'next_buy_price'] = next_buy_price
                cryptos_df.loc[index, 'next_buy_volume'] = next_buy_volume
                cryptos_df.loc[index, 'next_sell_price'] = next_sell_price
                cryptos_df.loc[index, 'next_sell_volume'] = next_sell_volume

                #print(cryptos_df)

                text = f"Kraken {crypto} trade : {trade_type.capitalize()} of {str(volume_trade).replace('.',',')} {pair[1:4]} @ {str(latest_trade_price).replace('.',',')} {pair[5:]}\n"
                text += f"Next trade :\n"
                text += f"*Buy* {str(next_buy_volume).replace('.',',')} {pair[1:4]} @ _{str(next_buy_price).replace('.',',')}_ {pair[5:]}\n"
                text += f"*Sell* {str(next_sell_volume).replace('.',',')} {pair[1:4]} @ _{str(next_sell_price).replace('.',',')}_ {pair[5:]}\n"
                #print(text)
                SendMarkdown(chat_id=chat_id, text=text, token=telegram_token)

        else:
            
            crypto = str(kraken_cryptos_dict[kraken_trade_request['result']['trades'][trade]['pair'][0:4]])
            pair = str(kraken_trade_request['result']['trades'][trade]['pair'])
            latest_trade_price = round(float(kraken_trade_request['result']['trades'][trade]['price']), 3)
            volume_trade = round(float(kraken_trade_request['result']['trades'][trade]['vol']), 3)
            trade_cost = round(float(kraken_trade_request['result']['trades'][trade]['cost']), 3)
            trade_type = str(kraken_trade_request['result']['trades'][trade]['type'])
            time = int(kraken_trade_request['result']['trades'][trade]['time'])
            crypto_balance = round(float(kraken_balance_request['result'][kraken_trade_request['result']['trades'][trade]['pair'][0:4]]), 3)
            next_buy_price = round(float(round(float(kraken_trade_request['result']['trades'][trade]['price']), 3)*0.9), 3)
            next_buy_volume = round(float(round(float(kraken_balance_request['result'][kraken_trade_request['result']['trades'][trade]['pair'][0:4]]), 3)*0.053), 3)
            next_sell_price = round(float(round(float(kraken_trade_request['result']['trades'][trade]['price']), 3)*1.1), 3)
            next_sell_volume = round(float(round(float(kraken_balance_request['result'][kraken_trade_request['result']['trades'][trade]['pair'][0:4]]), 3)*0.05), 3)

            cryptos_df = cryptos_df.append({ 
                "platform": "Kraken",
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


            text = f"Kraken {crypto} trade : {trade_type.capitalize()} of {str(volume_trade).replace('.',',')} {pair[1:4]} @ {str(latest_trade_price).replace('.',',')} {pair[5:]}\n"
            text += f"Next trade :\n"
            text += f"*Buy* {str(next_buy_volume).replace('.',',')} {pair[1:4]} @ _{str(next_buy_price).replace('.',',')}_ {pair[5:]}\n"
            text += f"*Sell* {str(next_sell_volume).replace('.',',')} {pair[1:4]} @ _{str(next_sell_price).replace('.',',')}_ {pair[5:]}\n"
            #print(text)
            SendMarkdown(chat_id=chat_id, text=text, token=telegram_token)
            
#print(cryptos_df)




cryptos_df.to_csv("data/cryptos.csv", index=False)