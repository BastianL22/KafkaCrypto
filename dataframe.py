from kafka import KafkaConsumer
import json
from datetime import datetime
import pandas as pd
import os

#deklarieren eines Consumers
consumer = KafkaConsumer(
    "Crypto",
    bootstrap_servers = "your-kafka-port",
    group_id = "consumer1",
    auto_offset_reset = "earliest",
    value_deserializer = lambda m: json.loads(m.decode("utf-8")),
    consumer_timeout_ms = 1000 # Timeout, kommt 1000ms keine Nachricht, wird der Consumer geschlossen
)




def consume_messages(consumer):
        #deklarieren leerer Listen, an die die Daten angehängt werden
        price_list_btc = []
        timestamp_list_btc = []
        volume_list_btc = []

        price_list_eth = []
        timestamp_list_eth = []
        volume_list_eth = []

        price_list_doge=[]
        timestamp_list_doge=[]
        volume_list_doge = []

        price_list_usdt=[]
        timestamp_list_usdt=[]
        volume_list_usdt = []

        price_list_xrp=[]
        timestamp_list_xrp=[]
        volume_list_xrp = []

        counter = 0

        #Schleife wird für jede neue Nachricht im Consumer durchlaufen
        for message in consumer:    
            if message.value["channel"] == "ticker_batch":
                counter += 1
                #Speichern des Preises, Volumens und Timestamps einer Nachricht in der jeweiligen Liste
                if message.value["events"][0]["tickers"][0]["product_id"] == "BTC-EUR":
                    price_btc = message.value["events"][0]["tickers"][0]["price"]
                    timestamp_btc = message.value["timestamp"]
                    price_list_btc.append(price_btc)
                    timestamp_list_btc.append(timestamp_btc)
                    volume_btc = (float(message.value["events"][0]["tickers"][0]["volume_24_h"]))*(float(message.value["events"][0]["tickers"][0]["price"]))
                    volume_btc = round(volume_btc, 2)
                    volume_list_btc.append(volume_btc)
                    #Timestamp wird nach der Sekundenanzahl abgeschnitten
                    for i in range(len(timestamp_list_btc)):
                        timestamp_list_btc[i] = timestamp_list_btc[i][:19] 
                   # Konvertierung des Timestamps ins Datetime Format                                                   
                    timestamp_list_btc_datetime = [datetime.strptime(i, "%Y-%m-%dT%H:%M:%S") for i in timestamp_list_btc] 

                elif message.value["events"][0]["tickers"][0]["product_id"] == "ETH-EUR":
                    price_eth = message.value["events"][0]["tickers"][0]["price"]
                    price_list_eth.append(price_eth)
                    timestamp_eth = message.value["timestamp"]
                    timestamp_list_eth.append(timestamp_eth)
                    volume_eth = (float(message.value["events"][0]["tickers"][0]["volume_24_h"]))*(float(message.value["events"][0]["tickers"][0]["price"]))
                    volume_eth = round(volume_eth, 2)
                    volume_list_eth.append(volume_eth)
                    for i in range(len(timestamp_list_eth)):
                        timestamp_list_eth[i] = timestamp_list_eth[i][:19]
                    timestamp_list_eth_datetime = [datetime.strptime(j, "%Y-%m-%dT%H:%M:%S") for j in timestamp_list_eth]

                elif message.value["events"][0]["tickers"][0]["product_id"] == "DOGE-EUR":
                    price_doge = message.value["events"][0]["tickers"][0]["price"]
                    price_list_doge.append(price_doge)
                    timestamp_doge = message.value["timestamp"]
                    timestamp_list_doge.append(timestamp_doge)
                    volume_doge = (float(message.value["events"][0]["tickers"][0]["volume_24_h"]))*(float(message.value["events"][0]["tickers"][0]["price"]))
                    volume_doge = round(volume_doge, 2)
                    volume_list_doge.append(volume_doge)
                    for i in range(len(timestamp_list_doge)):
                        timestamp_list_doge[i] = timestamp_list_doge[i][:19]
                    timestamp_list_doge_datetime = [datetime.strptime(j, "%Y-%m-%dT%H:%M:%S") for j in timestamp_list_doge]

                elif message.value["events"][0]["tickers"][0]["product_id"] == "USDT-EUR":
                    price_usdt = message.value["events"][0]["tickers"][0]["price"]
                    price_list_usdt.append(price_usdt)
                    timestamp_usdt = message.value["timestamp"]
                    timestamp_list_usdt.append(timestamp_usdt)
                    volume_usdt = (float(message.value["events"][0]["tickers"][0]["volume_24_h"]))*(float(message.value["events"][0]["tickers"][0]["price"]))
                    volume_usdt = round(volume_usdt, 2)
                    volume_list_usdt.append(volume_usdt)
                    for i in range(len(timestamp_list_usdt)):
                        timestamp_list_usdt[i] = timestamp_list_usdt[i][:19]
                    timestamp_list_usdt_datetime = [datetime.strptime(j, "%Y-%m-%dT%H:%M:%S") for j in timestamp_list_usdt]

                elif message.value["events"][0]["tickers"][0]["product_id"] == "XRP-EUR":
                    price_xrp = message.value["events"][0]["tickers"][0]["price"]
                    price_list_xrp.append(price_xrp)
                    timestamp_xrp = message.value["timestamp"]
                    timestamp_list_xrp.append(timestamp_xrp)
                    volume_xrp = (float(message.value["events"][0]["tickers"][0]["volume_24_h"]))*(float(message.value["events"][0]["tickers"][0]["price"]))
                    volume_xrp = round(volume_xrp, 2)
                    volume_list_xrp.append(volume_xrp)
                    for i in range(len(timestamp_list_xrp)):
                        timestamp_list_xrp[i] = timestamp_list_xrp[i][:19]
                    timestamp_list_xrp_datetime = [datetime.strptime(j, "%Y-%m-%dT%H:%M:%S") for j in timestamp_list_xrp]

        consumer.close()
        #Schreiben der Daten in Pandas Dataframes, die in csv-Dateien gespeichert werden.
        df_bit = pd.DataFrame({"time":timestamp_list_btc_datetime, "price":price_list_btc, "volume":volume_list_btc})
        output_path="bitcoin.csv"
        df_bit.to_csv(output_path, mode="a", header=not os.path.exists(output_path))
        df_eth = pd.DataFrame({"time":timestamp_list_eth_datetime, "price":price_list_eth, "volume":volume_list_eth})
        output_path="ethereum.csv"
        df_eth.to_csv(output_path, mode="a", header=not os.path.exists(output_path))
        df_doge = pd.DataFrame({"time":timestamp_list_doge_datetime, "price":price_list_doge, "volume":volume_list_doge})
        output_path="dogecoin.csv"
        df_doge.to_csv(output_path, mode="a", header=not os.path.exists(output_path))
        df_usdt = pd.DataFrame({"time":timestamp_list_usdt_datetime, "price":price_list_usdt, "volume":volume_list_usdt})
        output_path="tether.csv"
        df_usdt.to_csv(output_path, mode="a", header=not os.path.exists(output_path))
        df_xrp = pd.DataFrame({"time":timestamp_list_xrp_datetime, "price":price_list_xrp, "volume":volume_list_xrp})
        output_path="xrp.csv"
        df_xrp.to_csv(output_path, mode="a", header=not os.path.exists(output_path))
        #Ausgabe der Anzahl der neuen Einträge
        print("Es wurden " + str(counter) + " neue Einträge geschrieben")

            


def main():

    consume_messages(consumer)


if __name__ == "__main__":
    main()