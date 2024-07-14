from kafka import KafkaConsumer
import json
import pandas as pd
import pymsgbox





def alert():
    #Liste der verfügbaren Coins
    coin_list=["Bitcoin", "Ethereum", "Dogecoin", "Tether", "XRP"]

    #Nutzer wird nach Coin gefragt  
    print("\nBitcoin \nEthereum \nDogecoin \nTether \nXRP \n\nBitte suchen Sie sich eine der oben genannten Währungen aus, zu der Sie benachrichtigt werden wollen\n ")
    coin = input()

    while (True):
        if coin in coin_list:
            break
        else:
            print("\nBitcoin \nEthereum \nDogecoin \nTether \nXRP \n\nVersuchen Sie es nochmal, bitte achten Sie auf Groß- und Kleinschreibung\n ")
            coin = input()

    match coin:
        case "Bitcoin":
            coin_id = "BTC-EUR"
            group = "alert_btc"
        case "Ethereum":
            coin_id = "ETH-EUR"
            group = "alert_eth"
        case "Dogecoin":
            coin_id = "DOGE-EUR"
            group = "alert_doge"
        case "Tether":
            coin_id = "USDT-EUR"
            group = "alert_usdt"
        case "XRP":
            coin_id = "XRP-EUR"
            group = "alert_xrp"

    #Deklarieren eines Consumers mit Timeout, damit der Consumer am bei der letzten Nachricht sich wieder schließt
    consumer = KafkaConsumer(
        "Crypto",
        bootstrap_servers = "your-kafka-broker-adress",
        group_id = group, #Consumer Gruppe abhängig von Währung, man kann also einen alert pro Währung gleichzeitig laufen lassen
        auto_offset_reset = "earliest",
        value_deserializer = lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms = 1000
    )






    #Nutzer wird nach Preisgrenze gefragt
    while True:
        try:
            print("Nun geben Sie bitte einen Preis an, zu dem Sie benachrichtigt werden wollen: ")
            price = float(input())
            break
        except ValueError:
            print("Bitte geben Sie eine Nummer an")



    #der zuletzt gelesene Preis wird gespeichert
    for message in consumer:
        if message.value["channel"] == "ticker_batch":
            if message.value["events"][0]["tickers"][0]["product_id"] == coin_id:
                start_price = message.value["events"][0]["tickers"][0]["price"]
                start_price = float(start_price)

    consumer.close()

    
    #Neuer Consumer wird deklariert, diesmal ohne Timeout, damit er endlos läuft
    consumer = KafkaConsumer(
        "Crypto",
        bootstrap_servers = "your-kafka-broker-adress",
        group_id = group,
        auto_offset_reset = "latest",
        value_deserializer = lambda m: json.loads(m.decode("utf-8")),
    )

    #Ist der vorhin gespeicherte Preis kleiner, als die Preisgrenze, dann wird der Alarm ausgelöst sobald der momentane Preis höher ist und vice versa
    if start_price <= price :
        x = True
    else:
        x = False

    for message in consumer:
        if message.value["channel"] == "ticker_batch":
            if message.value["events"][0]["tickers"][0]["product_id"] == coin_id:
                price_coin = message.value["events"][0]["tickers"][0]["price"]
                price_coin = float(price_coin)
                print("Aktueller Preis von " + coin + " liegt bei: " + str(price_coin))

                if x:
                    if price_coin >= price:
                        pymsgbox.alert("Der Kurs von " + str(coin) + " hat die Marke " + str(price) + " erreicht!", "Preisalarm!")
                        consumer.close()
                else:
                    if price_coin <= price:
                        pymsgbox.alert("Der Kurs von " + str(coin) + " hat die Marke " + str(price) + " erreicht!", "Preisalarm!")
                        consumer.close()
            
def main():
    alert()

if __name__ == "__main__":
    main()


                    
                