from coinbase.websocket import WSClient
import json
from kafka import KafkaProducer

# deklarieren eines Producers
producer = KafkaProducer(
    bootstrap_servers='your-kafka-broker-adress',  # Kafka broker Adresse
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

# api key für coinbase
api_key = "your api key"
api_secret = "your api secret key"

# wird ausgeführt, wenn eine neue Nachricht reinkommt
def on_message(message):    
        message_json = json.loads(message)
        # Sendet Nachricht als event ins Topic "Crypto"
        producer.send("Crypto", message_json)   
        producer.flush()

ws_client = WSClient(api_key=api_key, api_secret=api_secret, on_message=on_message, verbose=True)

def main():
    #Verbindung mit dem Websocket Client
    ws_client.open()    
    # Abonnieren des Channels ticker_batch, der alle 5 Sekunden neue Daten für die angegebenen Währungen ausgibt
    ws_client.subscribe(["ETH-EUR", "BTC-EUR", "DOGE-EUR", "USDT-EUR", "XRP-EUR"], ["ticker_batch"])   
    #Endlose Laufzeit 
    ws_client.run_forever_with_exception_check()
      

if __name__ == "__main__":
    main()






