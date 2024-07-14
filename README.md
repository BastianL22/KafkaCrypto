# KafkaCrypto
In diesem Projekt werden Live Crypto Daten über ein Python Producer Programm und die Advanced Trade API von Coinbase in einen Kafka Broker geladen und anschließend von Python Consumer Programmen ausgewertet.   

Producer.py
Dieses Programm abonniert über einen Websocket Client den Ticker Batch Channel der Advanced Trading API, welche Nachrichten mit Preisupdates bereitstellt. Diese Nachrichten werden dann als Events in ein Kafka Topic geschrieben.

dataframe.py
Lädt den Preis, das Volumen und einen Timestamp in jeweilige Dataframes und speichert sie in CSV Dateien zur weiteren Verwendung.

plot.py
Erlaubt dem Nutzer einen Chart eines ausgewählten Coins zu erstellen auf Basis der zuvor erstellten CSV Dateien.

alert.py
Erlaubt dem Nutzer einen Preisalarm zu einem ausgewählten Coin aufzusetzen, das Programm kann mehrfach parallel nebeneinander laufen und erlaubt einen parallelen Alert pro Währung.
