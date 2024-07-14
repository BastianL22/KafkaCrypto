from datetime import datetime
import pandas as pd
import finplot as fplt

def plot():
    #Listen mit verfügbaren Coins und Modi
    list_coins = ["Bitcoin", "Ethereum", "Dogecoin", "Tether", "XRP"]
    list_modes = ["Preis", "Volumen"]

    #Nutzer wird nach Coin gefragt
    print("\nBitcoin \nEthereum \nDogecoin \nTether \nXRP \n\nBitte geben Sie die Währung an, zu der Sie einen Graphen sehen möchten, die verfügbaren Währungen sind oben gelistet:\n ")
    coin = input()

    #Prüfung der Eingabe
    while (True):
        if coin in list_coins :
            break
        else:
            print("\nBitcoin \nEthereum \nDogecoin \nTether \nXRP \n\nGeben Sie bitte nur eine der oben gelisteten Cryptowährungen an\n")
            coin = input()

    #dataframe der jeweiligen Eingabe
    match coin:
        case "Bitcoin":
            dataframe = "bitcoin.csv"
        case "Ethereum":
            dataframe = "ethereum.csv"
        case "Dogecoin":
            dataframe = "dogecoin.csv"
        case "Tether":
            dataframe = "tether.csv"
        case "XRP":
            dataframe = "xrp.csv"
    
    #Nutzer wird nach Modus gefragt
    print("\nPreis \nVolumen \n\nWas soll im Graphen dargestellt werden? Die verfügbaren Modi sind oben gelistet:\n ")
    mode = input()

    #Prüfung der Eingabe
    while (True):
        if mode in list_modes :
            break
        else:
            print("\nPreis \nVolumen \n\nGeben Sie bitte nur einen der oben gelisteten Modi an\n")
            mode = input()
    
    match mode:
        case "Preis":
            value = "price"
            label = "Preis"
            label_ax = "Preis (EUR)"
        case "Volumen":
            value = "volume"
            label = "Volumen"
            label_ax = "Volumen (EUR)"

    #Timestamp wird als index des Dataframes gesetzt
    coin_chart = pd.read_csv(dataframe, index_col=0)
    coin_chart["time"] = pd.to_datetime(coin_chart["time"], format="%Y-%m-%d %H:%M:%S")
    coin_chart = coin_chart.set_index("time")

    print(coin_chart)

    #Erstellen des Graphen
    ax = fplt.create_plot(label + " von " + coin)
    ax.set_visible(crosshair=True, xaxis=True, yaxis=True, xgrid=False, ygrid=True)
    ax.setLabel("right", label_ax)
    fplt.plot(coin_chart[[value]], legend = label)
    fplt.show()

def main(): 
    plot()


if __name__ == "__main__":
    main()
