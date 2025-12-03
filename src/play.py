from time import sleep
from ticker import Ticker

ticker = Ticker()

ticker.start()

while True:
    try:
        sleep(60)
    except KeyboardInterrupt:
        ticker.stop()
        break
