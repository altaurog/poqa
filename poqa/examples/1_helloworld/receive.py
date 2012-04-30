import time

from . import client

class HelloClient(client.AsyncClient):
    hello = client.Queue()
    count = 0

    @hello.basic_consumer
    def print_message(self, channel, method, props, body):
        print ("-> %s" % body)
        try:
            duration = float(body)
        except ValueError:
            pass
        else:
            time.sleep(duration)
        print ("<- done")


if __name__ == '__main__':
    c = HelloClient()
    c.basic_qos(prefetch_count=1)
    c.start()
