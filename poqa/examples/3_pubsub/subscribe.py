import time

from . import client

class LogClient(client.AsyncClient):
    logq_ = client.Queue()
    logq_.bind('nlogs')
    nlogs = client.Exchange(type='fanout')

    count = 0

    @logq_.basic_consumer(no_ack=True)
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
    c = LogClient()
    c.start()

